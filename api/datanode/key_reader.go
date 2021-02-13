package datanode

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/mengqi777/ozone-go/api/om"
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	ozone_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	log "github.com/wonderivan/logger"
	"io"
	"os"
)

type KeyReader struct {
	volume             string
	bucket             string
	key                string
	KeyInfo            *ozone_proto.KeyInfo
	OmClient           *om.OmClient
	blocks             []*ozone_proto.KeyLocation
	readers            []*BlockReader
	currentIndex       int
	blockOffsets       []uint64
	offset             int64
	bufferSizePerChunk int64
	closed             bool
	verifyChecksum     bool
	topologyAwareRead  bool
}

func NewKeyReader(volume, bucket, key string, client *om.OmClient, info *ozone_proto.KeyInfo) *KeyReader {
	return &KeyReader{
		volume:       volume,
		bucket:       bucket,
		key:          key,
		KeyInfo:      info,
		OmClient:     client,
		blocks:       nil,
		readers:      nil,
		currentIndex: 0,
		blockOffsets: nil,
		closed:       false,
		offset:       0,
	}
}

func (r *KeyReader) init() error {
	keyInfo := r.KeyInfo

	if len(keyInfo.KeyLocationList) == 0 {
		return errors.New("Get key returned with zero key location version " + r.volume + "/" + r.bucket + "/" + r.key)
	}

	if len(keyInfo.KeyLocationList[0].KeyLocations) == 0 {
		return errors.New("Key location doesn't have any datanode for key " + r.volume + "/" + r.bucket + "/" + r.key)
	}

	r.blocks = keyInfo.GetKeyLocationList()[0].KeyLocations
	offset := uint64(0)
	for _, b := range r.blocks {
		if b.GetLength() == 0 {
			continue
		}
		dnClient, err := CreateDatanodeClient(b.Pipeline)
		if err != nil {
			log.Error(err)
			return err
		}
		reader := NewBlockReader(BlockIdToDatanodeBlockId(b.BlockID), dnClient,
			r.verifyChecksum, r.topologyAwareRead, r.bufferSizePerChunk, r.Len())
		r.readers = append(r.readers, reader)
		r.blockOffsets = append(r.blockOffsets, offset)
		offset += b.GetLength()
	}

	return nil
}

func (r *KeyReader) ReadToWriter(writer io.Writer) (n int64, err error) {
	r.init()
	remain := r.KeyInfo.GetDataSize()
	log.Info("datasize",remain)
	for remain > 0 {
		log.Info("remain",remain)
		log.Info("r.currentIndex",r.currentIndex)
		current := r.readers[r.currentIndex]
		readLen, err := current.Get(&writer)
		if err != nil && err != io.EOF {
			return n, err
		}
		n += readLen
		remain -= uint64(readLen)
		r.offset += readLen
		if !current.hasRemaining() {
			r.currentIndex++
			current.Close()
		}
	}
	return n, nil
}

func (r *KeyReader) Read(buffer []byte) (n int, err error) {
	if err = r.init(); err != nil {
		return 0, err
	}
	remain := r.KeyInfo.GetDataSize()
	for remain > 0 {
		current := r.readers[r.currentIndex]
		readLen, err := current.Read(buffer[n:])
		if err != nil && err != io.EOF {
			return n, err
		}
		n += readLen
		remain -= uint64(readLen)
		if !current.hasRemaining() {
			r.currentIndex++
		}
		current.Close()
	}
	return n, nil
}

// ReadAt implements io.ReaderAt.
func (r *KeyReader) ReadAt(b []byte, off int64) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	if off < 0 {
		return 0, &os.PathError{"readat", r.key, errors.New("negative offset")}
	}

	_, err := r.Seek(off, 0)
	if err != nil {
		return 0, err
	}

	n, err := io.ReadFull(r, b)

	// For some reason, os.File.ReadAt returns io.EOF in this case instead of
	// io.ErrUnexpectedEOF.
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	return n, err
}

// Seeks to a given position
func (r *KeyReader) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	var off int64
	if whence == 0 {
		off = offset
	} else if whence == 1 {
		off = r.offset + offset
	} else if whence == 2 {
		off = int64(r.KeyInfo.GetDataSize()) + offset
	} else {
		return r.offset, fmt.Errorf("invalid whence: %d", whence)
	}

	if off < 0 || off > int64(r.KeyInfo.GetDataSize()) {
		return r.offset, fmt.Errorf("invalid resulting offset: %d", off)
	}

	if r.offset != off {
		r.offset = off
		if r.readers != nil {
			for _, reader := range r.readers {
				reader.Close()
			}
			r.readers = nil
		}
	}
	return r.offset, nil
}

// Returns current position
func (r *KeyReader) Position() (int64, error) {
	actualPos, err := r.Seek(0, 1)
	if err != nil {
		return 0, err
	}
	return actualPos, nil
}

func (r *KeyReader) Close() error {
	r.Reset()
	r.closed = true
	return nil
}

func (r *KeyReader) Reset() {
	for _, r := range r.readers {
		r.Reset()
	}
	r.currentIndex = -1
}

func (r *KeyReader) Len() int64 {
	return int64(r.KeyInfo.GetDataSize())
}

type BlockReader struct {
	// Only use to get BlockData.
	blockId            *dnapi.DatanodeBlockID
	dn                 *DatanodeClient
	readers            []*ChunkReader
	length             int64
	currentIndex       int
	chunkOffs          []uint64
	verifyChecksum     bool
	topologyAwareRead  bool
	bufferSizePerChunk int64
	readLength         int64
	initialized        bool
}

func NewBlockReader(blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum, topologyAwareRead bool,
	bufferSizePerChunk int64, length int64) *BlockReader {
	return newBlockReader(blockId, client, verifyChecksum, topologyAwareRead, bufferSizePerChunk, length)
}

func newBlockReader(blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum, topologyAwareRead bool,
	bufferSizePerChunk int64, length int64) *BlockReader {

	return &BlockReader{
		blockId:            blockId,
		dn:                 client,
		length:             length,
		verifyChecksum:     verifyChecksum,
		topologyAwareRead:  topologyAwareRead,
		bufferSizePerChunk: bufferSizePerChunk,
		readLength:         0,
	}
}

func (r *BlockReader) init() error {

	chunks, err := r.dn.GetBlock(r.blockId)
	if err != nil {
		return err
	}
	tmpOff := uint64(0)
	for idx, chunk := range chunks {
		log.Debug("chunk index", idx)
		reader := NewChunkReader(chunk, r.blockId, r.dn, r.verifyChecksum, r.topologyAwareRead, r.bufferSizePerChunk, chunk.Len)
		r.readers = append(r.readers, reader)

		r.chunkOffs = append(r.chunkOffs, tmpOff)
		tmpOff += uint64(chunk.Len)
	}

	r.currentIndex = 0
	r.initialized = true
	return nil
}
func (r *BlockReader) Get(writer *io.Writer) (int64, error) {
	if err := r.init(); err != nil {
		return 0, err
	}
	readLen := int64(0)
	if r.currentIndex >= len(r.readers) {
		return readLen, io.EOF
	}
	remain := int64(r.length)

	for remain > 0 {
		current := r.readers[r.currentIndex]
		n, err := current.Get(writer)
		if err != nil && err != io.EOF {
			return readLen, err
		}
		readLen += n
		remain -= n
		log.Debug("read length", readLen, "readmain", remain, "current.hasRemaining", current.remaining())
		if !current.hasRemaining() {
			r.currentIndex++
		}
		current.Close()
	}
	r.readLength = readLen
	return readLen, nil
}

func (r *BlockReader) Read(buff []byte) (int, error) {
	if err := r.init(); err != nil {
		return 0, err
	}
	n := 0
	if r.currentIndex >= len(r.readers) {
		return n, io.EOF
	}
	remain := int(r.length)

	for remain > 0 {
		current := r.readers[r.currentIndex]
		readLen, err := current.Read(buff[n : n+int(current.GetChunkInfo().Len)])
		log.Error("BlockReaderBlockReaderBlockReader",err)
		if err != nil && err != io.EOF {

			return readLen, err
		}
		n += readLen
		remain -= readLen
		log.Debug("read length", readLen, "readmain", remain, "current.hasRemaining", current.remaining())
		if !current.hasRemaining() {

			r.currentIndex++
		}
		current.Close()
	}
	r.readLength = int64(n)
	return n, nil
}

func (r *BlockReader) Close() error {
	r.Reset()
	return nil
}

func (r *BlockReader) Reset() {
	for _, reader := range r.readers {
		reader.Reset()
	}
	r.currentIndex = -1
}

func (r *BlockReader) hasRemaining() bool {
	return r.readLength < r.length
}


const (
	noSeekPos = -1
)

type ChunkReader struct {
	chunkInfo         ChunkInfo
	blockId           *dnapi.DatanodeBlockID
	dn                *DatanodeClient
	reader            io.Reader
	checksum          *ChecksumOperator
	verifyChecksum    bool
	topologyAwareRead bool
	length            int64
	readLength        int64
	chunkPosition     int64
	buffer            *bytes.Buffer
}

func NewChunkReader(chunkInfo ChunkInfo,
	blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum bool,
	topologyAwareRead bool,
	bufferSize int64, length int64) *ChunkReader {

	return newChunkReader(chunkInfo, blockId, client, verifyChecksum, topologyAwareRead, bufferSize, length)
}

func newChunkReader(chunkInfo ChunkInfo,
	blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum bool,
	topologyAwareRead bool,
	bufferSize int64, length int64) *ChunkReader {
	return &ChunkReader{
		chunkInfo:         chunkInfo,
		blockId:           blockId,
		dn:                client,
		length:            length,
		checksum:          newChecksumOperatorVerifier(chunkInfo.ChecksumData),
		verifyChecksum:    verifyChecksum,
		topologyAwareRead: topologyAwareRead,
		readLength:        0,
		chunkPosition:     noSeekPos,
	}
}

func (r *ChunkReader) Get(writer *io.Writer) (int64, error) {
	remain := r.chunkInfo.Len
	n := int64(0)
	for remain > 0 {
		readLen, err := r.readToLocal(writer)
		if err != nil {
			log.Fatal("err", err)
		}
		n += readLen
		remain -= readLen
	}
	r.readLength = n
	return n, nil
}

func (r *ChunkReader) Read(buff []byte) (int, error) {
	remain := len(buff)
	n := 0
	for remain > 0 {
		rn, err := r.readToBytes(buff)
		log.Error("rnrnrnr")
		if err != nil {

			return 0, err
		}
		n += rn
		remain -= rn
	}
	r.readLength = int64(n)
	return n, nil
}
//
func (r *ChunkReader) readToLocal(writer *io.Writer) (int64, error) {

	count := len(r.dn.datanodes)
	var index = 0
	var err error
	var succeccConnect = true
	for index < count {
		if succeccConnect {
			resp, err := r.dn.ReadChunk(r.blockId, r.chunkInfo)
			if err != nil {
				index++
				succeccConnect = false
				continue
			}
			r.reader = bytes.NewReader(resp)
			n, err := io.Copy(*writer, r.reader)
			if err != nil {
				log.Error("copy error", err)
			}
			resp = nil
			return n, nil
		} else {
			if err := r.dn.connectToNextStandalone(); err != nil {
				log.Error("error connection to standalone ", r.dn.currentDatanode.GetIpAddress(), err)
				index++
			} else {
				succeccConnect = true
			}
		}
	}
	return 0, err
}

func (r *ChunkReader) readToBytes(buf []byte) (int, error) {

	count := len(r.dn.datanodes)
	var index = 0
	var n int
	var err error
	var resp []byte
	var succeccConnect = true
	for index < count {

		if succeccConnect {
			resp, err = r.dn.ReadChunk(r.blockId, r.chunkInfo)
			if err != nil {
				succeccConnect = false
				continue
			}
			r.buffer = bytes.NewBuffer(resp)
			n, err = r.buffer.Read(buf)
			if err != nil {
				log.Error("copy error", err)
			}
			return n, nil
		} else {
			if err := r.dn.connectToNextStandalone(); err != nil {
				log.Error("error connection to standalone ", r.dn.currentDatanode.GetIpAddress(), err)
			} else {
				succeccConnect = true
			}
		}
		index=index+1
	}
	return 0, err
}

func (r *ChunkReader) Reset() {
	r.reader = nil
	r.chunkPosition = noSeekPos
}

func (r *ChunkReader) GetChunkInfo() ChunkInfo {
	return r.chunkInfo
}

func (r *ChunkReader) Len() int64 {
	return r.chunkInfo.Len
}

func (r *ChunkReader) computeChecksumBoundaries(startByteIndex, length uint64) (uint64, uint64) {

	bytesPerChecksum := uint64(*(r.chunkInfo.ChecksumData.BytesPerChecksum))

	checksumStartIndex := (startByteIndex / bytesPerChecksum) * bytesPerChecksum

	endByteIndex := startByteIndex + length - 1
	checksumEndIndex := ((endByteIndex / bytesPerChecksum) + 1) * bytesPerChecksum
	if checksumEndIndex > uint64(r.Len()) {
		checksumEndIndex = uint64(r.Len())
	}

	return checksumStartIndex, checksumEndIndex - checksumStartIndex
}

// Read more bytes when using Checksum. So we adjust buffer and remove extra bytes
func (r *ChunkReader) adjustBuffer(realReadOff, adjustedReadOff uint64, buffer *bytes.Buffer) (uint64, uint64) {
	buffer.Next(int(realReadOff - adjustedReadOff))
	return realReadOff, uint64(buffer.Len())
}

//Reset the chunkPosition once the buffers are allocated.
func (r *ChunkReader) resetSeekPosition() {
	r.chunkPosition = -1
}

func (r *ChunkReader) hasSeekPosition() bool {
	return r.chunkPosition >= 0
}

func (r *ChunkReader) hasRemaining() bool {
	return r.readLength < r.length
}

func (r *ChunkReader) remaining() int64 {
	return r.length - r.readLength
}

func (r *ChunkReader) Close() {
	r.Reset()
}
