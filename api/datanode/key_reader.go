package datanode

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/mengqi777/ozone-go/api"
	"github.com/mengqi777/ozone-go/api/common"
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
	ozone_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	log "github.com/wonderivan/logger"
	"go-ozone/internal/protocol/hadoop_hdds_datanode"
	"go-ozone/internal/protocol/hadoop_ozone"
	"io"
	"os"
	"sort"
)

type KeyReader struct {
	volume string
	bucket string
	key string

	ozClient         *api.OzoneClient
	blocks           []*ozone_proto.KeyLocation
	readers          []*BlockReader
	currentIndex     int
	blockOffsets     []uint64
	closed           bool
	offset           int64
}

func NewKeyReader(volume,bucket,key string,client *api.OzoneClient) *KeyReader {
	return &KeyReader{
		volume:       volume,
		bucket:       bucket,
		key:          key,
		ozClient:     client,
		blocks:       nil,
		readers:      nil,
		currentIndex: 0,
		blockOffsets: nil,
		closed:       false,
		offset:       0,
	}
}

func (r *KeyReader) init() error {
    keyInfo,err:=r.ozClient.OmClient.GetKey(r.volume,r.bucket,r.key)
    if err!=nil{
    	log.Error(err)
    	return err
	}
	if len(keyInfo.KeyLocationList) == 0 {
		return errors.New("Get key returned with zero key location version " + r.volume + "/" + r.bucket + "/" + r.key)
	}

	if len(keyInfo.KeyLocationList[0].KeyLocations) == 0 {
		return errors.New("Key location doesn't have any datanode for key " + r.volume + "/" + r.bucket + "/" + r.key)
	}

	r.blocks=keyInfo.GetKeyLocationList()[0].KeyLocations
	offset := uint64(0)
	for _, b := range r.blocks {
		if b.GetLength() == 0 {
			continue
		}
		pipelineOperator, err := NewPipelineOperator(b.Pipeline, r.fsClient.topologyAwareReadEnabled)
		if err != nil {
			return nil
		}

		reader := NewBlockReader(BlockIdToDatanodeBlockId(b.BlockID),
			NewDatanodeClient(pipelineOperator),
			r.fsClient.verifyChecksum,
			r.fsClient.topologyAwareReadEnabled,
			1024,
			b.GetLength())
		r.readers = append(r.readers, reader)
		r.blockOffsets = append(r.blockOffsets, offset)
		offset += b.GetLength()
	}

	return nil
}

func (r *KeyReader) ReadToWriter(writer io.Writer) (n int64, err error) {
	remain := r.fileInfoOperator.fileInfo.GetLength()
	for remain > 0 {
		current := r.readers[r.currentIndex]
		readLen, err := current.Get(&writer)
		if err != nil && err != io.EOF {
			return n, err
		}
		n += readLen
		remain -= readLen
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
	remain := r.fileInfoOperator.fileInfo.GetLength()
	for remain > 0 {
		current := r.readers[r.currentIndex]
		readLen, err := current.Read(buffer[n:])
		if err != nil && err != io.EOF {
			return n, err
		}
		n += readLen
		remain -= int64(readLen)
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
		return 0, &os.PathError{"readat", r.name, errors.New("negative offset")}
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
		off = r.fileInfoOperator.GetFileInfo().GetLength() + offset
	} else {
		return r.offset, fmt.Errorf("invalid whence: %d", whence)
	}

	if off < 0 || off > r.fileInfoOperator.GetFileInfo().GetLength() {
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

func (r *KeyReader) Off() uint64 {
	if r.currentIndex == 0 {
		return r.readers[r.currentIndex].Off()
	}
	return r.blockOffsets[r.currentIndex] + r.readers[r.currentIndex].Off()
}

func (r *KeyReader) Len() int64 {
	return *r.fileInfoOperator.GetFileInfo().Length
}

func (r *KeyReader) blockHasPosition(pos uint64, blockIndex int) bool {
	if blockIndex < 0 || blockIndex >= len(r.readers) {
		return false
	}
	return pos >= r.blockOffsets[blockIndex] &&
		pos < (r.blockOffsets[blockIndex]+r.readers[blockIndex].Len())
}

type BlockReader struct {
	// Only use to get BlockData.
	blockId            *dnapi.DatanodeBlockID
	dn                 *DatanodeClient
	readers            []*ChunkReader
	length             uint64
	currentIndex       int
	chunkOffs          []uint64
	verifyChecksum     bool
	topologyAwareRead  bool
	bufferSizePerChunk uint64
	readLength         int64
	initialized        bool
}

func NewBlockReader(blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum, topologyAwareRead bool,
	bufferSizePerChunk uint64, length uint64) *BlockReader {
	return newBlockReader(blockId, client, verifyChecksum, topologyAwareRead, bufferSizePerChunk, length)
}

func newBlockReader(blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum, topologyAwareRead bool,
	bufferSizePerChunk uint64, length uint64) *BlockReader {

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
	for idx, chunk := range chunks{
		log.Debug("chunk index", idx)
		reader := NewChunkReader(chunk, r.blockData.BlockID, r.dn, r.verifyChecksum, r.topologyAwareRead, r.bufferSizePerChunk, chunk.GetLen())
		r.readers = append(r.readers, reader)

		r.chunkOffs = append(r.chunkOffs, tmpOff)
		tmpOff += *chunk.Len
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
		readLen, err := current.Read(buff[n:n+int(current.GetChunkInfo().GetLen())])
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

func (r *BlockReader) Seek(offset int64, whence int) (int64, error) {

	if !r.initialized {
		if err := r.init(); err != nil {
			return 0, err
		}
	}

	var pos int64
	switch whence {
	case 0:
		pos = offset
	case 1:
		pos = int64(r.Off()) + offset
	case 2:
		pos = *r.blockData.Size + offset
	default:
		return int64(r.Off()), fmt.Errorf("blockReader: %v invalid whence: %d", r.blockData.BlockID.String(), whence)
	}

	if pos < 0 || pos > *r.blockData.Size {
		return int64(r.Off()), fmt.Errorf("blockReader: %v invalid resulting position: %d, block length: %v",
			r.blockData.BlockID.String(), pos, r.Len())
	}

	var index int
	if r.chunkHasPosition(uint64(pos), r.currentIndex) {
		index = r.currentIndex
	} else {
		// current chunk is not in this position. So reset it.
		r.readers[r.currentIndex].Reset()
		index = sort.Search(len(r.readers), func(i int) bool { return uint64(pos) >= r.chunkOffs[i] })
		// verify the r.readers[index] is in reader list
		if !r.chunkHasPosition(uint64(pos), index) {
			return int64(r.Off()), fmt.Errorf("blockReader: %v failed to find position: %d in chunks",
				r.blockData.BlockID.String(), pos)
		}
	}

	posInChunk := pos - int64(r.chunkOffs[index])
	if _, err := r.readers[index].Seek(posInChunk, io.SeekStart); err != nil {
		return int64(r.Off()), fmt.Errorf("blockReader: %v seek to %v failed: %v",
			r.blockData.BlockID.String(), pos, err)
	} else {
		r.currentIndex = index
	}

	return pos, nil
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
	return uint64(r.readLength) < r.length
}

func (r *BlockReader) IsRemaining() bool {
	return int64(r.Off()) < *r.blockData.Size
}

func (r *BlockReader) Len() uint64 {
	return uint64(*r.blockData.Size)
}

func (r *BlockReader) Off() uint64 {
	if r.currentIndex == 0 {
		return r.readers[r.currentIndex].Off()
	}
	return r.chunkOffs[r.currentIndex-1] + r.readers[r.currentIndex-1].Off()
}

func (r *BlockReader) chunkHasPosition(pos uint64, chunkIndex int) bool {
	if chunkIndex < 0 || chunkIndex >= len(r.readers) {
		return false
	}
	return pos >= r.chunkOffs[chunkIndex] &&
		pos < (r.chunkOffs[chunkIndex]+r.readers[chunkIndex].Len())
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
	length            uint64
	buffer            *bytes.Buffer
	bufferSizeAdvance uint64
	bufferOffPresent  uint64
	bufferLenPresent  uint64
	readLength        int64
	chunkPosition     int64
}

func NewChunkReader(chunkInfo ChunkInfo,
	blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum bool,
	topologyAwareRead bool,
	bufferSize uint64, length uint64) *ChunkReader {

	return newChunkReader(chunkInfo, blockId, client, verifyChecksum, topologyAwareRead, bufferSize, length)
}

func newChunkReader(chunkInfo ChunkInfo,
	blockId *dnapi.DatanodeBlockID,
	client *DatanodeClient,
	verifyChecksum bool,
	topologyAwareRead bool,
	bufferSize uint64, length uint64) *ChunkReader {
	return &ChunkReader{
		chunkInfo:         chunkInfo,
		blockId:           blockId,
		dn:                client,
		length:            length,
		checksum:          newChecksumOperatorVerifier(chunkInfo.ChecksumData),
		verifyChecksum:    verifyChecksum,
		topologyAwareRead: topologyAwareRead,
		bufferSizeAdvance: bufferSize,
		bufferOffPresent:  0,
		bufferLenPresent:  0,
		readLength:        0,
		chunkPosition:     noSeekPos,
	}
}

func (r *ChunkReader) Get(writer *io.Writer) (int64, error) {
	remain := r.chunkInfo.GetLen()
	n := int64(0)
	for remain > 0 {
		readLen, err := r.readToLocal(writer)
		if err != nil {
			log.Fatal("err", err)
		}
		n += readLen
		remain -= uint64(readLen)
	}
	r.readLength = n
	return n, nil
}

func (r *ChunkReader) Read(buff []byte) (int, error) {
	remain := len(buff)
	n := 0
	for remain > 0 {
		rn, err := r.readToBuf(buff)
		if err != nil {
			return 0, err
		}
		n += rn
		remain -= rn
	}
	r.readLength = int64(n)
	return n, nil
}

func (r *ChunkReader) readToLocal(writer *io.Writer) (int64, error) {

	count := len(r.dn.pipeline.Pipeline.Members)
	var index = 0
	var err error
	var succeccConnect = true
	for index < count {
		if succeccConnect {
			resp, err := r.dn.GetChunk(r.blockId, r.chunkInfo)
			if err != nil {
				index++
				succeccConnect = false
				continue
			}
			r.reader = bytes.NewReader(resp.GetData())
			n, err := io.Copy(*writer, r.reader)
			if err != nil {
				log.Error("copy error", err)
			}
			resp=nil
			return n, nil
		} else {
			if err := r.dn.connectToStandalone(r.dn.getCurrentDataNodeIp(index)); err != nil {
				log.Error("error connection to standalone ", r.dn.getCurrentDataNodeIp(index), err)
				index++
			} else {
				succeccConnect = true
			}
		}
	}
	return 0, err
}

func (r *ChunkReader) readToBuf(buf []byte) (int, error) {

	count := len(r.dn.pipeline.Pipeline.Members)
	var index = 0
	var err error
	var succeccConnect = true
	for index < count {
		if succeccConnect {
			resp, err := r.dn.GetChunk(r.blockId, r.chunkInfo)
			if err != nil {
				index++
				succeccConnect = false
				continue
			}
			r.buffer = bytes.NewBuffer(resp.GetData())
		    n,err:= r.readFromBuffer(buf)
			if err != nil {
				log.Error("copy error", err)
			}
			return n, nil
		} else {
			if err := r.dn.connectToStandalone(r.dn.getCurrentDataNodeIp(index)); err != nil {
				log.Error("error connection to standalone ", r.dn.getCurrentDataNodeIp(index), err)
				index++
			} else {
				succeccConnect = true
			}
		}
	}
	return 0, err
}

func (r *ChunkReader) Seek(offset int64, whence int) (int64, error) {
	var pos int64
	switch whence {
	case 0:
		pos = offset
	case 1:
		pos = int64(r.bufferOffPresent) + offset
	case 2:
		pos = int64(r.Len()) + offset
	default:
		return int64(r.bufferOffPresent), fmt.Errorf("chunkReader: %v invalid whence: %d", *r.chunkInfo.ChunkName, whence)
	}

	if pos < 0 || pos > int64(r.Len()) {
		return int64(r.bufferOffPresent), fmt.Errorf("chunkReader: %v invalid resulting position: %d, chunk length: %v",
			*r.chunkInfo.ChunkName, pos, r.Len())
	}

	if r.bufferHasPosition(uint64(pos)) {
		r.bufferOffPresent, r.bufferLenPresent = r.adjustBuffer(uint64(pos), r.bufferOffPresent, r.buffer)
	} else {
		r.chunkPosition = pos
	}

	return pos, nil
}

func (r *ChunkReader) Reset() {
	//r.buffer.Reset()
	r.buffer = nil
	r.reader = nil
	//r.bufferOffPresent = 0
	//r.bufferLenPresent = 0
	r.chunkPosition = noSeekPos
}

func (r *ChunkReader) IsRemaining() bool {
	var bufferPos uint64
	if r.chunkPosition >= 0 {
		bufferPos = uint64(r.chunkPosition)
	} else {
		bufferPos = r.bufferOffPresent + r.bufferLenPresent
	}

	return bufferPos < r.Len()
}

func (r *ChunkReader) Off() uint64 {
	if r.hasSeekPosition() {
		return uint64(r.chunkPosition)
	}
	return r.bufferOffPresent
}

func (r *ChunkReader) GetChunkInfo() *hadoop_hdds_datanode.ChunkInfo {
	return r.chunkInfo
}

func (r *ChunkReader) Len() uint64 {
	return *r.chunkInfo.Len
}

func (r *ChunkReader) computeChecksumBoundaries(startByteIndex, length uint64) (uint64, uint64) {

	bytesPerChecksum := uint64(*(r.chunkInfo.ChecksumData.BytesPerChecksum))

	checksumStartIndex := (startByteIndex / bytesPerChecksum) * bytesPerChecksum

	endByteIndex := startByteIndex + length - 1
	checksumEndIndex := ((endByteIndex / bytesPerChecksum) + 1) * bytesPerChecksum
	if checksumEndIndex > r.Len() {
		checksumEndIndex = r.Len()
	}

	return checksumStartIndex, checksumEndIndex - checksumStartIndex
}

// Read more bytes when using Checksum. So we adjust buffer and remove extra bytes
func (r *ChunkReader) adjustBuffer(realReadOff, adjustedReadOff uint64, buffer *bytes.Buffer) (uint64, uint64) {
	buffer.Next(int(realReadOff - adjustedReadOff))
	return realReadOff, uint64(buffer.Len())
}

func (r *ChunkReader) nextBufferOff() uint64 {
	return r.bufferOffPresent + r.bufferLenPresent
}

//Reset the chunkPosition once the buffers are allocated.
func (r *ChunkReader) resetSeekPosition() {
	r.chunkPosition = -1
}

func (r *ChunkReader) hasSeekPosition() bool {
	return r.chunkPosition >= 0
}

func (r *ChunkReader) bufferHasData() bool {
	return r.buffer != nil && r.buffer.Len() > 0
}

func (r *ChunkReader) bufferHasPosition(pos uint64) bool {
	if r.bufferHasData() {
		return pos >= r.bufferOffPresent && pos < r.bufferOffPresent+r.bufferLenPresent
	}
	return false
}

// After saveCondition. ChunkReader can read again when some error occurs
func (r *ChunkReader) saveCondition() {
	r.buffer.Reset()
	r.chunkPosition = int64(r.bufferOffPresent)
	r.bufferLenPresent = 0
}

func (r *ChunkReader) readFromBuffer(b []byte) (int, error) {
	readBytes, err := r.buffer.Read(b)
	r.bufferOffPresent += uint64(readBytes)
	r.bufferLenPresent -= uint64(readBytes)
	return readBytes, err
}

func (r *ChunkReader) hasRemaining() bool {
	return uint64(r.readLength) < r.length
}

func (r *ChunkReader) remaining() uint64 {
	return r.length - uint64(r.readLength)
}

func (r *ChunkReader) Close() {
	r.Reset()
}

