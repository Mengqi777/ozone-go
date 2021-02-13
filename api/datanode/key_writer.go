package datanode

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/mengqi777/ozone-go/api/common"
	"github.com/mengqi777/ozone-go/api/om"
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
	ozone_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	"github.com/mengqi777/ozone-go/api/proto/ratis"
	"github.com/mengqi777/ozone-go/api/util"
	log "github.com/wonderivan/logger"
)

type flushStatus string

const (
	flushedInitStatus flushStatus = "INIT"
	flushedRunStatus  flushStatus = "RUN"
	flushedSuccStatus flushStatus = "SUCC"
	flushedFailStatus flushStatus = "FAIL"
)

const maxRetryCount = 3

type KeyWriter struct {
	OmClient *om.OmClient

	volume string
	bucket string
	key    string

	KeyInfo             *ozone_proto.KeyInfo
	ID                  *uint64
	dataSize            uint64
	checksumType        dnapi.ChecksumType
	bytesPerChecksum    uint32
	chunkSize           uint64
	blockSize           uint64
	writers             []*BlockWriter
	currentIndex        int
	closed              bool
	flushChunkBatchSize int
}

func NewKeyWriter(length uint64, id *uint64, info *ozone_proto.KeyInfo, client *om.OmClient) *KeyWriter {
	return &KeyWriter{
		OmClient:     client,
		volume:       info.GetVolumeName(),
		bucket:       info.GetBucketName(),
		key:          info.GetKeyName(),
		KeyInfo:      info,
		ID:           id,
		dataSize:     length,
		writers:      make([]*BlockWriter, 0),
		currentIndex: -1,
		closed:       false,

		checksumType:        common.DEFAULT_CHECKSUM_TYPE,      //default
		bytesPerChecksum:    common.DEFAULT_BYTES_PER_CHECKSUM, //default
		chunkSize:           common.DEFAULT_CHUNK_SIZE,         //default
		blockSize:           common.DEFAULT_BLOCK_SIZE,         //default
		flushChunkBatchSize: common.DEFAULT_FLUSH_LENGTH,       //default
	}
}

func (keyWriter *KeyWriter) init() error {
	_, writer, err := keyWriter.addBlockLocation(keyWriter.KeyInfo.KeyLocationList[0].GetKeyLocations()[0])
	if err != nil {
		log.Error(err)
		return err
	}
	writers := make([]BlockWriter, 0)
	writers = append(writers, *writer)
	keyWriter.currentIndex = 0
	return nil
}

func (keyWriter *KeyWriter) allocateBlockIfNeed() (int, *BlockWriter, error) {
	if keyWriter.currentIndex == 0 {
		return keyWriter.currentIndex, keyWriter.writers[0], nil
	}
	index, _, err := keyWriter.allocateBlock()
	if err != nil {
		return -1, nil, err
	}
	keyWriter.currentIndex = index
	log.Debug("block index ", index)
	return keyWriter.currentIndex, keyWriter.writers[keyWriter.currentIndex], nil

}

func (keyWriter *KeyWriter) allocateBlock() (int, *BlockWriter, error) {
	blockLocation, err := keyWriter.OmClient.AllocateBlock(keyWriter.volume, keyWriter.bucket, keyWriter.key, keyWriter.ID)
	if err != nil {
		return 0, nil, err
	}
	index, writer, err := keyWriter.addBlockLocation(blockLocation.GetKeyLocation())
	return index, writer, err
}

func (keyWriter *KeyWriter) addBlockLocation(blockLocation *ozone_proto.KeyLocation) (int, *BlockWriter, error) {

	next := len(keyWriter.writers)

	// Param: topology just same to ozone java client.
	dn  := CreateDatanodeClientRaft(blockLocation.Pipeline)

	pipline, err := NewPipelineOperator(blockLocation.GetPipeline(), common.DEFAULT_TOPOLOGY_AWARE_READ)
	if err != nil {
		log.Error(err)
		return 0, nil, err
	}
	writer := newBlockWriter(
		BlockIdToDatanodeBlockId(blockLocation.BlockID),
		&keyWriter.checksumType,
		*blockLocation.Length,
		keyWriter.bytesPerChecksum,
		keyWriter.chunkSize,
		keyWriter.flushChunkBatchSize,
		dn,
		pipline,
		blockLocation,
	)

	keyWriter.writers = append(keyWriter.writers, writer)
	if err := dn.connectToLeaderRaft(writer.pipelineOperator.GetLeaderAddr(PipelinePortNameRATIS)); err != nil {
		return 0, nil, err
	}
	dn.watch(0, ratis.ReplicationLevel_MAJORITY)
	return next, writer, nil
}

func (keyWriter *KeyWriter) Write(p []byte) (uint64, error) {
	if keyWriter.closed {
		return 0, errors.New("closed error")
	}
	if err := keyWriter.init(); err != nil {
		return 0, err
	}

	keyWriter.dataSize = uint64(len(p))
	//flushedWriteLen:= uint64(0)
	writtenLen := uint64(0)
	remain := uint64(len(p))
	endPos := uint64(0)
	locations := make([]*ozone_proto.KeyLocation, 0)
	for remain > 0 {
		_, current, err := keyWriter.allocateBlockIfNeed()
		if err != nil {
			return 0, err
		}

		endPos = util.ComputeEndPos(remain, writtenLen, common.DEFAULT_BLOCK_SIZE, endPos)
		log.Debug("file currentIndex", keyWriter.currentIndex, " remain", remain, "writtenLen", writtenLen, "endPos", endPos)
		n, err := current.Write(p[writtenLen:endPos])
		writtenLen += n
		remain -= n
		current.length = n

		if err != nil {
			continue
		}
		locations = append(locations, current.location)
		//if err != nil && err != customerror.BufferErrFull {
		//	log.Error("customerror.BufferErrFull", err)
		//	return writtenLen, err
		//}

		//if !current.HasRemaining() {
		//	if err := current.Close(); err != nil {
		//		return writtenLen, err
		//	}
		//}
	}

	if err := keyWriter.Close(); err != nil {
		log.Error("closed error", err)
		return writtenLen, err
	}

	keyWriter.OmClient.CommitKey(keyWriter.volume, keyWriter.bucket, keyWriter.key, keyWriter.ID, locations, keyWriter.dataSize)
	return writtenLen, nil

}

func (keyWriter *KeyWriter) Close() error {
	if keyWriter.closed {
		return errors.New("error closed")
	}

	//err := keyWriter.writers[keyWriter.currentIndex].Close()

	keyWriter.closed = true
	return nil
}

type BlockWriter struct {
	blockId          *dnapi.DatanodeBlockID
	blockData        *dnapi.BlockData
	dn               *DatanodeClient
	pipelineOperator *PipelineOperator
	blockSize        uint64
	length           uint64
	offset           uint64
	checksumType     *dnapi.ChecksumType
	bytesPerChecksum uint32
	chunkSize        uint64
	writers          []*ChunkWriter

	flushedMap          map[int]flushStatus
	flushChunkBatchSize int
	flushedIndex        int
	currentIndex        int
	closed              bool
	location            *ozone_proto.KeyLocation
}

func newBlockWriter(blockId *dnapi.DatanodeBlockID, checksumType *dnapi.ChecksumType, blockSize uint64,
	bytesPerChecksum uint32, chunkSize uint64, flushChunkBatchSize int, dn *DatanodeClient,
	pipeline *PipelineOperator, blockLocation *ozone_proto.KeyLocation) *BlockWriter {

	var metadata []*dnapi.KeyValue

	typ := "TYPE"
	ky := "KEY"
	metadata = append(metadata, &dnapi.KeyValue{Key: &typ, Value: &ky})
	blockData := &dnapi.BlockData{
		BlockID:  blockId,
		Metadata: metadata,
		Chunks:   make([]*dnapi.ChunkInfo, 0),
	}
	return &BlockWriter{
		blockId:             blockId,
		blockData:           blockData,
		blockSize:           blockSize,
		pipelineOperator:    pipeline,
		length:              0,
		dn:                  dn,
		checksumType:        checksumType,
		bytesPerChecksum:    bytesPerChecksum,
		chunkSize:           chunkSize,
		writers:             make([]*ChunkWriter, 0),
		flushedMap:          make(map[int]flushStatus),
		flushChunkBatchSize: flushChunkBatchSize,
		flushedIndex:        -1,
		currentIndex:        -1,
		closed:              false,
		location:            blockLocation,
	}

}

func (bw *BlockWriter) allocateChunk() (int, *ChunkWriter) {
	nextIndex := len(bw.writers)
	chunkName := fmt.Sprintf("%d_chunk_%d", *bw.blockId.LocalID, nextIndex+1)
	log.Debug("chunkName",chunkName)
	writer := newChunkWriter(chunkName, bw.chunkSize, bw.blockId, bw.checksumType, bw.bytesPerChecksum, nextIndex, bw.dn)

	bw.writers = append(bw.writers, writer)

	bw.flushedMap[nextIndex] = flushedInitStatus
	return nextIndex, writer
}

func (bw *BlockWriter) currentWritableChunk() (int, *ChunkWriter) {

	var cw *ChunkWriter
	if len(bw.writers) == 0 {
		bw.currentIndex, cw = bw.allocateChunk()
		return bw.currentIndex, cw
	}

	cw = bw.writers[bw.currentIndex]
	if !cw.HasRemaining() {
		bw.currentIndex, cw = bw.allocateChunk()
		return bw.currentIndex, cw
	}
	return bw.currentIndex, cw
}

func (bw *BlockWriter) BlockId() *dnapi.DatanodeBlockID {
	return bw.blockId
}

func (bw *BlockWriter) Pipeline() *hdds.Pipeline {
	return bw.pipelineOperator.Pipeline
}

func (bw *BlockWriter) Write(p []byte) (uint64, error) {

	writtenLen := uint64(0)
	flushedWrittenLen := uint64(0)
	remain := uint64(len(p))
	endPos := uint64(0)

	data := p[:remain]

	for remain > 0 {
		currentIndex, current := bw.currentWritableChunk()
		endPos = util.ComputeEndPos(remain, writtenLen, bw.chunkSize, endPos)
		log.Debug("block currentIndex", currentIndex, "remain", remain, "writtenLen", writtenLen, "endPos", endPos)
		log.Debug("block current Remaining", current.Remaining(), "data length", (endPos - writtenLen))

		n, err := current.Write(data[writtenLen:endPos])

		bw.length += uint64(n)
		writtenLen += uint64(n)
		remain -= uint64(n)

		if err != nil && err != bufio.ErrBufferFull {
			return writtenLen, err
		}

		if !current.HasRemaining() {
			if err = bw.writeChunkToContainerAsync(currentIndex); err != nil {
				return flushedWrittenLen, err
			}
		} else {
			if remain <= 0 {
				if err = bw.writeChunkToContainerAsync(currentIndex); err != nil {
					return flushedWrittenLen, err
				}
			}
		}

		//当达到batch size的时候，flush chunk，put block
		if currentIndex-bw.flushedIndex >= bw.flushChunkBatchSize {
			if err = bw.putBlockAsync(false); err != nil {
				return flushedWrittenLen, err
			}
			flushedWrittenLen = writtenLen
			if err = bw.dn.watch(uint64(bw.flushedIndex), ratis.ReplicationLevel_ALL_COMMITTED); err != nil {
				bw.dn.watch(uint64(bw.flushedIndex), ratis.ReplicationLevel_MAJORITY_COMMITTED)
			}
		}
		current.Close()
	}

	if writtenLen > flushedWrittenLen {
		if err := bw.putBlockAsync(true); err != nil {
			return flushedWrittenLen, err
		}
		flushedWrittenLen = writtenLen
		if err := bw.dn.watch(uint64(bw.flushedIndex), ratis.ReplicationLevel_ALL_COMMITTED); err != nil {
			bw.dn.watch(uint64(bw.flushedIndex), ratis.ReplicationLevel_MAJORITY_COMMITTED)
		}
	}

	return flushedWrittenLen, nil
}

func (bw *BlockWriter) shouldFlush() bool {
	return false
}

func (bw *BlockWriter) Close() error {

	if bw.closed {
		return errors.New("error closed")
	}

	if bw.flushedMap[bw.currentIndex] == flushedInitStatus {
		bw.writeChunkToContainerAsync(bw.currentIndex)
	}

	if err := bw.waitForAllChunkFlushed(true); err != nil {
		return err
	}

	var err error
	bw.putBlockAsync(true)

	err = bw.waitForPutBlock()
	bw.closed = err == nil
	return err

}

func (bw *BlockWriter) IsClosed() bool {
	return bw.closed
}

func (bw *BlockWriter) Len() uint64 {
	return bw.length
}

func (bw *BlockWriter) HasRemaining() bool {
	if bw.closed {
		return false
	}
	return bw.blockSize > bw.length
}

func (bw *BlockWriter) Remaining() uint64 {
	if bw.closed {
		return 0
	}
	return bw.blockSize - bw.length
}

func (bw *BlockWriter) isPutBlockComplete() bool {
	return bw.currentIndex == bw.flushedIndex
}

func (bw *BlockWriter) waitForPutBlock() error {
	for i := 0; i < maxRetryCount; i++ {
		if !bw.isPutBlockComplete() {
			bw.putBlockAsync(true)
		} else {
			return nil
		}
	}
	return fmt.Errorf("put block failed")
}

func (bw *BlockWriter) waitForAllChunkFlushed(containCurrent bool) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		if !bw.isAllChunkFlushed(containCurrent) {
			err = bw.flushFailedChunk(containCurrent)
		} else {
			return nil
		}
	}

	return err
	//return fmt.Errorf("write chunk failed")
}

func (bw *BlockWriter) flushFailedChunk(containCurrent bool) error {
	start := 0
	end := bw.currentIndex
	if !containCurrent {
		end = bw.currentIndex - 1
	}

	if end < 0 {
		end = 0
	}
	var err error
	for i := start; i <= end; i++ {
		switch bw.flushedMap[i] {
		case flushedFailStatus:
			log.Debug("flush fail chunk ", i, bw.writers[i].chunkInfo.GetChunkName())
			err = bw.writeChunkToContainerAsync(i)
		case flushedSuccStatus:
			// do nothing on succ
		case flushedRunStatus:
			// do nothing on run
		case flushedInitStatus:
			// do nothing on init
		}
	}
	return err
}

func (bw *BlockWriter) isAllChunkFlushed(containCurrent bool) bool {
	start := 0
	end := bw.currentIndex
	if !containCurrent {
		end = bw.currentIndex - 1
	}

	if end < 0 {
		end = 0
	}

	for i := start; i <= end; i++ {
		if bw.flushedMap[i] != flushedSuccStatus {
			return false
		}
	}

	return true

}

func (bw *BlockWriter) putBlockAsync(eof bool) error {
	if err := bw.putBlock(eof); err != nil {
		fmt.Println("put block failed: ", err)
		return err
	}
	return nil
}

func (bw *BlockWriter) putBlock(eof bool) error {

	var chunks []*dnapi.ChunkInfo
	for _, writer := range bw.orderedFlushedChunks() {
		chunks = append(chunks, writer.Info())
	}
	if len(chunks) == 0 {
		return nil
	}
	typ := "TYPE"
	ky := "KEY"
	var metadata []*dnapi.KeyValue
	metadata = append(metadata, &dnapi.KeyValue{Key: &typ, Value: &ky})

	blockData := &dnapi.BlockData{
		BlockID:  bw.blockId,
		Metadata: metadata,
		Chunks:   chunks,
	}

	resp, err := bw.dn.PutBlockRatis(blockData.BlockID, chunks)
	if err != nil {
		return err
	}
	bw.blockId = resp.GetCommittedBlockLength.GetBlockID()
	bw.blockData = blockData
	if len(chunks) != 0 {
		bw.flushedIndex = len(chunks) - 1
	}
	return nil
}

func (bw *BlockWriter) orderedFlushedChunks() []*ChunkWriter {

	completedIndex := 0
	for index := range bw.writers {
		if bw.flushedMap[index] == flushedSuccStatus {
			completedIndex = index
		} else {
			break
		}
	}
	return bw.writers[:completedIndex+1]

}

func (bw *BlockWriter) writeChunkToContainerAsync(writerIndex int) error {

	bw.flushedMap[writerIndex] = flushedRunStatus
	if err := bw.writers[writerIndex].WriteToContainer(); err != nil {
		bw.flushedMap[writerIndex] = flushedFailStatus
		return err
	}
	bw.flushedMap[writerIndex] = flushedSuccStatus
	//if err := bw.writers[writerIndex].Close(); err != nil {
	//	log.Debug("closed chunk writer failed: ", err)
	//}
	return nil
}

type ChunkWriter struct {
	blockId          *dnapi.DatanodeBlockID
	chunkInfo        *dnapi.ChunkInfo
	data             []byte
	bytesPerChecksum uint32
	chunkSize        uint64
	length           uint64
	myIndex          int
	buffer           *limitBuffer
	dn               *DatanodeClient
	closed           bool
	written          bool
	checksumType     *dnapi.ChecksumType
}

func newChunkWriter(chunkName string, chunkSize uint64, blockId *dnapi.DatanodeBlockID,
	checksumType *dnapi.ChecksumType, bytesPerChecksum uint32, myIndex int, dn *DatanodeClient) *ChunkWriter {

	chunkInfo := &dnapi.ChunkInfo{
		ChunkName:    util.Ptr(chunkName),
		Offset:       util.Ptri(0),
		Len:          util.Ptri(0),
		ChecksumData: nil,
	}

	return &ChunkWriter{
		blockId:          blockId,
		bytesPerChecksum: bytesPerChecksum,
		checksumType:     checksumType,
		chunkInfo:        chunkInfo,
		chunkSize:        chunkSize,
		length:           0,
		dn:               dn,
		myIndex:          myIndex,
		closed:           false,
		written:          false,
	}
}

func (cw *ChunkWriter) Info() *dnapi.ChunkInfo {
	return cw.chunkInfo
}

func (cw *ChunkWriter) checkWritable() error {
	if cw.closed {
		return errors.New("error closed")
	}

	if cw.written {
		return fmt.Errorf("chunk: %s has written to container", cw.chunkInfo.GetChunkName())
	}

	cw.verifyBuffer()

	return nil

}

func (cw *ChunkWriter) Write(p []byte) (n int, err error) {
	if err := cw.checkWritable(); err != nil {
		return 0, err
	}
	n, err = cw.buffer.Write(p)
	cw.length += uint64(n)

	return n, err
}

func (cw *ChunkWriter) Close() error {
	if cw.closed {
		return errors.New("error closed")
	}

	if !cw.written {
		if err := cw.WriteToContainer(); err != nil {
			return err
		}
	}

	cw.written = true
	cw.closed = true
	// Clear buffer when writer is closed
	return nil
}

func (cw *ChunkWriter) Remaining() uint64 {
	if err := cw.checkWritable(); err != nil {
		return 0
	}
	return cw.chunkSize - cw.length
}

func (cw *ChunkWriter) HasRemaining() bool {
	if err := cw.checkWritable(); err != nil {
		return false
	}
	return cw.chunkSize > cw.length
}

func (cw *ChunkWriter) Len() uint64 {
	return cw.length
}

func (cw *ChunkWriter) IsClosed() bool {
	return cw.closed
}

func (cw *ChunkWriter) WriteToContainer() error {
	var data []byte
	if cw.length == 0 {
		data = []byte{}
	} else {
		data = cw.buffer.Bytes()
	}
	checksumOperator := newChecksumOperatorComputer(cw.checksumType,cw.bytesPerChecksum)
	if err := checksumOperator.ComputeChecksum(data); err != nil {
		return err
	}
	checksumData := checksumOperator.Checksum

	cw.chunkInfo.ChecksumData = checksumData

	cw.chunkInfo.Offset = util.Ptri(uint64(cw.myIndex) * common.DEFAULT_CHUNK_SIZE)
	cw.chunkInfo.Len = &cw.length

	_, err := cw.dn.WriteChunkRaft(cw.blockId, cw.chunkInfo, data)

	cw.written = err == nil
	return err
}

func (cw *ChunkWriter) verifyBuffer() {
	if cw.buffer == nil {
		cw.buffer = newLimitBuffer(cw.chunkSize)
	}
}

type limitBuffer struct {
	buffer *bytes.Buffer
	limit  uint64
}

func newLimitBuffer(limit uint64) *limitBuffer {
	buffer := bytes.NewBuffer(make([]byte, limit, limit))
	buffer.Reset()
	return &limitBuffer{buffer: buffer, limit: limit}
}

func (lb *limitBuffer) Write(p []byte) (int, error) {
	if !lb.HasRemaining() {
		return 0, bufio.ErrBufferFull
	}

	remaining := lb.Remaining()
	if uint64(len(p)) > remaining {
		return lb.buffer.Write(p[:remaining])
	}

	return lb.buffer.Write(p)
}

func (lb *limitBuffer) Read(p []byte) (int, error) {
	return lb.buffer.Read(p)
}

func (lb *limitBuffer) Bytes() []byte {
	return lb.buffer.Bytes()
}

func (lb *limitBuffer) Remaining() uint64 {
	return lb.Limit() - lb.Len()
}

func (lb *limitBuffer) HasRemaining() bool {
	return lb.Limit() > lb.Len()
}

func (lb *limitBuffer) Reset() {
	lb.buffer.Reset()
}

func (lb *limitBuffer) Next(n int) []byte {
	return lb.buffer.Next(n)
}

func (lb *limitBuffer) Len() uint64 {
	return uint64(lb.buffer.Len())
}

func (lb *limitBuffer) Limit() uint64 {
	return lb.limit
}
