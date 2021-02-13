package datanode

import (
	"bytes"
	"fmt"
	"github.com/mengqi777/ozone-go/api"
	"github.com/mengqi777/ozone-go/api/util"
	log "github.com/wonderivan/logger"
	"os"
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
	fileInfoOperator    *FileInfoOperator
	ozoneClient         *api.OzoneClient
	fsClient            *FSClient
	fileName            string
	path                string
	size                uint64
	checksumType        *hadoop_hdds_datanode.ChecksumType
	bytesPerChecksum    uint32
	chunkSize           uint64
	chunkQueueSize      int
	blockSize           uint64
	writers             []*BlockWriter
	currentIndex        int
	closed              bool
	flushChunkBatchSize int
}

func (fw *KeyWriter) allocateBlockIfNeed() (int, *BlockWriter, error) {
	index, _, err := fw.allocateBlock()
	if err != nil {
		return -1, nil, err
	}
	fw.currentIndex = index


	return fw.currentIndex, fw.writers[fw.currentIndex], nil

}

func (fw *KeyWriter) allocateBlock() (int, *BlockWriter, error) {

	blockLocation, err := fw.fsClient.AllocateBlock(fw.path, nil)
	if err != nil {
		return 0, nil, err
	}
	index, writer, err := fw.addBlockLocation(blockLocation)
	return index, writer, err
}

func (fw *KeyWriter) addBlockLocation(blockLocation *hadoop_ozone.BlockLocation) (int, *BlockWriter, error) {

	next := len(fw.writers)

	// Param: topology just same to ozone java client.
	pipeline, _ := NewPipelineOperator(blockLocation.Pipeline, false)
	log.Debug("AddBlockLocation pipeline: ", pipeline.GetPipeline().String())
	dn := NewDatanodeClient(pipeline)
	dn.callId = 0
	writer := newBlockWriter(
		BlockIdToDatanodeBlockId(blockLocation.BlockID),
		fw.checksumType,
		*blockLocation.Length,
		fw.bytesPerChecksum,
		fw.chunkSize,
		fw.flushChunkBatchSize,
		fw.chunkQueueSize,
		dn,
		pipeline,
	)
	fw.writers = append(fw.writers, writer)
	if err := dn.connectToLeaderRatis(dn.leaderIp); err != nil {
		return 0, nil, err
	}
	dn.watch(0, ratis_common.ReplicationLevel_MAJORITY)
	return next, writer, nil
}

func (fw *KeyWriter) Write(p []byte) (uint64, error) {

	if fw.closed {
		return 0, customerror.ErrClosedPipe
	}
	fw.size = uint64(len(p))
	//flushedWriteLen:= uint64(0)
	writtenLen := uint64(0)
	remain := uint64(len(p))
	endPos := uint64(0)
	for remain > 0 {
		_, current, err := fw.allocateBlockIfNeed()
		if err != nil {
			return 0, err
		}

		endPos = util.ComputeEndPos(remain, writtenLen, uint64(api.DEFAULT_BLOCK_SIZE), endPos)
		log.Debug("file currentIndex", fw.currentIndex, " remain", remain, "writtenLen", writtenLen, "endPos", endPos)
		n, err := current.Write(p[writtenLen:endPos])
		writtenLen += n
		remain -= n
		current.length = n

		if err != nil {
			continue
		}

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

	if err := fw.Close(); err != nil {
		log.Error("closed error", err)
		return writtenLen, err
	}

	fw.complete()
	return writtenLen, nil

}

func (fw *KeyWriter) Close() error {
	if fw.closed {
		return customerror.ErrClosedPipe
	}

	//err := fw.writers[fw.currentIndex].Close()

	fw.closed = true
	return nil
}

func (fw *KeyWriter) complete() {
	var blockLocations []*hadoop_ozone.BlockLocation
	for _, writer := range fw.writers {
		pipeline := writer.pipelineOperator.Pipeline
		location := &hadoop_ozone.BlockLocation{
			BlockID: &hadoop_hdds.BlockID{
				ContainerBlockID: &hadoop_hdds.ContainerBlockID{
					ContainerID: writer.blockId.ContainerID,
					LocalID:     writer.blockId.LocalID,
				},
				BlockCommitSequenceId: writer.blockId.BlockCommitSequenceId,
			},
			Offset:               &writer.offset,
			Length:               &writer.length,
			CreateVersion:        nil,
			Token:                nil,
			Pipeline:             pipeline,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		blockLocations = append(blockLocations, location)
	}

	err := fw.fsClient.CompleteFile(fw.path, blockLocations, fw.size)
	if err != nil {
		log.Error("Complete file error ", err)
		os.Exit(1)
	}
}

type BlockWriter struct {
	blockId          *hadoop_hdds_datanode.DatanodeBlockID
	blockData        *hadoop_hdds_datanode.BlockData
	dn               *DatanodeClient
	pipelineOperator *PipelineOperator
	blockSize        uint64
	length           uint64
	offset           uint64
	checksumType     *hadoop_hdds_datanode.ChecksumType
	bytesPerChecksum uint32
	chunkSize        uint64
	writers          []*ChunkWriter

	flushedMap          map[int]flushStatus
	flushChunkBatchSize int
	flushedIndex        int
	currentIndex        int
	closed              bool
}

func newBlockWriter(blockId *hadoop_hdds_datanode.DatanodeBlockID, checksumType *hadoop_hdds_datanode.ChecksumType, blockSize uint64,
	bytesPerChecksum uint32, chunkSize uint64, flushChunkBatchSize int, queueSize int, dn *DatanodeClient, pipeline *PipelineOperator) *BlockWriter {

	var metadata []*hadoop_hdds_datanode.KeyValue
	metadata = append(metadata, &hadoop_hdds_datanode.KeyValue{Key: proto.String("TYPE"), Value: proto.String("KEY")})
	blockData := &hadoop_hdds_datanode.BlockData{
		BlockID:  blockId,
		Metadata: metadata,
		Chunks:   make([]*hadoop_hdds_datanode.ChunkInfo, 0),
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
	}

}

func (bw *BlockWriter) allocateChunk() (int, *ChunkWriter) {
	nextIndex := len(bw.writers)
	chunkName := fmt.Sprintf("%d_chunk_%d", *bw.blockId.LocalID, nextIndex+1)
	writer := newChunkWriter(chunkName, bw.chunkSize, bw.blockId, bw.checksumType, bw.bytesPerChecksum, nextIndex, bw.dn)

	bw.writers = append(bw.writers, writer)

	bw.flushedMap[nextIndex] = flushedInitStatus
	return nextIndex, writer
}

func (bw *BlockWriter) currentWritableChunk() (int, *ChunkWriter) {

	var current *ChunkWriter
	if len(bw.writers) == 0 {
		bw.currentIndex, current = bw.allocateChunk()
		return bw.currentIndex, current
	}

	current = bw.writers[bw.currentIndex]
	if !current.HasRemaining() {
		bw.currentIndex, current = bw.allocateChunk()
		return bw.currentIndex, current
	}
	return bw.currentIndex, current
}

func (bw *BlockWriter) BlockId() *hadoop_hdds_datanode.DatanodeBlockID {
	return bw.blockId
}

func (bw *BlockWriter) Pipeline() *hadoop_hdds.Pipeline {
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
		if err != nil && err != customerror.BufferErrFull {
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
			if err = bw.dn.watch(uint64(bw.flushedIndex), ratis_common.ReplicationLevel_ALL_COMMITTED); err != nil {
				bw.dn.watch(uint64(bw.flushedIndex), ratis_common.ReplicationLevel_MAJORITY_COMMITTED)
			}
		}
		current.Close()
	}

	if writtenLen > flushedWrittenLen {
		if err := bw.putBlockAsync(true); err != nil {
			return flushedWrittenLen, err
		}
		flushedWrittenLen = writtenLen
		if err := bw.dn.watch(uint64(bw.flushedIndex), ratis_common.ReplicationLevel_ALL_COMMITTED); err != nil {
			bw.dn.watch(uint64(bw.flushedIndex), ratis_common.ReplicationLevel_MAJORITY_COMMITTED)
		}
	}

	return flushedWrittenLen, nil
}

func (bw *BlockWriter) shouldFlush() bool {
	return false
}

func (bw *BlockWriter) Close() error {

	if bw.closed {
		return customerror.ErrClosedPipe
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
			log.Debug("flush fail chunk ", i, bw.writers[i].chunkInfo.ChunkName)
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

	var chunks []*hadoop_hdds_datanode.ChunkInfo
	for _, writer := range bw.orderedFlushedChunks() {
		chunks = append(chunks, writer.Info())
	}
	if len(chunks) == 0 {
		return nil
	}

	var metadata []*hadoop_hdds_datanode.KeyValue
	metadata = append(metadata, &hadoop_hdds_datanode.KeyValue{Key: proto.String("TYPE"), Value: proto.String("KEY")})

	blockData := &hadoop_hdds_datanode.BlockData{
		BlockID:  bw.blockId,
		Metadata: metadata,
		Chunks:   chunks,
	}

	resp, _, err := bw.dn.PutBlock(blockData, eof)
	if err != nil {
		return err
	}
	bw.blockId = resp.CommittedBlockLength.BlockID
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
	blockId          *hadoop_hdds_datanode.DatanodeBlockID
	checksumType     *hadoop_hdds_datanode.ChecksumType
	chunkInfo        *hadoop_hdds_datanode.ChunkInfo
	bytesPerChecksum uint32
	chunkSize        uint64
	length           uint64
	buffer           *limitBuffer
	myIndex          int
	dn               *DatanodeClient
	closed           bool
	written          bool
}

func newChunkWriter(chunkName string, chunkSize uint64, blockId *hadoop_hdds_datanode.DatanodeBlockID,
	checksumType *hadoop_hdds_datanode.ChecksumType, bytesPerChecksum uint32, myIndex int, dn *DatanodeClient) *ChunkWriter {

	chunkInfo := &hadoop_hdds_datanode.ChunkInfo{
		ChunkName: proto.String(chunkName),
		Offset:    proto.Uint64(0),
		Len:       proto.Uint64(0),
	}

	return &ChunkWriter{
		blockId:          blockId,
		checksumType:     checksumType,
		bytesPerChecksum: bytesPerChecksum,
		chunkInfo:        chunkInfo,
		chunkSize:        chunkSize,
		length:           0,
		dn:               dn,
		myIndex:          myIndex,
		closed:           false,
		written:          false,
	}
}

func (cw *ChunkWriter) Info() *hadoop_hdds_datanode.ChunkInfo {
	return cw.chunkInfo
}

func (cw *ChunkWriter) verifyBuffer() {
	if cw.buffer == nil {
		cw.buffer = newLimitBuffer(cw.chunkSize)
	}
}

func (cw *ChunkWriter) checkWritable() error {
	if cw.closed {
		return customerror.ErrClosedPipe
	}

	if cw.written {
		return fmt.Errorf("chunk: %s has written to container", *cw.chunkInfo.ChunkName)
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
		return customerror.ErrClosedPipe
	}

	if !cw.written {
		if err := cw.WriteToContainer(); err != nil {
			return err
		}
	}

	cw.written = true
	cw.closed = true
	// Clear buffer when writer is closed
	cw.buffer = nil
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
	checksum := newChecksumOperatorComputer(cw.checksumType, cw.bytesPerChecksum)
	if err := checksum.ComputeChecksum(data); err != nil {
		return err
	}
	checksumData := checksum.Checksum

	cw.chunkInfo.ChecksumData = checksumData

	cw.chunkInfo.Offset = proto.Uint64(uint64(cw.myIndex) * api.DEFAULT_CHUNK_SIZE)
	cw.chunkInfo.Len = &cw.length

	_, _, err := cw.dn.WriteChunk(cw.blockId, cw.chunkInfo, data)

	cw.written = err == nil
	return err
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
		return 0, customerror.BufferErrFull
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
