package datanode

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/golang/protobuf/proto"
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
	"github.com/mengqi777/ozone-go/api/proto/ratis"
	"github.com/mengqi777/ozone-go/api/util"
	log "github.com/wonderivan/logger"
	"google.golang.org/grpc"
	"io"
	"strconv"
	"strings"
)

type ChunkInfo struct {
	Name         string
	Offset       int64
	Len          int64
	ChecksumData *dnapi.ChecksumData
}

type DatanodeClient struct {
	ratisClient        *ratis.RaftClientProtocolService_UnorderedClient
	ratisReceiver      chan ratis.RaftClientReplyProto
	standaloneClient   *dnapi.XceiverClientProtocolService_SendClient
	standaloneReceiver chan dnapi.ContainerCommandResponseProto

	ctx             context.Context
	datanodes       []*hdds.DatanodeDetailsProto
	currentDatanode hdds.DatanodeDetailsProto
	grpcConnection  *grpc.ClientConn
	pipelineId      *hdds.PipelineID
	memberIndex     int
	seqNum          uint64
	callId          uint64
	reqId           string
	replyId         string
	groupId         []byte
}

func (dn *DatanodeClient) GetCurrentDnUUid() *string {
	uid := dn.currentDatanode.GetUuid()
	return &uid
}

func (dnClient *DatanodeClient) connectToNextStandalone() error {
	if dnClient.grpcConnection != nil {
		dnClient.grpcConnection.Close()
	}
	dnClient.memberIndex = dnClient.memberIndex + 1
	if dnClient.memberIndex == len(dnClient.datanodes) {
		dnClient.memberIndex = 0
	}
	selectedDatanode := dnClient.datanodes[dnClient.memberIndex]
	dnClient.currentDatanode = *selectedDatanode

	standalonePort := 0
	for _, port := range dnClient.currentDatanode.Ports {
		if *port.Name == "STANDALONE" {
			standalonePort = int(*port.Value)
		}
	}

	address := *dnClient.currentDatanode.IpAddress + ":" + strconv.Itoa(standalonePort)
	println("Connecting to the standalone " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}

	dnClient.ratisReceiver = make(chan ratis.RaftClientReplyProto)
	dnClient.standaloneReceiver = make(chan dnapi.ContainerCommandResponseProto)
	maxSizeOption := grpc.MaxCallRecvMsgSize(32 * 10e6)
	maxSendSizeOption := grpc.MaxCallSendMsgSize(32 * 10e6)

	client, err := dnapi.NewXceiverClientProtocolServiceClient(conn).Send(dnClient.ctx, maxSizeOption,maxSendSizeOption)
	dnClient.standaloneClient = &client

	go dnClient.StandaloneReceive()
	return nil
}

func (dnClient *DatanodeClient) connectToLeaderRaft( address string) error {
	if dnClient.grpcConnection != nil {
		dnClient.grpcConnection.Close()
	}

	println("Connecting to the ratis " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}

	dnClient.ratisReceiver = make(chan ratis.RaftClientReplyProto)
	dnClient.standaloneReceiver = make(chan dnapi.ContainerCommandResponseProto)
	maxSizeOption := grpc.MaxCallRecvMsgSize(32 * 10e6)
	maxSendSizeOption := grpc.MaxCallSendMsgSize(32 * 10e6)

	client, err := ratis.NewRaftClientProtocolServiceClient(conn).Unordered(dnClient.ctx, maxSizeOption,maxSendSizeOption)
	if err != nil {
		panic(err)
	}
	dnClient.ratisClient = &client
	//go dnClient.RaftReceiver()
	return nil
}

func CreateDatanodeClient(pipeline *hdds.Pipeline) (*DatanodeClient, error) {
	dnClient := &DatanodeClient{
		ctx:         context.Background(),
		pipelineId:  pipeline.Id,
		datanodes:   pipeline.Members,
		memberIndex: -1,
	}
	err := dnClient.connectToNextStandalone()
	if err != nil {
		return nil, err
	}
	return dnClient, nil
}


func CreateDatanodeClientRaft(pipeline *hdds.Pipeline) *DatanodeClient {


	id :=uuid.New().String()[0:16]
	rid, _ := hex.DecodeString(strings.ReplaceAll(pipeline.GetId().GetId(), "-", ""))
	dnClient := &DatanodeClient{
		ctx:         context.Background(),
		pipelineId:  pipeline.Id,
		datanodes:   pipeline.Members,
		memberIndex: -1,
		callId: 0,
		seqNum: 1,
		groupId: rid,
		reqId:id,
		replyId: id,
	}

	return dnClient
}

func (dnClient *DatanodeClient) RaftReceiver() {
	for {
		proto, err := (*dnClient.ratisClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		dnClient.ratisReceiver <- *proto
	}
}


func (dnClient *DatanodeClient) watch(index uint64,replicationLevel ratis.ReplicationLevel) error {
	//init watch 0,ratis.ReplicationLevel_MAJORITY
	//first watch index,ratis.ReplicationLevel_ALL_COMMITTED
	//last watch index,ratis.ReplicationLevel_ALL_COMMITTED
	//exception index,ratis.ReplicationLevel_MAJORITY_COMMITTED

	Watch := &ratis.RaftClientRequestProto_Watch{Watch: &ratis.WatchRequestTypeProto{
		Index:       index,
		Replication: replicationLevel,
	}}
	req := &ratis.RaftClientRequestProto{
		RpcRequest: &ratis.RaftRpcRequestProto{
			RequestorId: []byte(dnClient.reqId),
			ReplyId:     []byte(dnClient.replyId),
			RaftGroupId: &ratis.RaftGroupIdProto{
				Id: dnClient.groupId,
			},
			CallId: uint64(dnClient.callId),
			SlidingWindowEntry: &ratis.SlidingWindowEntry{
				SeqNum:  dnClient.seqNum,
				IsFirst: true,
			},
		},
		Message: nil,
		Type:    Watch,
	}
	reply, err := dnClient.raftSendAndRecv(req)
	if err != nil {
		log.Error("raft send and recv error", err)
		log.Fatal(err)
		return err
	}
	log.Debug("Watch request", req.String())
	dnClient.seqNum += 1
	dnClient.callId += 1
	log.Debug("Watch response", reply.String())
	return nil
}


func (dnClient *DatanodeClient) CreateAndWriteChunk(id *dnapi.DatanodeBlockID, blockOffset uint64, buffer []byte, length uint64) (dnapi.ChunkInfo, error) {
	bpc := uint32(12)
	checksumType := dnapi.ChecksumType_NONE
	checksumDataProto := dnapi.ChecksumData{
		Type:             &checksumType,
		BytesPerChecksum: &bpc,
	}
	chunkName := fmt.Sprintf("chunk_%d", blockOffset)
	chunkInfoProto := dnapi.ChunkInfo{
		ChunkName:    &chunkName,
		Offset:       &blockOffset,
		Len:          &length,
		ChecksumData: &checksumDataProto,
	}
	return dnClient.WriteChunk(id, chunkInfoProto, buffer[0:length])
}

func (dnClient *DatanodeClient) WriteChunk(id *dnapi.DatanodeBlockID, info dnapi.ChunkInfo, data []byte) (dnapi.ChunkInfo, error) {

	req := dnapi.WriteChunkRequestProto{
		BlockID:   id,
		ChunkData: &info,
		Data:      data,
	}
	commandType := dnapi.Type_WriteChunk
	uuid := dnClient.currentDatanode.GetUuid()
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		WriteChunk:   &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: &uuid,
	}

	_, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return info, err
	}
	return info, nil
}


func (dnClient *DatanodeClient) WriteChunkRaft(id *dnapi.DatanodeBlockID, info *dnapi.ChunkInfo, data []byte) (*dnapi.ChunkInfo, error) {

	req := dnapi.WriteChunkRequestProto{
		BlockID:   id,
		ChunkData:info,
		Data:      data,
	}
	commandType := dnapi.Type_WriteChunk
	uuid := dnClient.currentDatanode.GetUuid()
	proto := &dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		WriteChunk:   &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: &uuid,
	}

	_, err := dnClient.raftSend(proto)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (dnClient *DatanodeClient) ReadChunk(id *dnapi.DatanodeBlockID, info ChunkInfo) ([]byte, error) {
	result := make([]byte, 0)

	bpc := info.ChecksumData.GetBytesPerChecksum()
	checksumType := info.ChecksumData.GetType()
	checksumDataProto := dnapi.ChecksumData{
		Type:             &checksumType,
		BytesPerChecksum: &bpc,
	}
	offset := uint64(info.Offset)
	length := uint64(info.Len)
	chunkInfoProto := dnapi.ChunkInfo{
		ChunkName:    &info.Name,
		Offset:       &offset,
		Len:          &length,
		ChecksumData: &checksumDataProto,
	}
	req := dnapi.ReadChunkRequestProto{
		BlockID:   id,
		ChunkData: &chunkInfoProto,
	}
	commandType := dnapi.Type_ReadChunk
	uuid := dnClient.currentDatanode.GetUuid()
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		ReadChunk:    &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: &uuid,
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		log.Error(err)
		return result, err
	}
	checksumOp := newChecksumOperator(info.ChecksumData)
	if err = checksumOp.VerifyChecksum(resp.GetReadChunk().GetData(), uint64(0)); err != nil {
		log.Error(err)
		return result, err
	}

	if resp.GetResult() != dnapi.Result_SUCCESS {
		return nil, errors.New(resp.GetResult().String() + " " + resp.GetMessage())
	}

	return resp.GetReadChunk().Data, nil
}

func (dnClient *DatanodeClient) PutBlock(id *dnapi.DatanodeBlockID, chunks []*dnapi.ChunkInfo) error {

	flags := int64(0)
	req := dnapi.PutBlockRequestProto{
		BlockData: &dnapi.BlockData{
			BlockID:  id,
			Flags:    &flags,
			Metadata: make([]*dnapi.KeyValue, 0),
			Chunks:   chunks,
		},
	}
	commandType := dnapi.Type_PutBlock
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		PutBlock:     &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: dnClient.GetCurrentDnUUid(),
	}

	_, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return err
	}
	return nil
}

func (dnClient *DatanodeClient) PutBlockRatis(id *dnapi.DatanodeBlockID, chunks []*dnapi.ChunkInfo) (*dnapi.ContainerCommandResponseProto, error) {

	flags := int64(0)
	req := dnapi.PutBlockRequestProto{
		BlockData: &dnapi.BlockData{
			BlockID:  id,
			Flags:    &flags,
			Metadata: make([]*dnapi.KeyValue, 0),
			Chunks:   chunks,
		},
	}
	commandType := dnapi.Type_PutBlock
	proto := &dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		PutBlock:     &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: dnClient.GetCurrentDnUUid(),
	}


	reply, err := dnClient.raftSend(proto)
	if err != nil {
		return nil,err
	}
	return reply,nil
}


func (dc *DatanodeClient) raftSend(req *dnapi.ContainerCommandRequestProto) (*dnapi.ContainerCommandResponseProto, error) {

	raftRpcReq := &ratis.RaftRpcRequestProto{
		RequestorId: []byte(dc.reqId),
		ReplyId:     []byte(dc.replyId),
		RaftGroupId: &ratis.RaftGroupIdProto{
			Id: dc.groupId,
		},
		CallId: dc.callId,
		SlidingWindowEntry: &ratis.SlidingWindowEntry{
			SeqNum:               dc.seqNum,
			IsFirst:              false,
		},
	}
	message := toMessage(req)
	var raftClientReq = &ratis.RaftClientRequestProto{RpcRequest: raftRpcReq,
		Message: message}
	if util.IsReadOnly(req.GetCmdType().String()) {
		raftClientReq.Type = &ratis.RaftClientRequestProto_Read{Read: &ratis.ReadRequestTypeProto{}}
	} else {
		raftClientReq.Type = &ratis.RaftClientRequestProto_Write{Write: &ratis.WriteRequestTypeProto{}}
	}
	reply, err := dc.raftSendAndRecv(raftClientReq)
	if err != nil {
		log.Error("raft send and recv error", err)
		return nil, err
	}

	resp := dnapi.ContainerCommandResponseProto{}
	err = proto.Unmarshal(reply.Message.GetContent(), &resp)
	if err != nil {
		return nil, err
	}
	dc.seqNum += 1
	dc.callId += 1
	return &resp, err

}


func (dnClient *DatanodeClient) GetBlock(id *dnapi.DatanodeBlockID) ([]ChunkInfo, error) {
	result := make([]ChunkInfo, 0)

	req := dnapi.GetBlockRequestProto{
		BlockID: id,
	}
	commandType := dnapi.Type_GetBlock
	proto := dnapi.ContainerCommandRequestProto{
		CmdType:      &commandType,
		GetBlock:     &req,
		ContainerID:  id.ContainerID,
		DatanodeUuid: dnClient.GetCurrentDnUUid(),
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return result, err
	}
	for _, chunkInfo := range resp.GetGetBlock().GetBlockData().Chunks {
		result = append(result, ChunkInfo{
			Name:         chunkInfo.GetChunkName(),
			Offset:       int64(chunkInfo.GetOffset()),
			Len:          int64(chunkInfo.GetLen()),
			ChecksumData: chunkInfo.GetChecksumData(),
		})
	}
	return result, nil
}

func (dnClient *DatanodeClient) sendDatanodeCommand(proto dnapi.ContainerCommandRequestProto) (dnapi.ContainerCommandResponseProto, error) {
	return dnClient.sendStandaloneDatanodeCommand(proto)
}

func (dn *DatanodeClient) Close() {
	(*dn.standaloneClient).CloseSend()
}

func BlockIdToDatanodeBlockId(blockId *hdds.BlockID) *dnapi.DatanodeBlockID {
	return &dnapi.DatanodeBlockID{
		ContainerID:           blockId.ContainerBlockID.ContainerID,
		LocalID:               blockId.ContainerBlockID.LocalID,
		BlockCommitSequenceId: blockId.BlockCommitSequenceId,
	}
}



func toMessage(req *dnapi.ContainerCommandRequestProto) *ratis.ClientMessageEntryProto {
	data := make([]byte, 0)
	if req.GetCmdType().String() == dnapi.Type_WriteChunk.String() {
		data = req.WriteChunk.GetData()
		req.WriteChunk.Data = nil
	} else if req.GetCmdType().String() == dnapi.Type_PutSmallFile.String() {
		data = req.PutSmallFile.GetData()
		req.PutSmallFile.Data = nil
	}
	bytes, err := proto.Marshal(req)
	if err != nil {
		log.Debug("request: ", req.String())
		log.Fatal("marshal error", err)
	}
	lengthHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthHeader, uint32(len(bytes)))
	content := append(lengthHeader, bytes...)
	message := &ratis.ClientMessageEntryProto{
		Content:              append(content, data...),
	}
	return message
}
