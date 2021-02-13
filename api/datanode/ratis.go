package datanode

import (
	"github.com/mengqi777/ozone-go/api/proto/ratis"
)


func (dc *DatanodeClient) raftSendAndRecv(req *ratis.RaftClientRequestProto) (*ratis.RaftClientReplyProto, error) {
	err := (*dc.ratisClient).Send(req)

	if err != nil {
		return nil, err
	}
	//
	//go func() {
	//	time.Sleep(60 * time.Second)
	//}()

	reply, err := (*dc.ratisClient).Recv()
	if err != nil {
		return nil, err
	}
	return reply, err
}

