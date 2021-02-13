package datanode

import (
	"fmt"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
)
type PipelinePortName string

const (
	PipelinePortNameREST       PipelinePortName = "REST"
	PipelinePortNameRATIS      PipelinePortName = "RATIS"
	PipelinePortNameSTANDALONE PipelinePortName = "STANDALONE"
)

type PipelineOperator struct {
	topology     bool
	currentIndex int
	Pipeline     *hdds.Pipeline
}

func NewPipelineOperator(pipeline *hdds.Pipeline, topology bool) (*PipelineOperator, error) {
	operator := &PipelineOperator{Pipeline: pipeline, topology: topology, currentIndex: 0}

	memberLen := len(operator.Pipeline.Members)
	//memberOrderLen := len(operator.Pipeline.MemberOrders)

	switch {
	case memberLen == 0:
		return nil, fmt.Errorf("members of Pipeline is empty. Pipeline: %v", pipeline.String())
		//case memberOrderLen == 0:
		//	return nil, fmt.Errorf("memberOrders of Pipeline is empty. Pipeline: %v", pipeline.String())
		//case memberOrderLen != memberLen:
		//	return nil, fmt.Errorf("members chunkSize: %v not match memberOrders chunkSize: %v, Pipeline: %v", memberLen, memberOrderLen, pipeline.String())
	}

	return operator, nil

}

func (operator *PipelineOperator) GetPipeline() *hdds.Pipeline {
	return operator.Pipeline
}

func (operator *PipelineOperator) GetId() string {
	return *operator.Pipeline.Id.Id
}

func (operator *PipelineOperator) GetNodeAddr(portName PipelinePortName, firstNode bool) string {

	var dn *hdds.DatanodeDetailsProto
	if firstNode {
		dn = operator.GetFirstNode()
	} else {
		dn = operator.GetClosestNode()
	}

	for _, port := range dn.Ports {
		if *port.Name == string(portName) {
			return fmt.Sprintf("%v:%v", *dn.IpAddress, *port.Value)
		}
	}
	return ""
}

func (operator *PipelineOperator) GetLeaderAddr(portName PipelinePortName) string {

	var dn =operator.GetLeaderNode()

	for _, port := range dn.Ports {
		if *port.Name == string(portName) {
			return fmt.Sprintf("%v:%v", *dn.IpAddress, *port.Value)
		}
	}
	return ""
}

func (operator *PipelineOperator) SetCurrentToNext() {
	if operator.currentIndex+1 >= len(operator.GetPipeline().Members) {
		operator.currentIndex = 0
	} else {
		operator.currentIndex += 1
	}
}

func (operator *PipelineOperator) GetCurrentNode() *hdds.DatanodeDetailsProto {

	if operator.currentIndex >= len(operator.GetPipeline().Members) {
		operator.currentIndex = 0
	}

	if operator.topology {
		return operator.Pipeline.Members[operator.Pipeline.MemberOrders[operator.currentIndex]]
	} else {
		return operator.Pipeline.Members[operator.currentIndex]
	}
}

func (operator *PipelineOperator) GetFirstNode() *hdds.DatanodeDetailsProto {
	return operator.Pipeline.Members[0]
}

func (operator *PipelineOperator) GetLeaderNode() *hdds.DatanodeDetailsProto {
	leaderId:= operator.Pipeline.GetLeaderID()
	for _,pp:=range operator.GetPipeline().GetMembers(){
		if leaderId==pp.GetUuid(){
			return pp
		}
	}
	return operator.GetFirstNode()
}

func (operator *PipelineOperator) GetClosestNode() *hdds.DatanodeDetailsProto {
	return operator.Pipeline.Members[operator.Pipeline.MemberOrders[0]]
}

