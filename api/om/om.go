package om

import (
	"errors"
	"github.com/mengqi777/ozone-go/api/common"
	hadoop_ipc_client "github.com/mengqi777/ozone-go/api/gohadoop/hadoop_common/ipc/client"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
	ozone_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	uuid "github.com/nu7hatch/gouuid"
	log "github.com/wonderivan/logger"
)

var OM_PROTOCOL = "org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol"

type OmClient struct {
	OmHost   string
	client   *hadoop_ipc_client.Client
	ClientId string
}

func CreateOmClient(omhost string) OmClient {
	clientId, _ := uuid.NewV4()
	ugi, _ := common.CreateSimpleUGIProto()
	//net.JoinHostPort(omhost, strconv.Itoa(common.DEFAULT_OMHOST_PORT))
	c := &hadoop_ipc_client.Client{
		ClientId:      clientId,
		Ugi:           ugi,
		ServerAddress: omhost,
	}

	return OmClient{
		OmHost: omhost,
		client: c,
	}
}

func (om *OmClient) GetKey(volume string, bucket string, key string) (*ozone_proto.KeyInfo, error) {

	keyArgs := &ozone_proto.KeyArgs{
		VolumeName: &volume,
		BucketName: &bucket,
		KeyName:    &key,
	}
	req := ozone_proto.LookupKeyRequest{
		KeyArgs: keyArgs,
	}

	cmdType := ozone_proto.Type_LookupKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		LookupKeyRequest: &req,
		ClientId:         &om.ClientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	keyProto := resp.GetLookupKeyResponse().GetKeyInfo()

	return keyProto, nil
}

func (om *OmClient) ListKeys(volume string, bucket string, prefix string, startKey string) ([]*ozone_proto.KeyInfo, error) {
	return om.ListKeysPrefix(volume, bucket, prefix, startKey)

}

func (om *OmClient) AllocateBlock(volume string, bucket string, key string, clientID *uint64) (*ozone_proto.AllocateBlockResponse, error) {
	req := ozone_proto.AllocateBlockRequest{
		KeyArgs: &ozone_proto.KeyArgs{
			VolumeName: &volume,
			BucketName: &bucket,
			KeyName:    &key,
		},
		ClientID: clientID,
	}

	cmdType := ozone_proto.Type_AllocateBlock
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:              &cmdType,
		AllocateBlockRequest: &req,
		ClientId:             &om.ClientId,
	}
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	return resp.AllocateBlockResponse, nil
}

func (om *OmClient) CreateKey(volume string, bucket string, key string) (*ozone_proto.CreateKeyResponse, error) {
	req := ozone_proto.CreateKeyRequest{
		KeyArgs: &ozone_proto.KeyArgs{
			VolumeName: &volume,
			BucketName: &bucket,
			KeyName:    &key,
		},
	}

	cmdType := ozone_proto.Type_CreateKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		CreateKeyRequest: &req,
		ClientId:         &om.ClientId,
	}
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}

	return resp.CreateKeyResponse, nil
}

func (om *OmClient) CreateKeyRaft(volume string, bucket string, key string) (*ozone_proto.CreateKeyResponse, error) {
	req := ozone_proto.CreateKeyRequest{
		KeyArgs: &ozone_proto.KeyArgs{
			VolumeName:         &volume,
			BucketName:         &bucket,
			KeyName:            &key,
			Type:               hdds.ReplicationType_RATIS.Enum(),
			Factor:             hdds.ReplicationFactor_THREE.Enum(),
		},
	}

	cmdType := ozone_proto.Type_CreateKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		CreateKeyRequest: &req,
		ClientId:         &om.ClientId,
	}
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}

	return resp.CreateKeyResponse, nil
}

func (om *OmClient) CommitKey(volume string, bucket string, key string, id *uint64, keyLocations []*ozone_proto.KeyLocation, size uint64) (common.Key, error) {
	one := hdds.ReplicationFactor_ONE
	standalone := hdds.ReplicationType_STAND_ALONE
	req := ozone_proto.CommitKeyRequest{
		KeyArgs: &ozone_proto.KeyArgs{
			VolumeName:   &volume,
			BucketName:   &bucket,
			KeyName:      &key,
			KeyLocations: keyLocations,
			DataSize:     &size,
			Factor:       &one,
			Type:         &standalone,
		},
		ClientID: id,
	}

	cmdType := ozone_proto.Type_CommitKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		CommitKeyRequest: &req,
		ClientId:         &om.ClientId,
	}
	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return common.Key{}, err
	}

	return common.Key{}, nil
}

func (om *OmClient) CommitKeyRaft(volume string, bucket string, key string, id *uint64, keyLocations []*ozone_proto.KeyLocation, size uint64) (common.Key, error) {
	three := hdds.ReplicationFactor_THREE
	ratis := hdds.ReplicationType_RATIS
	req := ozone_proto.CommitKeyRequest{
		KeyArgs: &ozone_proto.KeyArgs{
			VolumeName:   &volume,
			BucketName:   &bucket,
			KeyName:      &key,
			KeyLocations: keyLocations,
			DataSize:     &size,
			Factor:       &three,
			Type:         &ratis,
		},
		ClientID: id,
	}

	cmdType := ozone_proto.Type_CommitKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		CommitKeyRequest: &req,
		ClientId:         &om.ClientId,
	}
	_, err := om.submitRequest(&wrapperRequest)

	if err != nil {
		return common.Key{}, err
	}

	return common.Key{}, nil
}

func (om *OmClient) ListKeysPrefix(volume string, bucket string, prefix string, startKey string) ([]*ozone_proto.KeyInfo, error) {

	req := ozone_proto.ListKeysRequest{
		VolumeName: &volume,
		BucketName: &bucket,
		StartKey:   ptr(startKey),
		Prefix:     ptr(prefix),
		Count:      ptri(1000),
	}

	cmdType := ozone_proto.Type_ListKeys
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:         &cmdType,
		ListKeysRequest: &req,
		ClientId:        &om.ClientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}

	return resp.GetListKeysResponse().GetKeyInfo(), nil
}

func (om *OmClient) DeleteKey(v string, b string, k string) error {
	keyArgs := &ozone_proto.KeyArgs{
		VolumeName: ptr(v),
		BucketName: ptr(b),
		KeyName:    ptr(k),
	}
	req := ozone_proto.DeleteKeyRequest{
		KeyArgs: keyArgs,
	}

	cmdType := ozone_proto.Type_DeleteKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		DeleteKeyRequest: &req,
		ClientId:         &om.ClientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error(resp.String())
		return err
	}
	return nil
}

func (om *OmClient) DeleteKeys(v string, b string, keys []string) error {

	deleteKeys:=&ozone_proto.DeleteKeyArgs{
		VolumeName: ptr(v),
		BucketName: ptr(b),
		Keys:       keys,
	}
	req := ozone_proto.DeleteKeysRequest{
		DeleteKeys: deleteKeys,
	}

	cmdType := ozone_proto.Type_DeleteKeys
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		DeleteKeysRequest: &req,
		ClientId:         &om.ClientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error(resp.String())
		return err
	}
	return nil
}


func (om *OmClient) Rename(v,b,fromKey,toKey string) error {
	req:=ozone_proto.RenameKeyRequest{
		KeyArgs:   &ozone_proto.KeyArgs{
			VolumeName:         ptr(v),
			BucketName:         ptr(b),
			KeyName:            ptr(fromKey),
		},
		ToKeyName: ptr(toKey),
	}


	cmdType := ozone_proto.Type_RenameKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &cmdType,
		RenameKeyRequest: &req,
		ClientId:         &om.ClientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error(resp.String())
		return err
	}
	return nil
}


func getRpcPort(ports []*hdds.Port) uint32 {
	for _, port := range ports {
		if port.GetName() == "RATIS" {
			return port.GetValue()
		}
	}
	return 0
}

func ptri(i int32) *int32 {
	return &i
}

func ptr(s string) *string {
	return &s
}

func (om *OmClient) submitRequest(request *ozone_proto.OMRequest, ) (*ozone_proto.OMResponse, error) {
	wrapperResponse := ozone_proto.OMResponse{}
	err := om.client.Call(common.GetCalleeRPCRequestHeaderProto(&OM_PROTOCOL), request, &wrapperResponse)
	if err != nil {
		return nil, err
	}
	if *wrapperResponse.Status != ozone_proto.Status_OK {
		return nil, errors.New("Error on calling OM " + wrapperResponse.Status.String() + " " + *wrapperResponse.Message)
	}
	return &wrapperResponse, nil
}

