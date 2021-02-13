package om

import (
	"github.com/mengqi777/ozone-go/api/common"
	om_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	"github.com/mengqi777/ozone-go/api/util"
	log "github.com/wonderivan/logger"
)

func (om *OmClient) CreateBucket(volume string, bucket string) error {
	isVersionEnabled := false
	storageType := om_proto.StorageTypeProto_DISK
	bucketInfo := om_proto.BucketInfo{
		BucketName:       &bucket,
		VolumeName:       &volume,
		IsVersionEnabled: &isVersionEnabled,
		StorageType:      &storageType,
	}
	req := om_proto.CreateBucketRequest{
		BucketInfo: &bucketInfo,
	}

	cmdType := om_proto.Type_CreateBucket
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:             &cmdType,
		CreateBucketRequest: &req,
		ClientId:            &clientId,
	}

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}

func (om *OmClient) ListBuckets(volume string, prefix string, startKey string) []common.Bucket {
	count := int32(1000)
	req := om_proto.ListBucketsRequest{
		VolumeName: &volume,
		StartKey:   &startKey,
		Prefix:     &prefix,
		Count:      &count,
	}

	cmdType := om_proto.Type_ListBuckets
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:            &cmdType,
		ListBucketsRequest: &req,
		ClientId:           &clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error("list buckets", req.String(), "response", err)
		return nil
	}
	bukets:=make([] common.Bucket,0)
	for _, proto := range resp.GetListBucketsResponse().GetBucketInfo() {
		bukets=append(bukets, BucketFromProto(proto))
	}
	return bukets
}


func (om *OmClient) InfoBucket(volume string, bucket string) (common.Bucket,error) {
	req := om_proto.InfoBucketRequest{
		VolumeName: &volume,
		BucketName: &bucket,
	}

	cmdType := om_proto.Type_InfoBucket
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:            &cmdType,
		InfoBucketRequest: &req,
		ClientId:           &clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error("Info buckets", req.String(), "response", err)
		return common.Bucket{},err
	}
	return BucketFromProto(resp.InfoBucketResponse.BucketInfo),nil
}

func (om *OmClient) DeleteBucket(volume string, bucket string) error {
	req := om_proto.DeleteBucketRequest{
		VolumeName: &volume,
		BucketName: &bucket,
	}

	cmdType := om_proto.Type_InfoBucket
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:            &cmdType,
		DeleteBucketRequest: &req,
		ClientId:           &clientId,
	}
	_, err := om.submitRequest(&wrapperRequest)
	return err
}


func BucketFromProto(proto *om_proto.BucketInfo)  common.Bucket {
	return  common.Bucket{
		VolumeName:       proto.GetVolumeName(),
		BucketName:       proto.GetBucketName(),
		Versioning:       proto.GetIsVersionEnabled(),
		StorageType:      proto.GetStorageType().String(),
		CreationTime:     util.ParseTimeFromMills(int64(proto.GetCreationTime())),
		ObjectID:         proto.GetObjectID(),
		UpdateID:         proto.GetUpdateID(),
		ModificationTime: util.ParseTimeFromMills(int64(proto.GetModificationTime())),
		SourceVolume:     proto.GetSourceVolume(),
		SourceBucket:     proto.GetSourceBucket(),
	}
}
