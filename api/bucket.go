package api

import (
	"github.com/mengqi777/ozone-go/api/common"
)


func (ozoneClient *OzoneClient) CreateBucket(volume string, bucket string) error {
	return ozoneClient.OmClient.CreateBucket(volume, bucket)
}

func (ozoneClient *OzoneClient) ListBuckets(volume string, prefix string,startKey string) [] common.Bucket {
	return ozoneClient.OmClient.ListBuckets(volume, prefix,startKey)

}

func (ozoneClient *OzoneClient) InfoBucket(volume string, bucket string) (common.Bucket,error) {
	return ozoneClient.OmClient.InfoBucket(volume, bucket)
}


func (ozoneClient *OzoneClient) DeleteBuckets(volume string, bucket string) error{
	return ozoneClient.OmClient.DeleteBucket(volume, bucket)
}
