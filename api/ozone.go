package api

import (
	"github.com/mengqi777/ozone-go/api/om"
)

type OzoneClient struct {
	OmClient *om.OmClient
}


func CreateOzoneClient(omhost string) *OzoneClient {
	client := om.CreateOmClient(omhost)
	return &OzoneClient{
		OmClient: &client,
	}
}
