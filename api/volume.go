package api

import "github.com/mengqi777/ozone-go/api/common"

func (ozoneClient *OzoneClient) ListVolumes(length int,user string) ([]common.Volume, error) {
	return ozoneClient.OmClient.ListVolumes(length,user)
}

func (ozoneClient *OzoneClient) CreateVolume(name string) error {
	return ozoneClient.OmClient.CreateVolume(name)
}

func (ozoneClient *OzoneClient) DeleteVolume(volume string) error {
	return ozoneClient.OmClient.DeleteVolume(volume)
}

func (ozoneClient *OzoneClient) InfoVolume(volume string) (common.Volume,error) {
	return ozoneClient.OmClient.InfoVolume(volume)
}

