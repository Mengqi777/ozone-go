package om

import (
	"github.com/mengqi777/ozone-go/api/common"
	om_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
	"github.com/mengqi777/ozone-go/api/util"
	uuid "github.com/nu7hatch/gouuid"
	log "github.com/wonderivan/logger"
)

func (om *OmClient) ListVolumes(length int, user string) ([]common.Volume, error) {
	scope := om_proto.ListVolumeRequest_VOLUMES_BY_CLUSTER
	if user != "" {
		scope = om_proto.ListVolumeRequest_VOLUMES_BY_USER
	}
	count := uint32(length)
	req := om_proto.ListVolumeRequest{
		Scope:    &scope,
		UserName: ptr(user),
		MaxKeys:  &count,
	}
	traceId, _ := uuid.NewV4()
	tid := traceId.String()
	listKeys := om_proto.Type_ListVolume
	clientId := "goClient" + tid

	wrapperRequest := om_proto.OMRequest{
		CmdType:           &listKeys,
		ListVolumeRequest: &req,
		ClientId:          &clientId,
		TraceID:           &tid,
	}

	volumes := make([]common.Volume, 0)
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	for _, volProto := range resp.GetListVolumeResponse().GetVolumeInfo() {
		volumes = append(volumes,VolumeFromProto(volProto))
	}
	return volumes, nil
}

func (om *OmClient) CreateVolume(name string) error {
	onegig := uint64(1024 * 1024 * 1024)
	aclUser:=om_proto.OzoneAclInfo{
		Type:     om_proto.OzoneAclInfo_USER.Enum(),
		Name:     &name,
		Rights: []byte{128},
		AclScope: om_proto.OzoneAclInfo_ACCESS.Enum(),
	}

	aclGroup:=om_proto.OzoneAclInfo{
		Type:     om_proto.OzoneAclInfo_GROUP.Enum(),
		Name:     &name,
		Rights: []byte{128},
		AclScope: om_proto.OzoneAclInfo_ACCESS.Enum(),
	}
	volumeInfo := om_proto.VolumeInfo{
		AdminName:    ptr(common.UserName),
		OwnerName:    ptr(common.UserName),
		Volume:       ptr(name),
		QuotaInBytes: &onegig,
		VolumeAcls:   []*om_proto.OzoneAclInfo{&aclUser,&aclGroup},
	}
	req := om_proto.CreateVolumeRequest{
		VolumeInfo: &volumeInfo,
	}

	cmdType := om_proto.Type_CreateVolume
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:             &cmdType,
		CreateVolumeRequest: &req,
		ClientId:            &clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	log.Info(resp.String())
	return nil
}

func (om *OmClient) DeleteVolume(volume string) error {

	req := om_proto.DeleteVolumeRequest{VolumeName: &volume}

	cmdType := om_proto.Type_DeleteVolume
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:             &cmdType,
		DeleteVolumeRequest: &req,
		ClientId:            &clientId,
	}

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}

func (om *OmClient) InfoVolume(volume string) (common.Volume, error) {

	req := om_proto.InfoVolumeRequest{VolumeName: &volume}

	cmdType := om_proto.Type_InfoVolume
	clientId := "goClient"
	wrapperRequest := om_proto.OMRequest{
		CmdType:           &cmdType,
		InfoVolumeRequest: &req,
		ClientId:          &clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		log.Error(err)
		return common.Volume{}, err
	}
	return VolumeFromProto(resp.GetInfoVolumeResponse().GetVolumeInfo()), nil
}

func VolumeFromProto(proto *om_proto.VolumeInfo) common.Volume {
	acls:=make([]common.Acl,len(proto.GetVolumeAcls()))
	aclProtos:=proto.GetVolumeAcls();
	for i, aclProto := range aclProtos {
		acl:=common.Acl{
			Type:     aclProto.GetType().String(),
			Name:     aclProto.GetName(),
			AclScope: aclProto.GetAclScope().String(),
			AclList:  aclProto.GetRights(),
		}
		acls[i]=acl
	}

	return common.Volume{
		AdminName:        proto.GetAdminName(),
		OwnerName:        proto.GetOwnerName(),
		Volume:           proto.GetVolume(),
		QuotaInBytes:     proto.GetQuotaInBytes(),
		CreationTime:     util.ParseTimeFromMills(int64(proto.GetCreationTime())),
		ObjectID:         proto.GetObjectID(),
		UpdateID:         proto.GetUpdateID(),
		ModificationTime: util.ParseTimeFromMills(int64(proto.GetModificationTime())),
		Acls: acls,
	}
}
