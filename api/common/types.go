package common

import (
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	"github.com/mengqi777/ozone-go/api/proto/hdds"
	om_proto "github.com/mengqi777/ozone-go/api/proto/ozone"
)

type ReplicationType int
var CLUSTER_MODE string="STANDALONE"
const (
	RATIS      ReplicationType = 1
	STANDALONE ReplicationType = 2

	ACL_READ int8 =0
	//WRITE,
	//CREATE,
	//LIST,
	//DELETE,
	//READ_ACL,
	//WRITE_ACL,
	//ALL,
	//NONE;
)

var (
	DEFAULT_CHECKSUM_TYPE = dnapi.ChecksumType_CRC32

	DEFAULT_TOPOLOGY_AWARE_READ = false
	DEFAULT_VERIFY_CHECKSUM     = true

	DEFAULT_BLOCK_SIZE = uint64(268435456) //256MB

	//Means use BytesPerChecksum in chuck
	DEFAULT_BYTES_PER_CHECKSUM = uint32(1048576)

	DEFAULT_CHUNK_SIZE = uint64(4194304)

	DEFAULT_FLUSH_BUFF_SIZE  = int64(16777216)
	DEFAULT_BUFF_SIZE        = 4194304
	DEFAULT_BUFF_MAX_SIZE    = int64(33554432)
	DEFAULT_FLUSH_LENGTH     = 4
	DEFAULT_MAX_RETRY        = 10
	DEFAULT_CHUNK_QUEUE_SIZE = 4
	DEFAULT_OMHOST_PORT = 9862
)


var UserName string=""

type Volume struct {
	AdminName        string //  *string          `protobuf:"bytes,1,req,name=adminName" json:"adminName,omitempty"`
	OwnerName        string //   *string          `protobuf:"bytes,2,req,name=ownerName" json:"ownerName,omitempty"`
	Volume           string //   *string          `protobuf:"bytes,3,req,name=volume" json:"volume,omitempty"`
	QuotaInBytes     uint64 //  *uint64          `protobuf:"varint,4,opt,name=quotaInBytes" json:"quotaInBytes,omitempty"`
	CreationTime     string //  *uint64          `protobuf:"varint,7,opt,name=creationTime" json:"creationTime,omitempty"`
	ObjectID         uint64 //  *uint64          `protobuf:"varint,8,opt,name=objectID" json:"objectID,omitempty"`
	UpdateID         uint64 //  *uint64          `protobuf:"varint,9,opt,name=updateID" json:"updateID,omitempty"`
	ModificationTime string // *uint64          `protobuf:"varint,10,opt,name=modificationTime" json:"modificationTime,omitempty"`
	Acls             []Acl
}




type Acl struct {
	Type string
	Name string
	AclScope string
	AclList []byte
}

type Key struct {
	VolumeName        string
	BucketName        string
	Name              string
	ReplicationType   string
	ReplicationFactor hdds.ReplicationFactor
	DataSize          string
	CreationTime      string
	ModificationTime  string
	KeyLocationList    []*om_proto.KeyLocationList
}

type Bucket struct {
	VolumeName       string
	BucketName       string
	Versioning       bool
	StorageType      string
	CreationTime     string
	ObjectID         uint64
	UpdateID         uint64
	ModificationTime string
	SourceVolume     string
	SourceBucket     string
}

