package checksum

import (
	"crypto/md5"
	"crypto/sha256"
	"github.com/mengqi777/ozone-go/api/util"
	"hash/crc32"
)

func CRC32(data []byte) []byte {
	var b = make([]byte, 8)
	util.Uint64toBytes(b, uint64(crc32.ChecksumIEEE(data)))
	return b
}

func CRC32c(data []byte) []byte {
	var b = make([]byte, 8)
	t := crc32.MakeTable(crc32.Castagnoli)
	util.Uint64toBytes(b, uint64(crc32.Checksum(data, t)))
	return b
}

func SHA256(data []byte) []byte {
	c := sha256.New()
	c.Write(data)
	return c.Sum(nil)
}

func MD5(data []byte) []byte {
	c := md5.New()
	c.Write(data)
	return c.Sum(nil)
}
