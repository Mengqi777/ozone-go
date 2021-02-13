package common

import (
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	dnapi "github.com/mengqi777/ozone-go/api/proto/datanode"
	"github.com/mengqi777/ozone-go/api/util/checksum"
)

type checksumFunc func(data []byte) []byte

type ChecksumOperator struct {
	Checksum *dnapi.ChecksumData
}

func newChecksumOperator(checksum *dnapi.ChecksumData) *ChecksumOperator {
	return &ChecksumOperator{Checksum: checksum}
}

func newChecksumOperatorComputer(checksumType *dnapi.ChecksumType, bytesPerChecksum uint32) *ChecksumOperator {
	return newChecksumOperator(&dnapi.ChecksumData{
		Type:             checksumType,
		BytesPerChecksum: proto.Uint32(bytesPerChecksum),
	})
}

func newChecksumOperatorVerifier(checksum *dnapi.ChecksumData) *ChecksumOperator {
	return newChecksumOperator(checksum)
}

func (operator *ChecksumOperator) ComputeChecksum(data []byte) error {
	var cf checksumFunc
	switch *operator.Checksum.Type {
	case dnapi.ChecksumType_NONE:
		operator.Checksum.Checksums = make([][]byte, 0)
		return nil
	case dnapi.ChecksumType_CRC32:
		cf = checksum.CRC32
	case dnapi.ChecksumType_CRC32C:
		cf = checksum.CRC32c
	case dnapi.ChecksumType_SHA256:
		cf = checksum.SHA256
	case dnapi.ChecksumType_MD5:
		cf = checksum.MD5
	default:
		return fmt.Errorf("unknown checksum type: %s", operator.Checksum.Type.String())
	}

	l := uint32(len(data))
	bytesPerChecksum := *operator.Checksum.BytesPerChecksum
	numChecksums := (l + bytesPerChecksum - 1) / bytesPerChecksum

	checksums := make([][]byte, numChecksums)
	for i := uint32(0); i < numChecksums; i++ {
		start := i * bytesPerChecksum
		end := start + bytesPerChecksum
		if end > l {
			end = l
		}
		csm := cf(data[start:end])
		for i, j := 0, len(csm)-1; i < j; i, j = i+1, j-1 {
			csm[i], csm[j] = csm[j], csm[i]
		}
		//var index = 0
		//for idx, b := range csm {
		//	if b != 0 {
		//		index = idx
		//		break
		//	}
		//}
		checksums[i] = csm[4:]
	}
	operator.Checksum.Checksums = checksums
	return nil
}

func (operator *ChecksumOperator) VerifyChecksum(data []byte, off uint64) error {
	// Range: [startIndex, endIndex)
	startIndex := off / uint64(*operator.Checksum.BytesPerChecksum)
	endIndex := startIndex + uint64(len(data))/uint64(*operator.Checksum.BytesPerChecksum)
	if uint64(len(data))%uint64(*operator.Checksum.BytesPerChecksum) > 0 {
		// but it works. :-)
		endIndex += 1
	}

	if endIndex > uint64(len(operator.Checksum.Checksums)) {
		return fmt.Errorf("data to verify checksums from %v, len: %v is out off index of chunk checksums(%v), bytes per Checksum: %v",
			off, len(data), len(operator.Checksum.Checksums), *operator.Checksum.BytesPerChecksum)
	}

	var cf checksumFunc
	switch *operator.Checksum.Type {
	case dnapi.ChecksumType_NONE:
		return nil
	case dnapi.ChecksumType_CRC32:
		cf = checksum.CRC32
	case dnapi.ChecksumType_CRC32C:
		cf = checksum.CRC32c
	case dnapi.ChecksumType_SHA256:
		cf = checksum.SHA256
	case dnapi.ChecksumType_MD5:
		cf = checksum.MD5
	default:
		return fmt.Errorf("unknown Checksum type: " + operator.Checksum.Type.String())
	}

	for checkIndex, byteStart := startIndex, uint32(0); checkIndex < endIndex && byteStart < uint32(len(data)); {

		byteEnd := byteStart + *operator.Checksum.BytesPerChecksum
		if byteEnd > uint32(len(data)) {
			byteEnd = uint32(len(data))
		}

		checksumBytes := cf(data[byteStart:byteEnd])
		if bytes.Compare(checksumBytes, operator.Checksum.Checksums[checkIndex]) != 0 {
			return fmt.Errorf("ChecksumMismatchError ChecksumType %v checksumBytes %v CompareTo %v",
				operator.Checksum.Type.String(), checksumBytes, operator.Checksum.Checksums[checkIndex])
		}
		byteStart = byteEnd
		checkIndex++
	}

	return nil

}
