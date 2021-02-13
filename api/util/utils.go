package util

import (
	"encoding/binary"
	"fmt"
	"github.com/mengqi777/ozone-go/api/proto/datanode"
	log "github.com/wonderivan/logger"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func Uint64toBytes(b []byte, v uint64) {
	binary.LittleEndian.PutUint64(b, v)
}

func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}

func Uint16toBytes(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func Uint8toBytes(b []byte, v uint8) {
	b[0] = byte(v)
}

func IndexList(b ...interface{}) []int {
	var l []int
	for i := range b {
		l = append(l, i)
	}
	return l
}

func RunTimesUntilSuccessfully(f func() error, times int) error {
	var err error
	for i := 0; i < times; i++ {
		err = f()
		if err != nil {
			continue
		}
		return nil
	}
	return err
}

func ParseFSPath(p string) (omHost string, fsPath string, err error) {

	fsPath = p
	if strings.HasPrefix(p, "o3fs://") {

		r, _ := regexp.Compile("o3fs://([^/]*):([\\d]+)/+(.*)")

		if r.MatchString(p) {
			s := r.FindStringSubmatch(p)

			omHost = s[1] + ":" + s[2]
			fsPath = string(os.PathSeparator) + s[3]

		} else {
			return "", "", fmt.Errorf("%v is not a o3fs uri", p)
		}

	}
	return omHost, path.Clean(fsPath), nil

}

// 判断所给路径文件/文件夹是否存在
func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// 判断所给路径是否为文件夹
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// 判断所给路径是否为文件
func IsFile(path string) bool {
	return !IsDir(path)
}

func IsReadOnly(cmdType string) bool {
	switch cmdType {
	case datanode.Type_ReadContainer.String():
		return true
	case datanode.Type_ReadChunk.String():
		return true
	case datanode.Type_ListBlock.String():
		return true
	case datanode.Type_GetBlock.String():
		return true
	case datanode.Type_GetSmallFile.String():
		return true
	case datanode.Type_ListContainer.String():
		return true
	case datanode.Type_ListChunk.String():
		return true
	case datanode.Type_GetCommittedBlockLength.String():
		return true

	case datanode.Type_CloseContainer.String():
		return false
	case datanode.Type_WriteChunk.String():
		return false
	case datanode.Type_UpdateContainer.String():
		return false
	case datanode.Type_CompactChunk.String():
		return false
	case datanode.Type_CreateContainer.String():
		return false
	case datanode.Type_DeleteChunk.String():
		return false
	case datanode.Type_DeleteContainer.String():
		return false
	case datanode.Type_DeleteBlock.String():
		return false
	case datanode.Type_PutBlock.String():
		return false
	case datanode.Type_PutSmallFile.String():
		return false

	default:
		return false
	}
}

func ToUUIDMLBit(name string) (int64, int64) {
	components := strings.Split(name, "-")
	if len(components) != 5 {
		log.Error("Invalid UUID string: " + name)
		os.Exit(1)
	}

	mostSigBits, _ := strconv.ParseInt(components[0], 16, 64)
	mostSigBits <<= 16
	m1, _ := strconv.ParseInt(components[1], 16, 64)
	mostSigBits |= m1
	mostSigBits <<= 16
	m2, _ := strconv.ParseInt(components[2], 16, 64)
	mostSigBits |= m2

	leastSigBits, _ := strconv.ParseInt(components[3], 16, 64)
	leastSigBits <<= 48
	l4, _ := strconv.ParseInt(components[4], 16, 64)
	leastSigBits |= l4

	return mostSigBits, leastSigBits
}

func RepresentsDirectory(uriPath string) bool {
	lastIdx := strings.LastIndex(uriPath, "/")
	name := uriPath[lastIdx+1:]
	// Path will munch off the chars that indicate a dir, so there's no way
	// to perform this test except by examining the raw basename we maintain
	return len(name) == 0 || name == "\\." || name == "\\.\\."
}

func SubStr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

func ComputeEndPos(remain uint64, writtenLen uint64, capacity uint64, endPos uint64) uint64 {
	if remain/capacity > 0 {
		endPos = writtenLen + capacity
	} else {
		if endPos == 0 {
			endPos = remain
		} else {
			endPos = writtenLen + remain
		}
	}
	return endPos
}

func ParseTimeFromMills(mills int64) string{
	cTime := time.Unix(0, mills*int64(time.Millisecond))
	s := cTime.Format("2006-01-02T15:04:05.000Z")
	return s
}


func FormatBytesToHuman(i uint64) string {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fT", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fG", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		return fmt.Sprintf("%#.1fM", float64(i)/1024/1024)
	case i > 1024:
		return fmt.Sprintf("%#.1fK", float64(i)/1024)
	default:
		return fmt.Sprintf("%dB", i)
	}
}


func Ptri(i uint64) *uint64 {
	return &i
}

func Ptr(s string) *string {
	return &s
}

