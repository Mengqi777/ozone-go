/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"github.com/mengqi777/ozone-go/api"
	"github.com/mengqi777/ozone-go/api/util"
	"github.com/spf13/cobra"
	osuser "os/user"

)

var Force bool
var Recursive bool
var SkipTrash bool
var Parents bool
var HumanReadable bool
var Sum bool

var FsCmd = &cobra.Command{
	Use:   "fs",
	Short: "Ozone filesystem command client",
	Long:  `Native Ozone filesystem client`,
}

func NewOMClient() *api.OzoneClient  {
	return api.CreateOzoneClient(OmHost)
}


func InitFsArgs(path string) (user, host, fsPath string, err error) {
	host, fsPath, err = api.ParseFSPath(path)
	if err != nil {
		return "", "", "", err
	}

	if host == "" {
		if OmHost == "" {
			return "", "", "", fmt.Errorf("om host not set")
		}
		host = OmHost
	}

	if UserName == "" {
		if u, err := osuser.Current(); err != nil {
			return "", "", "", err
		} else {
			user = u.Name
			UserName = user
		}
	}

	return user, host, fsPath, nil
}
//
//func PrintFiles(humanReadable bool, files ...os.FileInfo) {
//
//	tw := tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
//	for _, f := range files {
//		fi := f.(*OzoneFileInfo)
//		mode := fi.Mode().String()
//		owner := fi.Owner()
//		group := fi.OwnerGroup()
//		factor := int32(0)
//		if !fi.IsDir() {
//			factor = hadoop_hdds.ReplicationFactor_value[fi.Status.FileInfoProto.Factor.String()]
//		}
//
//		size := strconv.FormatInt(fi.Size(), 10)
//		if humanReadable {
//			size = formatBytes(uint64(fi.Size()))
//		}
//		name := fi.Path()
//		modTime := fi.ModTime()
//		date := modTime.Format("2006-01-02 15:04")
//
//		_, _ = fmt.Fprintf(tw, "%s \t%d \t%s \t %s \t %s \t%s \t%s\n",
//			mode, factor, owner, group, size, date, name)
//	}
//	tw.Flush()
//
//}
//
//func formatBytes(i uint64) string {
//	switch {
//	case i > (1024 * 1024 * 1024 * 1024):
//		return fmt.Sprintf("%#.1fT", float64(i)/1024/1024/1024/1024)
//	case i > (1024 * 1024 * 1024):
//		return fmt.Sprintf("%#.1fG", float64(i)/1024/1024/1024)
//	case i > (1024 * 1024):
//		return fmt.Sprintf("%#.1fM", float64(i)/1024/1024)
//	case i > 1024:
//		return fmt.Sprintf("%#.1fK", float64(i)/1024)
//	default:
//		return fmt.Sprintf("%dB", i)
//	}
//}
//
//// TODO: not really sure checking for a leading \ is the way to test for
//// escapedness.
//func hasGlob(fragment string) bool {
//	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
//	return match
//}
//
//// ExpandGlobs recursively expands globs in a filepath. It assumes the paths
//// are already cleaned and normalized (ie, absolute).
//func ExpandGlobs(globbedPath string) ([]string, error) {
//	parts := strings.Split(globbedPath, "/")[1:]
//	var res []string
//	var splitAt int
//	for splitAt = range parts {
//		if hasGlob(parts[splitAt]) {
//			break
//		}
//	}
//	log.Debug(parts)
//	var base, glob, next, remainder string
//	base = "/" + path.Join(parts[:splitAt]...)
//	glob = parts[splitAt]
//
//	if len(parts) > splitAt+1 {
//		next = parts[splitAt+1]
//		remainder = path.Join(parts[splitAt+2:]...)
//	} else {
//		next = ""
//		remainder = ""
//	}
//	log.Debug(base)
//	list, err := FsCli.ListStatus(base)
//	if err != nil {
//		return nil, err
//	}
//
//	for _, fi := range list {
//		match, _ := path.Match(glob, fi.GetFileInfoProto().GetName())
//		if !match {
//			continue
//		}
//
//		newPath := path.Join(base, fi.GetFileInfoProto().GetName(), next, remainder)
//		if hasGlob(newPath) {
//			if fi.GetIsDirectory() {
//				children, err := ExpandGlobs(newPath)
//				if err != nil {
//					return nil, err
//				}
//
//				res = append(res, children...)
//			}
//		} else {
//			//_, err := FsCli.GetFileStatus(newPath)
//			//if err!=nil {
//			//	continue
//			//}
//
//			res = append(res, newPath)
//		}
//	}
//
//	return res, nil
//}
//
//func processPath(paths []string) []string {
//	var err error
//	tmpPaths := make([]string, 0)
//	FsCli, err = NewFSClientFromPath(paths[0])
//	if err != nil {
//		log.Error(err)
//		os.Exit(1)
//	}
//
//	for _, p := range paths {
//		_, p, _ = util.ParseFSPath(p)
//		if !hasGlob(p) {
//			tmpPaths = append(tmpPaths, p)
//			continue
//		}
//		li, err := ExpandGlobs(path.Clean(p))
//		if len(li) == 0 {
//			log.Error(p,err)
//			os.Exit(1)
//		}
//		if err != nil {
//			log.Error(err)
//			os.Exit(1)
//		}
//		tmpPaths = append(tmpPaths, li...)
//	}
//	return tmpPaths
//}
