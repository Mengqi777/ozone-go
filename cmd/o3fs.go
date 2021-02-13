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
	"encoding/json"
	"github.com/mengqi777/ozone-go/api"
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
	"path"
	"strings"
)

var Force bool
var Recursive bool
var List bool
var Length int
var CustomUser string
var Prefix string
var StartItem string
var SkipTrash bool
var Parents bool
var HumanReadable bool
var Sum bool

var VolumeCmd = &cobra.Command{
	Use:     "volume",
	Aliases: []string{"vol", "v"},
	Short:   "Volume specific operationst",
	Long:    "Native Ozone client operations",
}

var BucketCmd = &cobra.Command{
	Use:     "bucket",
	Aliases: []string{"bkt", "b"},
	Short:   "Bucket specific operations",
	Long:    "Native Ozone client operations",
}

var KeyCmd = &cobra.Command{
	Use:     "key",
	Aliases: []string{"k"},
	Short:   "Key specific operations",
	Long:    "Native Ozone client operations",
}

func NewOMClient() *api.OzoneClient {
	return api.CreateOzoneClient(OmHost)
}

func PrintOzone(op string, v interface{}) {
	out, err := json.MarshalIndent(v, "", "   ")
	if err != nil {
		log.Error(op, err)
		os.Exit(1)
	}
	println(string(out))
}

func splitArgsToVolume(s string) (v string) {
	if !strings.HasPrefix(s, "/") {
		s = "/" + s
	}
	arr := strings.Split(path.Clean(s), "/")
	return arr[1]
}

func splitArgsToBucket(s string) (v, b string) {
	if !strings.HasPrefix(s, "/") {
		s = "/" + s
	}
	arr := strings.Split(path.Clean(s), "/")
	return arr[1], arr[2]
}

func splitArgsToKey(s string) (v, b, k string) {
	if !strings.HasPrefix(s, "/") {
		s = "/" + s
	}
	arr := strings.Split(path.Clean(s), "/")
	if len(arr)==4{
		return arr[1], arr[2], arr[3]
	}else {
		return arr[1], arr[2], strings.Join(arr[3:],"/")
	}

}

func checkLength(args []string, length int) {
	if len(args) != length {
		log.Error("error length", len(args), "expected length", length)
		os.Exit(1)
	}
}
