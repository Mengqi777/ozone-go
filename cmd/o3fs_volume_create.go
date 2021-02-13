package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
	"strings"
)

const volumeCreateOpName = "create volume"

var volumeCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a given volume",
	Long: "usage: \n" +
		"\tcreate volume\n" +
		"\tcreate /volume\n" ,
	Aliases: []string{"c"},
	Run: func(cmd *cobra.Command, args []string) {
		runVolumeCreate(args...)
	},
}

func runVolumeCreate(args ...string) {

	client := NewOMClient()
	var volume string
	volume = args[0]
	if strings.HasPrefix(volume, "/") {
		volume = volume[1:]
	}
	err := client.CreateVolume(volume)
	if err != nil {
		log.Error(volumeCreateOpName, err)
		os.Exit(1)
	}
}

func init() {
	VolumeCmd.AddCommand(volumeCreateCmd)
}
