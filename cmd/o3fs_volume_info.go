package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
	"strings"
)

const volumeInfoOpName = "volume"

var volumeInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "info volume in a given volume",
	Long: "usage: \n" +
		"\tinfo volume\n" +
		"\tinfo /volume\n" ,
	Run: func(cmd *cobra.Command, args []string) {
		runVolumeInfo(args...)
	},
}

func runVolumeInfo(args ...string) {
	client := NewOMClient()
	var volume string
	volume = args[0]
	if strings.HasPrefix(volume, "/") {
		volume = volume[1:]
	}
	volumeInfo ,err:= client.InfoVolume(volume)
	if err != nil {
		log.Error(volumeInfoOpName, err)
		os.Exit(1)
	}
	PrintOzone(volumeInfoOpName,volumeInfo)
}

func init() {
	VolumeCmd.AddCommand(volumeInfoCmd)
	volumeInfoCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
}
