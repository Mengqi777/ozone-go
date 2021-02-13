package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"strings"
)

const volumeDeleteOpName = "volume"

var volumeDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete a given volume",
	Long: "usage: \n" +
		"\tdelete volume\n" +
		"\tdelete /volume\n" ,
	Aliases: []string{"rm"},
	Run: func(cmd *cobra.Command, args []string) {
		runvolumeDelete(args...)
	},
}

func runvolumeDelete(args ...string) {
	client := NewOMClient()
	var volume string
	volume = args[0]
	if strings.HasPrefix(volume, "/") {
		volume = volume[1:]
	}
	err := client.DeleteVolume(volume)
    if err==nil{
		log.Info(volumeDeleteOpName, volume, "success")
	}else {
		log.Error(volumeDeleteOpName, volume, "error",err)
	}

}

func init() {
	VolumeCmd.AddCommand(volumeDeleteCmd)
}
