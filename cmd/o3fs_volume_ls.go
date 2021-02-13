package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const volumeLsOpName = "list volume"

var volumeLsCmd = &cobra.Command{
	Use:   "list",
	Short: "list all volumes",
	Long: "usage: \n" +
		"\tlist\n" ,
	Aliases: []string{"ls"},
	Run: func(cmd *cobra.Command, args []string) {
		runVolumeLs(Length,CustomUser,args...)
	},
}

func runVolumeLs(length int,user string,args ...string) {
	client := NewOMClient()

	volumes,err := client.ListVolumes(length,user)
	if err != nil {
		log.Error(volumeLsOpName, err)
		os.Exit(1)
	}
	PrintOzone(volumeLsOpName,volumes)
}

func init() {
	VolumeCmd.AddCommand(volumeLsCmd)

	volumeLsCmd.Flags().IntVarP(&Length, "limit", "l", 100, "Maximum number of items to list\n\tDefault: 100")
	volumeLsCmd.Flags().StringVarP(&CustomUser, "userName", "u","", "List accessible volumes of the user. This will be\n\tignored if list all volumes option is specified.")
}
