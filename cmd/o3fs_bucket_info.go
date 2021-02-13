package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const bucketInfoOpName = "info bucket"

var bucketInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "info all information in a given bucket",
	Long: "usage: \n" +
		"\tinfo volume bucket\n" +
		"\tinfo /volume/bucket\n" ,
	Aliases: []string{"inf"},
	Run: func(cmd *cobra.Command, args []string) {
		runBucketInfo(args...)
	},
}

func runBucketInfo(args ...string) {
	client := NewOMClient()
	var volume,bucket string

	if len(args)==1{
		volume,bucket=splitArgsToBucket(args[0])
	}else {
		volume = args[0]
		bucket = args[1]
	}

	bucketInfo,err := client.InfoBucket(volume, bucket)
	if err != nil {
		log.Error(bucketInfoOpName, err)
		os.Exit(1)
	}
	PrintOzone(bucketLsOpName,bucketInfo)
}

func init() {
	BucketCmd.AddCommand(bucketInfoCmd)

	bucketInfoCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
}
