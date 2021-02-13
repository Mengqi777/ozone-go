package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
)

const bucketCreateOpName = "create bucket"

var bucketCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create bucket in a given volume bucket",
	Long: "usage: \n" +
		"\tcreate volume bucket\n" +
		"\tcreate /volume/bucket\n",
	Aliases: []string{"c"},
	Run: func(cmd *cobra.Command, args []string) {
		runBucketCreate(args...)
	},
}

func runBucketCreate(args ...string) {
	client := NewOMClient()
	var volume, bucket string

	if len(args) == 1 {
		volume, bucket = splitArgsToBucket(args[0])
	} else {
		volume = args[0]
		bucket = args[1]
	}
	err := client.CreateBucket(volume, bucket)
	if err != nil {
		log.Error(bucketCreateOpName, volume, bucket, "error", err)
	}

}

func init() {
	BucketCmd.AddCommand(bucketCreateCmd)

	bucketCreateCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
}
