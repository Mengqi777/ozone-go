package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
)

const bucketDeleteOpName = "delete bucket"

var bucketDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete all buckets in a given volume",
	Long: "usage: \n" +
		"\tdelete volume bucket\n" +
		"\tdelete /volume/bucket\n",
	Aliases: []string{"rm"},
	Run: func(cmd *cobra.Command, args []string) {
		runBucketDelete(args...)
	},
}

func runBucketDelete(args ...string) {
	client := NewOMClient()
	var volume, bucket string

	if len(args) == 1 {
		volume, bucket = splitArgsToBucket(args[0])
	} else {
		volume = args[0]
		bucket = args[1]
	}
	if err := client.DeleteBuckets(volume, bucket); err != nil {
		log.Error(bucketDeleteOpName, volume, bucket, "error", err)
	} else {
		log.Info(bucketDeleteOpName, volume, bucket, "success")
	}

}

func init() {
	BucketCmd.AddCommand(bucketDeleteCmd)

	bucketDeleteCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
}
