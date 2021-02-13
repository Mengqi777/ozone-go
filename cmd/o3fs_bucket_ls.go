package main

import (
	"github.com/spf13/cobra"
	"strings"
)

const bucketLsOpName = "list bucket"

var bucketLsCmd = &cobra.Command{
	Use:   "list",
	Short: "list all buckets in a given volume",
	Long: "usage: \n" +
		"\tls [-p prefix] [-s startItem] volume\n" +
		"\tls [-p prefix] [-s startItem] /volume\n" ,
	Aliases: []string{"ls"},
	Run: func(cmd *cobra.Command, args []string) {
		runBucketLs(Prefix,StartItem,args...)
	},
}

func runBucketLs(prefix string ,startKey string,args ...string) {
	client := NewOMClient()
	var volume string
	volume = args[0]
	if strings.HasPrefix(volume, "/") {
		volume = volume[1:]
	}
	buckets := client.ListBuckets(volume, startKey, prefix)

	PrintOzone(bucketLsOpName,buckets)
}

func init() {
	BucketCmd.AddCommand(bucketLsCmd)

	bucketLsCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
	bucketLsCmd.Flags().StringVarP(&Prefix, "prefix", "p", "", "Prefix to filter the items")
	bucketLsCmd.Flags().StringVarP(&StartItem, "startItem", "s", "", "The item to start the listing from.\nThis will be excluded from the result.")
}
