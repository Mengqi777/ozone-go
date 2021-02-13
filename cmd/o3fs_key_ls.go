package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const keyLsOpName = "list key"

var keyLsCmd = &cobra.Command{
	Use:   "list",
	Short: "list all keys in a given bucket",
	Long: "usage: key list [-l=<limit>] [-p=<prefix>] [-s=<startItem>] <value>\n" +
		"\t <value>      URI of the volume/bucket.\n" +
		"\t-l,           Maximum number of items to list. Default: 100\n" +
		"\t-p,           Prefix to filter the items\n" +
		"\t-s,           The item to start the listing from.\n" ,
	Aliases: []string{"ls"},
	Run: func(cmd *cobra.Command, args []string) {
		runKeyLs(HumanReadable,Prefix,StartItem, args...)
	},
}



func runKeyLs(humanReadable bool,prefix string,startKey string, args ...string) {
	client := NewOMClient()
	var volume, bucket string
	if len(args) == 1 {
	    volume,bucket=splitArgsToBucket(args[0])
	} else if len(args) == 2 {
		volume = args[0]
		bucket = args[1]
	}
	keys, err := client.ListKeys(humanReadable, volume, bucket, prefix,startKey)
	if err != nil {
		log.Error(keyLsOpName, err)
		os.Exit(1)
	}
	PrintOzone(keyLsOpName,keys)
}

func init() {
	KeyCmd.AddCommand(keyLsCmd)

	keyLsCmd.Flags().BoolVarP(&HumanReadable, "humanReadable", "H", false, "Format file sizes in a human-readable fashion")
	keyLsCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")
	keyLsCmd.Flags().StringVarP(&Prefix, "prefix", "p", "", "Prefix to filter the items")
	keyLsCmd.Flags().StringVarP(&StartItem, "startItem", "s", "", "The item to start the listing from.\nThis will be excluded from the result.")

}
