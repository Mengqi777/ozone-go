package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const keyInfoOpName = "info key"

var keyInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "returns information about an existing key",
	Long: "usage: key info <value>\n" +
		"\t<value>     URI of the volume/bucket/key.\n",
	Run: func(cmd *cobra.Command, args []string) {
		runKeyInfo(HumanReadable, args...)
	},
}

func runKeyInfo(humanReadable bool, args ...string) {
	checkLength(args, 1)
	client := NewOMClient()
	v, b, k := splitArgsToKey(args[0])
	keys, err := client.InfoKey(humanReadable, v, b, k)
	if err != nil {
		log.Error(keyInfoOpName, v, b, k, err)
		os.Exit(1)
	}

	PrintOzone(keyInfoOpName, keys)
}

func init() {
	KeyCmd.AddCommand(keyInfoCmd)

	keyInfoCmd.Flags().BoolVarP(&HumanReadable, "humanReadable", "H", false, "Format file sizes in a human-readable fashion")
	keyInfoCmd.Flags().BoolVarP(&List, "fileList", "l", false, "List by file system")

}
