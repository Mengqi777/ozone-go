package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const keyCatOpName = "cat key"

var keyCatCmd = &cobra.Command{
	Use:   "cat",
	Short: "Copies a specific Ozone key to standard output",
	Long: "usage: key cat <value>\n" +
		"\t<value>     URI of the volume/bucket/key.\n",
	Run: func(cmd *cobra.Command, args []string) {
		runKeyCat(args...)
	},
}

func runKeyCat(args ...string) {

	client := NewOMClient()
	for _, arg := range args {
		var v, b, k string
		v, b, k = splitArgsToKey(arg)
		_, err := client.GetKey(v, b, k, os.Stdout)
		if err != nil {
			log.Error(keyCatOpName, v, b, k, err)
		}
	}

}

func init() {
	KeyCmd.AddCommand(keyCatCmd)
}
