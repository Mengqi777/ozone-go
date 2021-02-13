package main

import (
	"bytes"
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
	"strconv"
)

const keyCpOpName = "cp key"

var keyCpCmd = &cobra.Command{
	Use:   "cp",
	Short: "copies an existing key to another one within the same bucket",
	Long: "usage: cp <value> <fromKey> <toKey>\n" +
		"\t <value>     URI of the volume/bucket.\n" +
		"\t <fromKey>   The existing key to be renamed\n" +
		"\t <toKey>     The new desired name of the key\n",
	Run: func(cmd *cobra.Command, args []string) {
		runKeyCp(Force, args...)
	},
}

func runKeyCp(force bool, args ...string) {

	checkLength(args, 2)
	checkLength(args, 3)
	client := NewOMClient()
	v, b := splitArgsToBucket(args[0])
	fromKey := args[1]
	toKey := args[2]

	if _, err := client.InfoKey(false, v, b, toKey); err == nil {
		if force {
			if err := client.DeleteKey(v, b, toKey); err != nil {
				log.Error(err)
				os.Exit(1)
			}
		} else {
			log.Error(keyCpOpName, v, b, toKey, "file already exist")
			os.Exit(1)
		}
	}

	key, err := client.InfoKey(false, v, b, fromKey)
	if err != nil {
		log.Error(keyCpOpName, err)
		os.Exit(1)
	}
	size, err := strconv.ParseInt(key.DataSize, 10, 64)
	buf := bytes.NewBuffer(make([]byte, size))
	if _, err = client.GetKey(v, b, fromKey, buf); err != nil {
		log.Error(keyCpOpName, err)
		os.Exit(1)
	}

	if _, err = client.PutKeyRaft(v, b, toKey, buf.Bytes()); err != nil {
		log.Error(keyCpOpName, err)
		os.Exit(1)
	}

}

func init() {
	KeyCmd.AddCommand(keyCpCmd)

	keyCpCmd.Flags().BoolVarP(&Force, "force", "f", false, "Overwrite destination")

}
