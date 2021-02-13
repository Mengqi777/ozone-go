package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const keyRenameOpName = "rename key"

var keyRenameCmd = &cobra.Command{
	Use:   "rename",
	Short: "renames an existing key",
	Long: "usage: key rename <value> <fromKey> <toKey>\n" +
		"\t<value>     URI of the volume/bucket.\n" +
		"\t<fromKey>   The existing key to be renamed\n" +
		"\t<toKey>     The new desired name of the key\n",
	Aliases: []string{"mv"},
	Run: func(cmd *cobra.Command, args []string) {
		runKeyRename(Force, args...)
	},
}

func runKeyRename(force bool, args ...string) {
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
			log.Error(keyRenameOpName, v, b, toKey, "file already exist")
			os.Exit(1)
		}
	}

	err := client.Rename(v, b, fromKey, toKey)
	if err != nil {
		log.Error(keyRenameOpName, err)
		os.Exit(1)
	}
}

func init() {
	KeyCmd.AddCommand(keyRenameCmd)

	keyRenameCmd.Flags().BoolVarP(&Force, "force", "f", false, "Overwrite destination")

}
