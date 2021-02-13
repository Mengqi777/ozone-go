package main

import (
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
)

const keyDeleteOpName = "delete key"

var keyDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "deletes an existing key",
	Long: "usage: key delete <value>\n" +
		"\t<value>     URI of the volume/bucket/key.\n" ,
	Run: func(cmd *cobra.Command, args []string) {
		runKeyDelete(args...)
	},
}


func runKeyDelete(args ...string) {
	client := NewOMClient()
	for _, arg := range args {
		v,b,k:=splitArgsToKey(arg)
		err:=client.DeleteKey(v,b,k)
		if err != nil {
			log.Error(keyDeleteOpName,arg, err)
		}
	}

}

func init() {
	KeyCmd.AddCommand(keyDeleteCmd)
}
