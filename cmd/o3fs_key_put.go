package main

import (
	"errors"
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"io/ioutil"
	"os"
)

const keyPutOpName = "put key"

var keyPutCmd = &cobra.Command{
	Use:   "put",
	Short: "creates or overwrites an existing key",
	Long: "usage: key put <value> <fileName>\n" +
		"\t<value>      URI of the volume/bucket/key.\n" +
		"\t<fileName>   File to upload\n",
	Run: func(cmd *cobra.Command, args []string) {
		runKeyPut(Force, args...)
	},
}

func runKeyPut(force bool, args ...string) {
	checkLength(args,2)
	client := NewOMClient()
	v, b, k := splitArgsToKey(args[0])
log.Info("client",client.OmClient.OmHost)
	file := args[1]

	if _, err := client.InfoKey(force, v, b, k); err == nil {
		if force {
			if err := client.DeleteKey(v, b, k); err != nil {
				log.Error(keyDeleteOpName, err)
				os.Exit(1)
			}
		} else {
			log.Error(os.PathError{
				Op:   "put",
				Path: args[0],
				Err:  errors.New("key already exist"),
			})
			os.Exit(1)
		}
	}

	f, err := os.Open(file)
	if err != nil {
		log.Error("open file", file, err)
		os.Exit(1)
	}
	content, err := ioutil.ReadAll(f)
	if err!=nil{
		log.Fatal(err)

	}
	_, err = client.PutKeyRaft(v, b, k, content)
	//_, err = client.PutKey(v, b, k, f)
	if err != nil {
		log.Error(keyPutOpName, err)
		os.Exit(1)
	}

}

func init() {
	KeyCmd.AddCommand(keyPutCmd)

	keyPutCmd.Flags().BoolVarP(&Force, "force", "f", false, "Overwrite destination")

}
