package main

import (
	"github.com/mengqi777/ozone-go/api/datanode"
	"github.com/mengqi777/ozone-go/api/util"
	"github.com/spf13/cobra"
	log "github.com/wonderivan/logger"
	"os"
)

const keyGetOpName = "get key"

var keyGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a specific key from ozone server",
	Long: "usage: key get <value> <fileName>\n" +
		"\t<value>      URI of the volume/bucket/key.\n" +
		"\t<fileName>   File path to download the key to\n",
	Run: func(cmd *cobra.Command, args []string) {
		runKeyGet(Force, args...)
	},
}

func runKeyGet(force bool, args ...string) {

	checkLength(args, 2)

	client := NewOMClient()
	var v, b, k string
	v, b, k = splitArgsToKey(args[0])
	file := args[1]
	if util.Exists(file) {
		if force {
			if err:=os.Remove(file);err!=nil{
				log.Error(err)
				os.Exit(1)
			}
		} else {
			log.Error(keyGetOpName, args[0], file, "file already exist")
		}
	}
	f, err := os.Create(file)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	info, err := client.OmClient.GetKey(v, b, k)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	keyReader := datanode.NewKeyReader(v, b, k, client.OmClient, info)
	//buf:=make([]byte,info.GetDataSize())
	//n,err:=keyReader.Read(buf)
	//f.Write(buf)
	if _, err = keyReader.ReadToWriter(f); err != nil {
		log.Error(keyGetOpName, err)
		os.Exit(1)
	}
	if err = f.Close(); err != nil {
		log.Error(keyGetOpName, err)
		os.Exit(1)
	}
}

func init() {
	KeyCmd.AddCommand(keyGetCmd)

	keyGetCmd.Flags().BoolVarP(&Force, "force", "f", false, "Overwrite destination")

}
