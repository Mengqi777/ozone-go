package main

// import "github.com/elek/ozone-go"
import (
	"encoding/json"
	"fmt"
	"github.com/elek/ozone-go/api"
	"github.com/urfave/cli"
	"os"
	"strings"
)

var version string
var commit string
var date string

func main() {

	app := cli.NewApp()
	app.Name = "ozone"
	app.Usage = "Ozone command line client"
	app.Description = "Native Ozone command line client"
	app.Version = fmt.Sprintf("%s (%s, %s)", version, commit, date)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "om",
			Required: true,
			Value:    "localhost",
			Usage:    "Host (or host:port) address of the OzoneManager",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:    "volume",
			Aliases: []string{"v", "vol"},
			Usage:   "Ozone volume related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "list",
					Aliases: []string{"ls"},
					Usage:   "List volumes.",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						volumes, err := ozoneClient.ListVolumes()
						if err != nil {
							return err
						}
						for _, volume := range volumes {
							println(volume.Name)
						}
						return nil
					},
				},
				{
					Name:    "create",
					Aliases: []string{"mk"},
					Usage:   "Create volume.",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						err := ozoneClient.CreateVolume(*address.Volume)
						if err != nil {
							return err
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "bucket",
			Aliases: []string{"b"},
			Usage:   "Ozone bucket related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "create",
					Aliases: []string{"mk"},
					Usage:   "Create bucket.",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						err := ozoneClient.CreateBucket(*address.Volume, *address.Bucket)
						if err != nil {
							return err
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "key",
			Aliases: []string{"k"},
			Usage:   "Ozone key related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "list",
					Aliases: []string{"ls"},
					Usage:   "List keys.",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						keys, err := ozoneClient.ListKeys(*address.Volume, *address.Bucket)
						if err != nil {
							return err
						}
						out, err := json.MarshalIndent(keys, "", "   ")
						if err != nil {
							return err
						}

						println(string(out))
						return nil
					},
				},
				{
					Name:    "info",
					Aliases: []string{"show"},
					Usage:   "Show information about one key",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						key, err := ozoneClient.InfoKey(*address.Volume, *address.Bucket, *address.Key)
						if err != nil {
							return err
						}
						out, err := json.MarshalIndent(key, "", "   ")
						if err != nil {
							return err
						}

						println(string(out))
						return nil
					},
				},
				{
					Name:    "cat",
					Aliases: []string{"c"},
					Usage:   "Show content of a file",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						_, err := ozoneClient.GetKey(*address.Volume, *address.Bucket, *address.Key, os.Stdout)
						if err != nil {
							return err
						}

						return nil
					},
				},
				{
					Name:    "put",
					Aliases: []string{"p"},
					Usage:   "Put file to Ozone",
					Action: func(c *cli.Context) error {
						ozoneClient := api.CreateOzoneClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						f, err := os.Open("/tmp/asd")
						if err != nil {
							return err
						}
						_, err = ozoneClient.PutKey(*address.Volume, *address.Bucket, *address.Key, f)
						if err != nil {
							return err
						}

						return nil
					},
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

type OzoneObjectAddress struct {
	Volume *string
	Bucket *string
	Key    *string
}

func OzoneObjectAddressFromString(get string) OzoneObjectAddress {
	volumeBucketKey := strings.SplitN(get, "/", 3)
	o := OzoneObjectAddress{Volume: &volumeBucketKey[0]}
	if len(volumeBucketKey) > 1 {
		o.Bucket = &volumeBucketKey[1]
	}
	if len(volumeBucketKey) > 2 {
		o.Key = &volumeBucketKey[2]
	}
	return o

}
