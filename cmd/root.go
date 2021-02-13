/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"github.com/mengqi777/ozone-go/api/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	log "github.com/wonderivan/logger"
	"os"
	"os/user"
	"strings"
)

var CfgFile string
var OmHost string
var LogLevel string

var version = "1.0"

// rootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "go-ozone",
	Short: "Go-Ozone command line client built with peace and love in Go",
}

var versionCmd = &cobra.Command{
	Use:   string("version"),
	Short: string("go-ozone client version :" + version),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(cmd.Short)
	},
}

// Execute adds all child commands to the rootPath command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(InitConfig)
	RootCmd.AddCommand(versionCmd)
	RootCmd.AddCommand(VolumeCmd)
	RootCmd.AddCommand(BucketCmd)
	RootCmd.AddCommand(KeyCmd)
	RootCmd.PersistentFlags().StringVar(&CfgFile, "config", "", "config file (default is ${pwd}/ozone.yaml)")
	RootCmd.PersistentFlags().StringVar(&OmHost, "om", "", "Ozone manager host address")
	RootCmd.PersistentFlags().StringVar(&common.UserName, "user", "", "User name")
	RootCmd.PersistentFlags().StringVar(&LogLevel, "loglevel", "", "log level : debug")
}

// InitConfig reads in config file and ENV variables if set.
func InitConfig() {

	if CfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(CfgFile)
	} else {
		// Find home directory.

		pwd, err := os.Getwd()
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".main" (without extension).
		viper.AddConfigPath(pwd)
		viper.SetConfigName("ozone")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		OmHost = viper.GetString("omHost")
		common.UserName = viper.GetString("userName")
		common.CLUSTER_MODE=viper.GetString("clusterMode")
		loglevel := viper.GetString("log.level")
		color := viper.GetString("log.color")
		if strings.ToLower(LogLevel) == "debug" || strings.ToLower(LogLevel) == "debg" {
			loglevel = "DEBG"
		}
		conf := "{\"Console\": {\"level\": \"" + loglevel + "\",\"color\": " + color + "}}"
		_ = log.SetLogger(conf)
	} else {
		log.Error("err", err)
	}
	if common.UserName == "" {
		name, _ := user.Current()
		common.UserName = name.Username
	}
}
