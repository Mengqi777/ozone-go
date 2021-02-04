package main

import (
	"github.com/spf13/cobra"
)

const fsLsOpName = "ls"

var fsLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List files in ozone filesystem",
	Run: func(cmd *cobra.Command, args []string) {
		runFsLs(HumanReadable, args...)
	},
}

func runFsLs(humanReadable bool, paths ...string) {
	//oldPath := paths
	//paths = processPath(paths)
	//if len(paths) == 0 {
	//	log.Error(oldPath, customerror.FileErrNotExist)
	//	os.Exit(1)
	//}
	//for _, path := range paths {
	//	fileStatus, err := FsCli.GetFileStatus(path)
	//	if err != nil {
	//		log.Error(fsLsOpName, err)
	//		break
	//	}
	//	if fileStatus.GetIsDirectory() {
	//		fileStatus, err := FsCli.ListStatus(path)
	//		if err != nil {
	//			log.Error(fsLsOpName, err)
	//			break
	//		}
	//		var fileList []os.FileInfo
	//		for _, s := range fileStatus {
	//			fileList = append(fileList, NewFileInfo(s))
	//		}
	//		PrintFiles(humanReadable, fileList...)
	//	} else {
	//		var fileInfo = NewFileInfo(fileStatus)
	//		PrintFiles(humanReadable, fileInfo)
	//	}
	//}

}

func init() {
	FsCmd.AddCommand(fsLsCmd)

	fsLsCmd.Flags().BoolVarP(&HumanReadable, "humanReadable", "H", false, "Format file sizes in a human-readable fashion")
	fsLsCmd.Flags().BoolVarP(&Recursive, "recursive", "r", false, "List files recursive")
}
