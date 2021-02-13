module github.com/mengqi777/ozone-go/cmd

go 1.15

replace github.com/mengqi777/ozone-go/api => ../api

require (
	github.com/hortonworks/gohadoop v0.0.0-20180913181356-4e92e1475b38 // indirect
	github.com/mengqi777/ozone-go/api v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.1
	github.com/wonderivan/logger v1.0.0
)
