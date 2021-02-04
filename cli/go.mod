module github.com/mengqi777/ozone-go

go 1.13

replace github.com/mengqi777/ozone-go/api => ../api

require (
	github.com/mengqi777/ozone-go/api v0.0.0-20180911220305-26e67e76b6c3
	github.com/urfave/cli v1.22.5
	google.golang.org/protobuf v1.25.0 // indirect
)
