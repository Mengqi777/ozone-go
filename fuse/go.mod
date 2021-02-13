module github.com/mengqi777/ozone-go/fuse

go 1.15

require (
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/hanwen/go-fuse v1.0.0
	github.com/hortonworks/gohadoop v0.0.0-20180913181356-4e92e1475b38 // indirect
	github.com/mengqi777/ozone-go/api v0.0.0-20180911220305-26e67e76b6c3
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d // indirect
	github.com/urfave/cli v1.22.1
	google.golang.org/grpc v1.35.0 // indirect
)

replace github.com/mengqi777/ozone-go/api => ../api
