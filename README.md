# Go cmdent for Apache Hadoop Ozone

This repository contains an experimental, proof-of-concept golang cmdent for apache hadoop ozone:

 * api: the location for the generic golang api
 * lib: sharable C library
 * cmd: standalone executable tool

** Highly experimental, most of the calls are not implemented. But it shows how can hadoop-rpc/grpc be used from golang.

## Testing with cmd:

```
go run cmd/ozone/main.go -om 127.0.0.1 volume create vol1
```

Or you can install it:

```
cd cmd
go install ./...
ozone -om 127.0.0.1:9862 volume create vol1
```

## Testing the python binding

Create the shared library:

```
go build -o ozone.so   -buildmode=c-shared lib/lib.go
```

Modify parameters of `python/test.py` (om address) and run it.
