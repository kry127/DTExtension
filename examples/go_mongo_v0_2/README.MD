# MongoDB DTExtension golang connector example

This example shows how to write MongoDB connector using Golang.

## Project structture
The project contains the following:
- cmd/sink -- server that runs gRPC server for MongoDB sink
- pkg/sink -- a description of gRPC service that interacts with MongoDB

### Protocol buffers generation
Just use `make gen_golang_v0_2` in the root folder, then, service files in `/go/pkg/api` will be produced to write
your own service.