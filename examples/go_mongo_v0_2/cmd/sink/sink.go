package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc"
	mongo_sink "kry127.ru/dtextension/examples/go_mongo_v0_2/pkg/sink"
	"kry127.ru/dtextension/go/pkg/api/v0_2/sink"
	"log"
	"net"
)

func main() {
	var port = 26927
	flag.IntVar(&port, "port", 26927, "specify port of service (default 26927")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	sink.RegisterSinkServiceServer(grpcServer, mongo_sink.NewMongoSinkService())
	log.Printf("starting mongo gRPC sinker")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server quit with error: %v", err)
		return
	}
}
