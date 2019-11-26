package main

import (
	pb "clusrun/proto"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"

	svc "github.com/judwhite/go-svc/svc"
	grpc "google.golang.org/grpc"
)

type program struct {
	grpc_server *grpc.Server
}

func (program) Init(env svc.Environment) error {
	fmt.Println("Service is being initialized")
	SetupFireWall()
	fmt.Println("Service initialized")
	return nil
}

func (p *program) Start() error {
	go p.StartClusnode()
	fmt.Println("Service started with pid", syscall.Getpid())
	return nil
}

func (p *program) Stop() error {
	fmt.Println("Service is stopping")
	p.grpc_server.GracefulStop()
	fmt.Println("Service stopped")
	return nil
}

func (p *program) StartClusnode() {
	_, port, err := ParseHostAddress(clusnode_hosting)
	if err != nil {
		log.Fatalf("Failed to parse clusnode host address: %v", err)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	p.grpc_server = grpc.NewServer()
	pb.RegisterClusnodeServer(p.grpc_server, &clusnode_server{})
	pb.RegisterHeadnodeServer(p.grpc_server, &headnode_server{})
	name, err := os.Hostname()
	if err != nil {
		name = default_nodename
	}
	log.Printf("Clusnode %v starts listening on %v", name, clusnode_hosting)
	if err := p.grpc_server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
