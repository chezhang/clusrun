package main

import (
	pb "clusrun/protobuf"
	"fmt"
	"net"
	"syscall"
	"time"

	svc "github.com/judwhite/go-svc/svc"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type program struct {
	grpc_server *grpc.Server
}

func (program) Init(env svc.Environment) error {
	fmt.Println("Service is being initialized")
	SetupFireWall()
	InitDatabase()
	fmt.Println("Service initialized")
	return nil
}

func (p *program) Start() error {
	go p.startNodeService()
	fmt.Println("Service started with pid", syscall.Getpid())
	return nil
}

func (p *program) Stop() error {
	fmt.Println("Service is stopping")
	go func() {
		time.Sleep(10 * time.Second)
		fmt.Println("Force stop service")
		p.grpc_server.Stop()
	}()
	p.grpc_server.GracefulStop()
	fmt.Println("Service stopped")
	return nil
}

func (p *program) startNodeService() {
	_, port, _, err := ParseHostAddress(NodeHost)
	if err != nil {
		LogFatality("Failed to parse node host address: %v", err)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		LogFatality("Failed to listen: %v", err)
	}
	options := []grpc.ServerOption{}
	msg := "without TLS"
	if Tls.Enabled {
		creds, err := credentials.NewServerTLSFromFile(Tls.CertFile, Tls.KeyFile)
		if err != nil {
			LogFatality("Failed to load TLS credentials from file: %s", err)
		}
		options = append(options, grpc.Creds(creds))
		msg = "with TLS"
	}
	p.grpc_server = grpc.NewServer(options...)
	pb.RegisterClusnodeServer(p.grpc_server, &clusnode_server{})
	pb.RegisterHeadnodeServer(p.grpc_server, &headnode_server{})
	LogInfo("Node %v starts listening on %v %v", NodeName, NodeHost, msg)
	if err := p.grpc_server.Serve(lis); err != nil {
		LogFatality("Failed to serve: %v", err)
	}
}
