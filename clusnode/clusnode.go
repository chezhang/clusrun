package main

import (
	pb "clusrun/proto"
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

const (
	heartbeat_interval = 1 * time.Second
	connect_timeout    = 30 * time.Second
	default_nodename   = "Unknown"
)

var (
	clusnode_hosting    string
	headnodes_reporting sync.Map
)

type clusnode_server struct {
	pb.UnimplementedClusnodeServer
}

func (s *clusnode_server) Validate(ctx context.Context, in *pb.ValidateRequest) (*pb.ValidateReply, error) {
	log.Printf("Received validation request from %v to %v", in.GetHeadnode(), in.GetClusnode())
	name, err := os.Hostname()
	return &pb.ValidateReply{Nodename: name}, err
}

func (s *clusnode_server) SetHeadnodes(ctx context.Context, in *pb.SetHeadnodesRequest) (*pb.SetHeadnodesReply, error) {
	headnodes := in.GetHeadnodes()
	results := make(map[string]string)
	for _, headnode := range headnodes {
		result := "OK"
		if err := AddHeadnode(headnode); err != nil {
			result = err.Error()
		}
		results[headnode] = result
	}
	log.Printf("SetHeadnodes result: %v", results)
	SaveHeadnodes()
	return &pb.SetHeadnodesReply{Results: results}, nil
}

func (s *clusnode_server) GetHeadnodes(ctx context.Context, in *pb.Empty) (*pb.GetHeadnodesReply, error) {
	headnodes := make(map[string]bool)
	var err error = nil
	headnodes_reporting.Range(func(key, val interface{}) bool {
		headnodes[key.(string)] = val.(bool)
		return true
	})
	log.Printf("GetHeadnodes result: %v", headnodes)
	return &pb.GetHeadnodesReply{Headnodes: headnodes}, err
}

func AddHeadnode(headnode string) error {
	hostname, port, err := ParseHostAddress(headnode)
	if err != nil {
		return errors.New("Failed to parse headnode host address: " + err.Error())
	}
	headnode = hostname + ":" + port
	if reporting, ok := headnodes_reporting.Load(headnode); ok && reporting.(bool) {
		return errors.New("Already reporting")
	}
	headnodes_reporting.Store(headnode, true)
	go HeartBeat(clusnode_hosting, headnode)
	return nil
}

func HeartBeat(from, to string) {
	log.Printf("Start heartbeat from %v to %v", from, to)
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, to, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Can not connect in %vs: %v", connect_timeout, err)
		headnodes_reporting.Store(to, false)
		return
	}
	defer conn.Close()

	c := pb.NewHeadnodeClient(conn)
	log.Printf("Connected to headnode: %v", to)
	for {
		name, err := os.Hostname()
		if err != nil {
			name = default_nodename
		}
		ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
		defer cancel()

		_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{Node: name, Host: from})
		if err != nil {
			log.Printf("Can not send heartbeat: %v", err)
			headnodes_reporting.Store(to, false)
			return
		}
		time.Sleep(heartbeat_interval)
	}
}
