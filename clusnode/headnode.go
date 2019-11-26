package main

import (
	pb "clusrun/proto"
	"context"
	"log"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

const (
	heartbeat_expire_time = 5 * time.Second
)

var (
	reported_time   = sync.Map{}
	validate_number = sync.Map{}
)

type headnode_server struct {
	pb.UnimplementedHeadnodeServer
}

func (s *headnode_server) Heartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.Empty, error) {
	nodename, host := in.GetNode(), in.GetHost()
	display_name := nodename
	if host != default_node {
		display_name += "(" + host + ")"
	} else {
		host = nodename + ":" + default_port
	}
	if _, ok := reported_time.Load(display_name); !ok {
		log.Printf("First heartbeat from %v", display_name)
	}
	reported_time.Store(display_name, time.Now())
	go Validate(display_name, nodename, host)
	return &pb.Empty{}, nil
}

func Validate(display_name, nodename, host string) {
	if number, ok := validate_number.LoadOrStore(display_name, 0); !ok || number.(int) > 0 {
		number := number.(int)
		if ok { // validate immediately in the first time, otherwise double validating interval after every failure
			validate_number.Store(display_name, 0) // value 0 means validation is ongoing
			delay := math.Pow(2, float64(number))
			if delay > 60 {
				delay = 60
			}
			time.Sleep(time.Duration(delay) * time.Second)
		}
		log.Printf("Start validating clusnode %v", display_name)
		conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Printf("Can not connect: %v", err)
			validate_number.Store(display_name, number+1)
			return
		}
		defer conn.Close()

		c := pb.NewClusnodeClient(conn)
		log.Printf("Connected to clusnode host %v", host)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		reply, err := c.Validate(ctx, &pb.ValidateRequest{Headnode: clusnode_hosting, Clusnode: host})
		name := reply.GetNodename()
		if err != nil {
			log.Printf("Validation failed: %v", err)
			validate_number.Store(display_name, number+1)
		} else if name != nodename {
			log.Printf("Validation failed: expect nodename %v, replied nodename %v", nodename, name)
			validate_number.Store(display_name, 10)
		} else {
			log.Printf("Clusnode %v is validated that being hosted by %v", nodename, host)
			validate_number.Store(display_name, -1)
		}
	}
}

func (s *headnode_server) GetNodes(ctx context.Context, in *pb.Empty) (*pb.GetNodesReply, error) {
	ready_nodes := []string{}
	error_nodes := []string{}
	lost_nodes := []string{}
	reported_time.Range(func(key interface{}, val interface{}) bool {
		node := key.(string)
		last_report := val.(time.Time)
		if time.Since(last_report) > heartbeat_expire_time {
			lost_nodes = append(lost_nodes, node)
		} else {
			if number, ok := validate_number.Load(node); ok && number.(int) < 0 {
				ready_nodes = append(ready_nodes, node)
			} else {
				error_nodes = append(error_nodes, node)
			}
		}
		return true
	})
	log.Printf("GetNodes result:\nReadyNodes: %v\nErrorNodes: %v\nLostNodes: %v", ready_nodes, error_nodes, lost_nodes)
	return &pb.GetNodesReply{ReadyNodes: ready_nodes, ErrorNodes: error_nodes, LostNodes: lost_nodes}, nil
}
