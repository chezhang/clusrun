package main

import (
	"context"
	"flag"
	"fmt"

	"google.golang.org/grpc"

	pb "clusrun/proto"
)

func Node(args []string) {
	fs := flag.NewFlagSet("node flag", flag.ExitOnError)
	headnode := fs.String("headnode", default_node, "specify the headnode to connect")
	monitor := fs.Bool("monitor", false, "keep refreshing the node information")
	fs.Parse(args)
	if len(fs.Args()) > 0 {
		fmt.Printf("Invalid parameter: %v", fs.Args())
		return
	}
	if !*monitor {
		ready_nodes, error_nodes, lost_nodes := GetNodes(*headnode)
		fmt.Printf("Ready nodes (%v): %v\nError nodes (%v): %v\nLost nodes (%v): %v",
			len(ready_nodes), ready_nodes, len(error_nodes), error_nodes, len(lost_nodes), lost_nodes)
	} else {
		fmt.Println("Not implemented yet")
	}
}

func GetNodes(headnode string) (ready_nodes []string, error_nodes []string, lost_nodes []string) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible\n", headnode)
		return
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()

	// Get nodes reporting to the headnode
	reply, err := c.GetNodes(ctx, &pb.Empty{})
	if err != nil {
		fmt.Println("Could not get nodes:", err)
	}
	return reply.GetReadyNodes(), reply.GetErrorNodes(), reply.GetLostNodes()
}
