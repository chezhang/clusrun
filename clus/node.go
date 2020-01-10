package main

import (
	pb "../protobuf"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"sort"
	"strings"
)

func Node(args []string) {
	fs := flag.NewFlagSet("clus node options", flag.ExitOnError)
	headnode := fs.String("headnode", local_host, "specify the headnode to connect")
	pattern := fs.String("pattern", "", "get nodes matching a certain regular expression pattern")
	monitor := fs.Bool("monitor", false, "keep refreshing the node information")
	group_by_state := fs.Bool("state", false, "group the nodes by node state")
	// group_by_group := fs.Bool("group", false, "group the nodes by node group")
	fs.Parse(args)
	if len(fs.Args()) > 0 {
		fmt.Printf("Invalid parameter: %v", fs.Args())
		return
	}
	if !*monitor {
		nodes := GetNodes(ParseHeadnode(*headnode), *pattern)
		if *group_by_state {
			ready_nodes, error_nodes, lost_nodes := []string{}, []string{}, []string{}
			for i := range nodes {
				name := nodes[i].Name
				switch nodes[i].State {
				case pb.NodeState_Ready:
					ready_nodes = append(ready_nodes, name)
				case pb.NodeState_Error:
					error_nodes = append(error_nodes, name)
				case pb.NodeState_Lost:
					lost_nodes = append(lost_nodes, name)
				}
			}
			PrintNodes(ready_nodes, "Ready nodes")
			PrintNodes(error_nodes, "Error nodes")
			PrintNodes(lost_nodes, "Lost nodes")
			fmt.Println(GetPaddingLine(""))
			fmt.Println("Ready nodes count:", len(ready_nodes))
			fmt.Println("Error nodes count:", len(error_nodes))
			fmt.Println("Lost nodes count:", len(lost_nodes))
		} else {
			sort.Slice(nodes, func(i, j int) bool { return strings.Compare(nodes[i].Name, nodes[j].Name) < 0 })
			max_name_length := 0
			for i := range nodes {
				if length := len(nodes[i].Name); length > max_name_length {
					max_name_length = length
				}
			}
			name_width := max_name_length + 3
			max_state_length, state_width := 5, 5
			fmt.Printf("%-*s%-*s\n", name_width, "Node", state_width, "State")
			fmt.Printf("%-*s%-*s\n", name_width, strings.Repeat("-", max_name_length), state_width, strings.Repeat("-", max_state_length))
			for i := range nodes {
				fmt.Printf("%-*s%-*s\n", name_width, nodes[i].Name, state_width, nodes[i].State)
			}
			fmt.Println(strings.Repeat("-", name_width+max_state_length))
			fmt.Println("Node count:", len(nodes))
		}
	} else {
		fmt.Println("Not implemented yet")
	}
}

func GetNodes(headnode, pattern string) (nodes []*pb.GetNodesReply_Node) {
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
	reply, err := c.GetNodes(ctx, &pb.GetNodesRequest{Pattern: pattern})
	if err != nil {
		fmt.Println("Could not get nodes:", err)
	}
	return reply.GetNodes()
}

func PrintNodes(nodes []string, name string) {
	if len(nodes) > 0 {
		fmt.Println(GetPaddingLine(fmt.Sprintf("---%v---", name)))
		max_name_length := 0
		for i := range nodes {
			if length := len(nodes[i]); length > max_name_length {
				max_name_length = length
			}
		}
		sort.Strings(nodes)
		if console_width == 0 {
			fmt.Println(strings.Join(nodes, ", "))
		} else {
			padding := 3
			width := max_name_length + padding
			count := (console_width + padding) / width
			if count == 0 {
				count = 1
			}
			for i := range nodes {
				fmt.Print(nodes[i])
				length := len(nodes[i])
				if i%count == count-1 {
					fmt.Println()
				} else {
					fmt.Print(strings.Repeat(" ", width-length))
				}
			}
			fmt.Println()
		}
	}
}
