package main

import (
	pb "../protobuf"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"os"
	"sort"
	"strings"
)

func Node(args []string) {
	fs := flag.NewFlagSet("clus node options", flag.ExitOnError)
	headnode := fs.String("headnode", local_host, "specify the headnode to connect")
	pattern := fs.String("pattern", "", "get nodes matching a certain regular expression pattern")
	filter_by_state := fs.String("state", "", "get nodes in certain state (ready, error or lost)")
	filter_by_groups := fs.String("groups", "", "get nodes in certain node groups")
	group_by := fs.String("groupby", "", "group the nodes by name prefix, state or node group")
	order_by := fs.String("orderby", "name", "sort the nodes by name, node groups or running jobs")
	format := fs.String("format", "table", "format the nodes in table, list or group")
	// prefix := fs.Int("prefix", 0, "merge the nodes with same name prefix of specified length (only in table format)")
	// monitor := fs.Bool("monitor", false, "keep refreshing the node information")
	// purge := fs.Bool("purge", false, "purge the lost nodes in headnode")
	fs.Parse(args)
	if len(fs.Args()) > 0 {
		fmt.Println("Invalid parameter:", fs.Args())
		return
	}
	nodes := GetNodes(ParseHeadnode(*headnode), *pattern, *filter_by_state, *filter_by_groups)
	switch strings.ToLower(*format) {
	case "table":
		NodePrintTable(nodes, *group_by, *order_by)
	case "list":
		NodePrintList(nodes, *group_by, *order_by)
	case "group":
		NodePrintGroups(nodes, *group_by)
	default:
		fmt.Println("Invalid format option:", *format)
		return
	}
}

func GetNodes(headnode, pattern, state, groups string) (nodes []*pb.GetNodesReply_Node) {
	// Validate node state
	node_state := pb.NodeState_Unknown
	switch strings.ToLower(state) {
	case "":
		node_state = pb.NodeState_Unknown
	case "ready":
		node_state = pb.NodeState_Ready
	case "error":
		node_state = pb.NodeState_Error
	case "lost":
		node_state = pb.NodeState_Lost
	default:
		fmt.Println("Invalid node state option:", state)
		os.Exit(0)
	}

	// Parse groups
	node_groups_set := map[string]bool{}
	node_groups := strings.Split(groups, ",")
	for i := range node_groups {
		node_groups_set[strings.TrimSpace(node_groups[i])] = true
	}
	node_groups = make([]string, 0, len(node_groups_set))
	for k, _ := range node_groups_set {
		node_groups = append(node_groups, k)
	}

	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible\n", headnode)
		os.Exit(0)
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()

	// Get nodes reporting to the headnode
	reply, err := c.GetNodes(ctx, &pb.GetNodesRequest{Pattern: pattern, Groups: node_groups, State: node_state})
	if err != nil {
		fmt.Println("Could not get nodes:", err)
	}
	return reply.GetNodes()
}

func NodePrintTable(nodes []*pb.GetNodesReply_Node, group_by, order_by string) {
	groups := GetSortedGroups(nodes, group_by)
	if len(groups) > 0 {
		gap := 3
		max_name_length, max_state_length := GetNodeTableMaxLength(nodes)
		name_width, state_width := max_name_length+gap, max_state_length+gap
		fmt.Printf("%-*s%-*s\n", name_width, "Node", state_width, "State")
		fmt.Printf("%-*s%-*s\n", name_width, strings.Repeat("-", max_name_length), state_width, strings.Repeat("-", max_state_length))
		for i := range groups {
			nodes := groups[i]
			SortNodes(nodes, order_by)
			for j := range nodes {
				fmt.Printf("%-*s%-*s\n", name_width, nodes[j].Name, state_width, nodes[j].State)
			}
			if i < len(groups)-1 {
				fmt.Println()
			}
		}
		fmt.Println(strings.Repeat("-", name_width+max_state_length))
	}
	fmt.Println("Node count:", len(nodes))
}

func NodePrintList(nodes []*pb.GetNodesReply_Node, group_by, order_by string) {
	groups := GetSortedGroups(nodes, group_by)
	for i := range groups {
		nodes := groups[i]
		SortNodes(nodes, order_by)
		for j := range nodes {
			fmt.Println("Name:", nodes[j].Name)
			fmt.Println("State:", nodes[j].State)
			fmt.Println()
		}
	}
	fmt.Println("Node count:", len(nodes))
}

func NodePrintGroups(nodes []*pb.GetNodesReply_Node, group_by string) {
	type group struct {
		name  string
		nodes []string
	}
	groups := []group{}
	for k, v := range GetNodesByGroup(nodes, group_by, true) {
		names := []string{}
		for i := range v {
			names = append(names, v[i].Name)
		}
		sort.Strings(names)
		groups = append(groups, group{k, names})
	}
	if len(groups) == 0 {
		fmt.Println("No group of nodes.")
		return
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Compare(groups[i].name, groups[j].name) < 0 })
	for i := range groups {
		PrintGroup(groups[i].name, groups[i].nodes)
	}
	fmt.Println(GetPaddingLine(""))
	for i := range groups {
		category := "Group"
		if strings.ToLower(group_by) == "state" {
			category = "State"
		}
		label := "no group"
		if group := groups[i].name; len(group) > 0 {
			label = fmt.Sprintf("%v '%v'", category, group)
		}
		fmt.Printf("Count of nodes in %v: %v\n", label, len(groups[i].nodes))
	}
}

func PrintGroup(name string, nodes []string) {
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

func GetNodesByGroup(nodes []*pb.GetNodesReply_Node, groupby string, separate_group bool) map[string][]*pb.GetNodesReply_Node {
	groups := map[string][]*pb.GetNodesReply_Node{}
	switch strings.ToLower(groupby) {
	case "":
		if len(nodes) > 0 {
			groups["All"] = nodes
		}
	case "state":
		for i := range nodes {
			state := nodes[i].State.String()
			groups[state] = append(groups[state], nodes[i])
		}
	case "group", "nodegroup":
		for i := range nodes {
			node_groups := nodes[i].Groups
			if len(node_groups) == 0 { // Node not in any node group
				node_groups = append(node_groups, "")
			}
			if separate_group {
				for j := range node_groups {
					group := node_groups[j]
					groups[group] = append(groups[group], nodes[i])
				}
			} else {
				sort.Strings(node_groups)
				group := strings.Join(node_groups, ",")
				groups[group] = append(groups[group], nodes[i])
			}
		}
	default:
		fmt.Println("Invalid groupby option:", groupby)
		os.Exit(0)
	}
	return groups
}

func GetNodeTableMaxLength(nodes []*pb.GetNodesReply_Node) (max_name_length, max_state_length int) {
	for i := range nodes {
		if length := len(nodes[i].Name); length > max_name_length {
			max_name_length = length
		}
		if length := len(nodes[i].State.String()); length > max_state_length {
			max_state_length = length
		}
	}
	return
}

func GetSortedGroups(nodes []*pb.GetNodesReply_Node, group_by string) (sorted_groups [][]*pb.GetNodesReply_Node) {
	type group struct {
		name  string
		nodes []*pb.GetNodesReply_Node
	}
	groups := []group{}
	for k, v := range GetNodesByGroup(nodes, group_by, false) {
		groups = append(groups, group{k, v})
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Compare(groups[i].name, groups[j].name) < 0 })
	for i := range groups {
		sorted_groups = append(sorted_groups, groups[i].nodes)
	}
	return
}

func SortNodes(nodes []*pb.GetNodesReply_Node, order_by string) {
	sorters := strings.Split(order_by, ",")
	sort.Slice(nodes, func(i, j int) bool {
		for k := range sorters {
			switch strings.ToLower(strings.TrimSpace(sorters[k])) {
			case "name":
				result := strings.Compare(nodes[i].Name, nodes[j].Name)
				if result != 0 {
					return result < 0
				}
			case "group", "groups", "nodegroup", "nodegroups":
				result := len(nodes[i].Groups) - len(nodes[j].Groups)
				if result != 0 {
					return result < 0
				}
				left_groups := strings.Join(nodes[i].Groups, ",")
				right_groups := strings.Join(nodes[j].Groups, ",")
				result = strings.Compare(left_groups, right_groups)
				if result != 0 {
					return result < 0
				}
			case "job", "jobs", "runningjob", "runningjobs":
				left_jobs, right_jobs := nodes[i].Jobs, nodes[j].Jobs
				result := len(left_jobs) - len(right_jobs)
				if result != 0 {
					return result < 0
				}
				sort.Slice(left_jobs, func(m, n int) bool { return left_jobs[m] < left_jobs[n] })
				sort.Slice(right_jobs, func(m, n int) bool { return right_jobs[m] < right_jobs[n] })
				for m := range left_jobs {
					if left_jobs[m] != right_jobs[m] {
						return left_jobs[m] < right_jobs[m]
					}
				}
			default:
				fmt.Println("Invalid orderby option:", order_by)
				os.Exit(0)
			}
		}
		return false
	})
	return
}
