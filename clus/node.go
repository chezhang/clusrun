package main

import (
	pb "clusrun/protobuf"
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"
)

func Node(args []string) {
	fs := flag.NewFlagSet("clus node options", flag.ExitOnError)
	SetGlobalParameters(fs)
	filterBy_pattern := fs.String("pattern", "", "filter nodes matching the specified regular expression pattern")
	filterBy_state := fs.String("state", "", "filter nodes in the specified state (ready, error or lost)")
	filterBy_groups := fs.String("groups", "", "filter nodes in the specified node groups")
	filterBy_groups_in_file := fs.String("groups-in-file", "", "filter nodes in the node groups specified by a file")
	filterBy_groups_intersect := fs.Bool("intersect", false, "specify to filter nodes in intersection (union if not specified) of node groups")
	groupBy := fs.String("group-by", "", "group the nodes by state or node group")         // name prefix, running jobs
	orderBy := fs.String("order-by", "name", "sort the nodes by node name or node groups") // running jobs
	format := fs.String("format", "table", "format the nodes in table, list or group")
	addGroups := fs.String("add-groups", "", "add nodes to the specified node groups")
	removeGroups := fs.String("remove-groups", "", "remove nodes from the specified node groups")
	// prefix := fs.Int("prefix", 0, "merge the nodes with same name prefix of specified length (only in table format)")
	// monitor := fs.Bool("monitor", false, "keep refreshing the node information")
	// purge := fs.Bool("purge", false, "purge the lost nodes in headnode")
	// reverse := fs.Bool("reverse", false, "reverse the order when displaying")
	_ = fs.Parse(args)
	if len(fs.Args()) > 0 {
		// TODO: query nodes info
		Fatallnf("Invalid parameter: %v", strings.Join(fs.Args(), " "))
	}

	// Get nodes
	groups := ParseNodesOrGroups(*filterBy_groups, *filterBy_groups_in_file)
	nodes := getNodes(*filterBy_pattern, *filterBy_state, groups, *filterBy_groups_intersect)

	// Add or remove node groups
	var groupMsgs []string
	if len(nodes) > 0 {
		setGroups := false
		if *addGroups != "" {
			groupMsgs = append(groupMsgs, setNodeGroups(*addGroups, nodes, false))
			setGroups = true
		}
		if *removeGroups != "" {
			groupMsgs = append(groupMsgs, setNodeGroups(*removeGroups, nodes, true))
			setGroups = true
		}
		if setGroups {
			nodes = getNodes(*filterBy_pattern, *filterBy_state, groups, *filterBy_groups_intersect)
		}
	}
	printGroupMsgs := func() {
		if len(groupMsgs) > 0 {
			for _, msg := range groupMsgs {
				Printlnf(msg)
			}
		}
	}

	// Print nodes
	switch strings.ToLower(*format) {
	case "table":
		nodePrintTable(nodes, *groupBy, *orderBy)
		printGroupMsgs()
	case "list":
		nodePrintList(nodes, *groupBy, *orderBy)
		printGroupMsgs()
	case "group":
		nodePrintGroups(nodes, *groupBy)
		printGroupMsgs()
	default:
		Fatallnf("Invalid format option: %v", *format)
	}
}

func getNodes(pattern, state string, groups []string, intersect bool) (nodes []*pb.Node) {
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
		Fatallnf("Invalid node state option: %v", state)
	}

	// Setup connection
	conn, cancel := ConnectHeadnode()
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Get nodes reporting to the headnode
	reply, err := c.GetNodes(ctx, &pb.GetNodesRequest{Pattern: pattern, Groups: groups, State: node_state, GroupsIntersect: intersect})
	if err != nil {
		Fatallnf("Could not get nodes: %v", err)
	}
	return reply.GetNodes()
}

func nodePrintTable(nodes []*pb.Node, group_by, order_by string) {
	groups := getSortedGroups(nodes, group_by)
	if len(groups) > 0 {
		gap := 3
		max_name_length, max_state_length, max_groups_length := getNodeTableMaxLength(nodes)
		header_node, header_state, header_groups := "Node", "State", "Groups"
		min_groups_length := len(header_groups) + gap
		if max_name_length < len(header_node) {
			max_name_length = len(header_node)
		}
		if max_state_length < len(header_state) {
			max_state_length = len(header_state)
		}
		name_width, state_width := max_name_length+gap, max_state_length+gap
		line_length := DefaultLineLength
		if ConsoleWidth > 0 {
			line_length = ConsoleWidth - 1
		}
		remain_length := line_length - name_width - state_width
		if remain_length < min_groups_length {
			remain_length = min_groups_length
		}
		if max_groups_length == 0 {
			state_width = max_state_length
			header_groups = ""
		} else {
			if max_groups_length > remain_length {
				max_groups_length = remain_length
			}
			if max_groups_length < len(header_groups) {
				max_groups_length = len(header_groups)
			}
		}
		for _, group := range groups {
			sortNodes(group, order_by)
		}
		groups_width := max_groups_length
		Printlnf("%-*s%-*s%-*s",
			name_width, header_node,
			state_width, header_state,
			groups_width, header_groups)
		Printlnf("%-*s%-*s%-*s",
			name_width, strings.Repeat("-", max_name_length),
			state_width, strings.Repeat("-", max_state_length),
			groups_width, strings.Repeat("-", max_groups_length))
		for i := range groups {
			for _, node := range groups[i] {
				node_groups := strings.Join(node.Groups, ", ")
				if len(node_groups) > max_groups_length {
					padding := "..."
					node_groups = node_groups[:max_groups_length-len(padding)]
					node_groups += padding
				}
				Printlnf("%-*s%-*s%-*s",
					name_width, node.Name,
					state_width, node.State,
					groups_width, node_groups)
			}
			if i < len(groups)-1 {
				Printlnf("")
			}
		}
		Printlnf(strings.Repeat("-", name_width+state_width+groups_width))
	}
	Printlnf("Node count: %v", len(nodes))
}

func nodePrintList(nodes []*pb.Node, group_by, order_by string) {
	item_node, item_state, item_groups := "Node", "State", "Groups"
	maxLength := MaxInt(len(item_node), len(item_state), len(item_groups))
	print := func(item string, value interface{}) {
		Printlnf("%-*v : %v", maxLength, item, value)
	}
	groups := getSortedGroups(nodes, group_by)
	for i := range groups {
		nodes := groups[i]
		sortNodes(nodes, order_by)
		for j := range nodes {
			print(item_node, nodes[j].Name)
			print(item_state, nodes[j].State)
			g := strings.Join(nodes[j].Groups, ", ")
			if len(g) > 0 {
				print(item_groups, g)
			}
			Printlnf(GetPaddingLine(""))
		}
	}
	Printlnf("Node count: %v", len(nodes))
}

func nodePrintGroups(nodes []*pb.Node, group_by string) {
	if len(group_by) == 0 {
		Fatallnf("Please specify group-by option.")
	}
	type group struct {
		name  string
		nodes []string
	}
	groups := []group{}
	for k, v := range getNodesByGroup(nodes, group_by, true) {
		names := []string{}
		for i := range v {
			names = append(names, v[i].Name)
		}
		sort.Strings(names)
		groups = append(groups, group{k, names})
	}
	if len(groups) == 0 {
		Printlnf("No group of nodes.")
		return
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Compare(groups[i].name, groups[j].name) < 0 })
	for i := range groups {
		printGroup(groups[i].name, groups[i].nodes)
	}
	Printlnf(GetPaddingLine(""))
	for i := range groups {
		category := "group"
		if strings.ToLower(group_by) == "state" {
			category = "state"
		}
		label := "no group"
		if group := groups[i].name; len(group) > 0 {
			label = fmt.Sprintf("%v '%v'", category, group)
		}
		Printlnf("Count of nodes in %v: %v", label, len(groups[i].nodes))
	}
}

func setNodeGroups(nodeGroups string, nodes []*pb.Node, remove bool) string {
	// Parse node groups
	all := false
	groups := strings.Split(nodeGroups, ",")
	for i, group := range groups {
		groups[i] = strings.TrimSpace(group)
		if len(groups[i]) == 0 {
			Fatallnf("Empty group name.")
		}
		if groups[i] == "*" {
			all = true
			if !remove {
				Fatallnf("Invalid group name: *")
			}
		}
	}

	// Setup connection
	conn, cancel := ConnectHeadnode()
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Add or remove node groups for nodes
	if _, err := c.SetNodeGroups(ctx, &pb.SetNodeGroupsRequest{Groups: groups, Nodes: nodes, Remove: remove}); err != nil {
		Fatallnf("Could not set node groups: %v", err)
	}
	v := "added to"
	if remove {
		v = "removed from"
	}
	t := fmt.Sprintf("node groups: %v", strings.Join(groups, ", "))
	if all {
		t = "all node groups"
	}
	return fmt.Sprintf("Nodes are %v %v", v, t)
}

func printGroup(name string, nodes []string) {
	if len(nodes) > 0 {
		if len(name) > 0 {
			name = fmt.Sprintf("[%v]", name)
		}
		Printlnf(GetPaddingLine(fmt.Sprintf("---%v---", name)))
		max_name_length := 0
		for i := range nodes {
			if length := len(nodes[i]); length > max_name_length {
				max_name_length = length
			}
		}
		sort.Strings(nodes)
		if ConsoleWidth == 0 {
			Printlnf(strings.Join(nodes, ", "))
		} else {
			padding := 3
			width := max_name_length + padding
			count := (ConsoleWidth + padding) / width
			if count == 0 {
				count = 1
			}
			for i := range nodes {
				fmt.Print(nodes[i])
				length := len(nodes[i])
				if i%count == count-1 {
					Printlnf("")
				} else {
					fmt.Print(strings.Repeat(" ", width-length))
				}
			}
			Printlnf("")
		}
	}
}

func getNodesByGroup(nodes []*pb.Node, groupby string, separate_group bool) map[string][]*pb.Node {
	groups := map[string][]*pb.Node{}
	switch strings.ToLower(groupby) {
	case "":
		if len(nodes) > 0 {
			groups[""] = nodes
		}
	case "state":
		for i := range nodes {
			state := nodes[i].State.String()
			groups[state] = append(groups[state], nodes[i])
		}
	case "node", "group", "nodegroup":
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
		Fatallnf("Invalid group-by option: %v", groupby)
	}
	return groups
}

func getNodeTableMaxLength(nodes []*pb.Node) (max_name_length, max_state_length, max_groups_length int) {
	for i := range nodes {
		if length := len(nodes[i].Name); length > max_name_length {
			max_name_length = length
		}
		if length := len(nodes[i].State.String()); length > max_state_length {
			max_state_length = length
		}
		if length := len(strings.Join(nodes[i].Groups, ", ")); length > max_groups_length {
			max_groups_length = length
		}
	}
	return
}

func getSortedGroups(nodes []*pb.Node, group_by string) (sorted_groups [][]*pb.Node) {
	type group struct {
		name  string
		nodes []*pb.Node
	}
	groups := []group{}
	for k, v := range getNodesByGroup(nodes, group_by, false) {
		groups = append(groups, group{k, v})
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Compare(groups[i].name, groups[j].name) < 0 })
	for i := range groups {
		sorted_groups = append(sorted_groups, groups[i].nodes)
	}
	return
}

func sortNodes(nodes []*pb.Node, order_by string) {
	sorters := strings.Split(order_by, ",")
	sort.Slice(nodes, func(i, j int) bool {
		for k := range sorters {
			switch strings.ToLower(strings.TrimSpace(sorters[k])) {
			case "name", "nodename":
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
				/*
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
				*/
			default:
				Fatallnf("Invalid order-by option: %v", order_by)
			}
		}
		return false
	})
}
