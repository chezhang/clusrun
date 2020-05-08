package main

import (
	pb "clusrun/protobuf"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

const (
	jobId_all = 0
)

func Job(args []string) {
	fs := flag.NewFlagSet("clus job options", flag.ExitOnError)
	headnode := fs.String("headnode", LocalHost, "specify the headnode to connect")
	format := fs.String("format", "", "format the jobs in table or list")
	cancel := fs.Bool("cancel", false, "cancel jobs")
	rerun := fs.Bool("rerun", false, "rerun jobs")
	// output := fs.Bool("output", false, "get output of jobs")
	// nodes := fs.String("nodes", "", "get info or output of jobs on certain nodes")
	// state := fs.String("state", "", "get jobs in certain state")
	fs.Parse(args)
	no_job_args := len(fs.Args()) == 0
	job_ids, err := parseJobIds(fs.Args())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if *cancel {
		if no_job_args {
			fmt.Println("Please specify jobs to cancel.")
			return
		}
		cancelJobs(ParseHeadnode(*headnode), job_ids)
		if !*rerun {
			return
		}
	}
	if no_job_args {
		job_ids[jobId_all] = false
	}
	jobs := getJobs(ParseHeadnode(*headnode), job_ids)
	if *rerun {
		if no_job_args {
			fmt.Println("Please specify jobs to rerun.")
		} else if len(jobs) == 0 {
			fmt.Println("No jobs to rerun.")
		} else {
			for _, job := range jobs {
				fmt.Printf("Rerun job %v: ", job.Id)
				RunJob(ParseHeadnode(*headnode), job.Command, job.Sweep, "", job.NodePattern, job.NodeGroups, job.SpecifiedNodes, 0, 0, true, false)
			}
		}
		return
	}
	if len(*format) == 0 {
		if no_job_args {
			*format = "table"
		} else {
			*format = "list"
		}
	}
	switch strings.ToLower(*format) {
	case "table":
		jobPrintTable(jobs)
	case "list":
		jobPrintList(jobs)
	default:
		fmt.Println("Invalid format option:", *format)
		return
	}
}

func parseJobIds(args []string) (job_ids map[int32]bool, err error) {
	job_ids = map[int32]bool{}
	for _, arg := range args {
		for _, id := range strings.Split(arg, ",") {
			if id == "*" || strings.ToLower(id) == "all" {
				job_ids[jobId_all] = false
				continue
			}
			if id == "~~" || strings.ToLower(id) == "last" {
				job_ids[-1] = false
				continue
			}
			inverse := false
			if len(id) > 0 && id[:1] == "~" {
				inverse = true
				id = id[1:]
			}
			var begin, end string
			if strings.Index(id, "-") <= 0 {
				begin = id
				end = id
			} else if parts := strings.Split(id, "-"); len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
				begin = parts[0]
				end = parts[1]
			} else {
				err = errors.New(fmt.Sprintf("Invalid range: %q", id))
				return
			}
			ids := make([]int, 2)
			for i, val := range []string{begin, end} {
				if job_id, e := strconv.Atoi(strings.TrimSpace(val)); e != nil || job_id == 0 || inverse && job_id < 0 {
					err = errors.New(fmt.Sprintf("Invalid id: %q", val))
					return
				} else {
					ids[i] = job_id
				}
			}
			for i := ids[0]; i <= ids[1]; i++ {
				id := i
				if inverse {
					id = -i
				}
				job_ids[int32(id)] = false
			}
		}
	}
	return
}

func cancelJobs(headnode string, job_ids map[int32]bool) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible\n", headnode)
		os.Exit(1)
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()

	// Cancel jobs in the cluster
	reply, err := c.CancelClusJobs(ctx, &pb.CancelClusJobsRequest{JobIds: job_ids})
	if err != nil {
		fmt.Println("Can not cancel jobs:", err)
		os.Exit(1)
	}
	result := reply.GetResult()
	if len(result) == 0 {
		fmt.Println("No job is cancelled.")
	} else {
		states := map[pb.JobState][]int32{}
		for id, state := range result {
			states[state] = append(states[state], id)
		}
		for state, ids := range states {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			fmt.Printf("%v jobs: %v\n", state, ids)
		}
	}
}

func getJobs(headnode string, ids map[int32]bool) []*pb.Job {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible\n", headnode)
		os.Exit(1)
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()

	// Get jobs in the cluster
	reply, err := c.GetJobs(ctx, &pb.GetJobsRequest{JobIds: ids})
	if err != nil {
		fmt.Println("Can not get jobs:", err)
		os.Exit(1)
	}
	jobs := reply.GetJobs()
	return jobs
}

func jobPrintTable(jobs []*pb.Job) {
	if len(jobs) > 0 {
		gap := 3
		max_id_length, max_state_length, max_nodes_length, max_command_length := getJobTableMaxLength(jobs)
		header_id, header_state, header_nodes, header_command := "Id", "State", "Nodes", "Command"
		min_command_length := len(header_command) + gap
		if max_id_length < len(header_id) {
			max_id_length = len(header_id)
		}
		if max_state_length < len(header_state) {
			max_state_length = len(header_state)
		}
		if max_nodes_length < len(header_nodes) {
			max_nodes_length = len(header_nodes)
		}
		id_width, state_width, nodes_width := max_id_length+gap, max_state_length+gap, max_nodes_length+gap
		line_length := DefaultLineLength
		if ConsoleWidth > 0 {
			line_length = ConsoleWidth - 1
		}
		remain_length := line_length - id_width - state_width - nodes_width
		if remain_length < min_command_length {
			remain_length = min_command_length
		}
		if max_command_length > remain_length {
			max_command_length = remain_length
		}
		if max_command_length < len(header_command) {
			max_command_length = len(header_command)
		}
		command_width := max_command_length
		fmt.Printf("%-*s%-*s%-*s%-*s\n",
			id_width, header_id,
			state_width, header_state,
			nodes_width, header_nodes,
			command_width, header_command)
		fmt.Printf("%-*s%-*s%-*s%-*s\n",
			id_width, strings.Repeat("-", max_id_length),
			state_width, strings.Repeat("-", max_state_length),
			nodes_width, strings.Repeat("-", max_nodes_length),
			command_width, strings.Repeat("-", max_command_length))
		for _, job := range jobs {
			command := job.Command
			if len(command) > max_command_length {
				padding := "..."
				command = command[:max_command_length-len(padding)]
				command += padding
			}
			fmt.Printf("%-*v%-*v%-*v%-*v\n",
				id_width, job.Id,
				state_width, job.State,
				nodes_width, len(job.Nodes),
				command_width, command)
		}
		fmt.Println(strings.Repeat("-", id_width+state_width+nodes_width+command_width))
	}
	fmt.Println("Job count:", len(jobs))
}

func jobPrintList(jobs []*pb.Job) {
	item_id, item_state, item_createTime, item_endTime, item_nodePattern, item_nodeGroups, item_specifiedNodes, item_nodes, item_failedNodes, item_cancelFailedNodes, item_sweep, item_command :=
		"Id", "State", "Create Time", "End Time", "Node Pattern", "Node Grouops", "Specified Nodes", "Nodes", "Failed Nodes", "Cancel Failed Nodes", "Sweep Parameter", "Command"
	maxLength := MaxInt(len(item_id), len(item_state), len(item_createTime), len(item_endTime), len(item_sweep), len(item_nodePattern),
		len(item_nodeGroups), len(item_specifiedNodes), len(item_nodes), len(item_failedNodes), len(item_cancelFailedNodes), len(item_command))
	print := func(name string, value interface{}) {
		fmt.Printf("%-*v : %v\n", maxLength, name, value)
	}
	for _, job := range jobs {
		print(item_id, job.Id)
		print(item_state, job.State)
		print(item_createTime, time.Unix(job.CreateTime, 0))
		if endTime := job.EndTime; endTime > 0 {
			print(item_endTime, time.Unix(endTime, 0))
		}
		if nodePattern := job.NodePattern; len(nodePattern) > 0 {
			print(item_nodePattern, nodePattern)
		}
		if nodeGroups := job.NodeGroups; len(nodeGroups) > 0 {
			print(item_nodeGroups, strings.Join(nodeGroups, ", "))
		}
		if specifiedNodes := job.SpecifiedNodes; len(specifiedNodes) > 0 {
			print(item_specifiedNodes, strings.Join(specifiedNodes, ", "))
		}
		print(item_nodes, strings.Join(job.Nodes, ", "))
		if failedNodes := job.FailedNodes; len(failedNodes) > 0 {
			nodes := make([]string, 0, len(failedNodes))
			for node := range failedNodes {
				nodes = append(nodes, node)
			}
			sort.Strings(nodes)
			for i := range nodes {
				if exitcode := failedNodes[nodes[i]]; exitcode != 0 {
					nodes[i] += fmt.Sprintf(" -> %v", exitcode)
				}
			}
			print(item_failedNodes, strings.Join(nodes, ", "))
		}
		if cancelFailedNodes := job.CancelFailedNodes; len(cancelFailedNodes) > 0 {
			print(item_cancelFailedNodes, strings.Join(cancelFailedNodes, ", "))
		}
		if sweep := job.Sweep; len(sweep) > 0 {
			print(item_sweep, sweep)
		}
		print(item_command, job.Command)
		fmt.Println(GetPaddingLine(""))
	}
	fmt.Println("Job count:", len(jobs))
}

func getJobTableMaxLength(jobs []*pb.Job) (id, state, nodes, command int) {
	for _, job := range jobs {
		if length := len(strconv.Itoa(int(job.Id))); length > id {
			id = length
		}
		if length := len(job.State.String()); length > state {
			state = length
		}
		if length := len(strconv.Itoa(len(job.Nodes))); length > nodes {
			nodes = length
		}
		job.Command = strings.ReplaceAll(strings.ReplaceAll(job.Command, "\r", `\r`), "\n", `\n`)
		if length := len(job.Command); length > command {
			command = length
		}
	}
	return
}
