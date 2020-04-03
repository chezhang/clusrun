package main

import (
	pb "../protobuf"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	label_last_job = -1
	label_all_jobs = -2
)

func Job(args []string) {
	fs := flag.NewFlagSet("clus job options", flag.ExitOnError)
	headnode := fs.String("headnode", local_host, "specify the headnode to connect")
	format := fs.String("format", "", "format the jobs in table or list")
	cancel := fs.Bool("cancel", false, "cancel jobs")
	// output := fs.Bool("output", false, "get output of job(s)")
	// nodes := fs.String("nodes", "", "get info or output of jobs on certain nodes")
	// state := fs.String("state", "", "get jobs in certain state")
	fs.Parse(args)
	no_job_args := len(fs.Args()) == 0
	job_ids := ParseJobIds(fs.Args())
	if *cancel {
		if no_job_args {
			job_ids[label_last_job] = false
		}
		CancelJobs(ParseHeadnode(*headnode), job_ids)
		return
	}
	if no_job_args {
		job_ids[label_all_jobs] = false
	}
	jobs := GetJobs(ParseHeadnode(*headnode), job_ids)
	if len(*format) == 0 {
		if no_job_args {
			*format = "table"
		} else {
			*format = "list"
		}
	}
	switch strings.ToLower(*format) {
	case "table":
		JobPrintTable(jobs)
	case "list":
		JobPrintList(jobs)
	default:
		fmt.Println("Invalid format option:", *format)
		return
	}
}

func ParseJobIds(args []string) map[int32]bool {
	job_ids := map[int32]bool{}
	if len(args) > 0 {
		if len(args) == 1 && (args[0] == "*" || strings.ToLower(args[0]) == "all") {
			job_ids[label_all_jobs] = false
			return job_ids
		}
		for _, arg := range args {
			for _, id := range strings.Split(arg, ",") {
				var begin, end string
				if parts := strings.Split(id, "-"); len(parts) == 1 {
					begin = parts[0]
					end = parts[0]
				} else if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
					begin = parts[0]
					end = parts[1]
				} else {
					fmt.Printf("Invalid job range: %q\n", id)
					os.Exit(0)
				}
				ids := make([]int, 2)
				for i, val := range []string{begin, end} {
					if job_id, err := strconv.Atoi(strings.TrimSpace(val)); err != nil || job_id <= 0 {
						fmt.Printf("Invalid job id: %q\n", val)
						os.Exit(0)
					} else {
						ids[i] = job_id
					}
				}
				for i := ids[0]; i <= ids[1]; i++ {
					job_ids[int32(i)] = false
				}
			}
		}
	}
	return job_ids
}

func CancelJobs(headnode string, job_ids map[int32]bool) {
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

	// Cancel job(s) in the cluster
	reply, err := c.CancelClusJobs(ctx, &pb.CancelClusJobsRequest{JobIds: job_ids})
	if err != nil {
		fmt.Println("Can not cancel job(s):", err)
		os.Exit(0)
	}
	result := reply.GetResult()
	if len(result) == 0 {
		fmt.Println("No job is cancelled.")
	} else {
		ids := make([]int32, 0, len(result))
		for id := range result {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			fmt.Printf("Job %v is %v\n", id, result[id])
		}
	}
}

func GetJobs(headnode string, ids map[int32]bool) []*pb.Job {
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

	// Get job(s) in the cluster
	reply, err := c.GetJobs(ctx, &pb.GetJobsRequest{JobIds: ids})
	if err != nil {
		fmt.Println("Can not get job(s):", err)
		os.Exit(0)
	}
	jobs := reply.GetJobs()
	if _, ok := ids[label_all_jobs]; !ok && len(ids) > 0 {
		for _, job := range jobs {
			ids[job.Id] = true
		}
		no_jobs := []int{}
		for k, v := range ids {
			if !v {
				no_jobs = append(no_jobs, int(k))
			}
		}
		if len(no_jobs) > 0 {
			sort.Ints(no_jobs)
			fmt.Println("Can not get job(s):", no_jobs)
			fmt.Println()
		}
	}
	return jobs
}

func JobPrintTable(jobs []*pb.Job) {
	if len(jobs) > 0 {
		gap := 3
		min_command_length := 10
		max_id_length, max_state_length, max_nodes_length, max_command_length := GetJobTableMaxLength(jobs)
		header_id, header_state, header_nodes, header_command := "Id", "State", "Nodes", "Command"
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
		line_length := default_line_length
		if console_width > 0 {
			line_length = console_width - 1
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

func JobPrintList(jobs []*pb.Job) {
	fmt.Println(GetPaddingLine(""))
	for _, job := range jobs {
		fmt.Println("Id:", job.Id)
		fmt.Println("State:", job.State)
		fmt.Println("Create Time:", time.Unix(job.CreateTime, 0))
		fmt.Println("End Time:", time.Unix(job.EndTime, 0))
		fmt.Println("Nodes:", job.Nodes)
		fmt.Println("Command:", job.Command)
		fmt.Println(GetPaddingLine(""))
	}
	fmt.Println("Job count:", len(jobs))
}

func GetJobTableMaxLength(jobs []*pb.Job) (id, state, nodes, command int) {
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
