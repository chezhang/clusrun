package main

import (
	pb "clusrun/protobuf"
	"context"
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
	retry := fs.Bool("retry", false, "retry jobs on the failed nodes")
	// output := fs.Bool("output", false, "get output of jobs")
	// nodes := fs.String("nodes", "", "get info or output of jobs on certain nodes")
	// state := fs.String("state", "", "get jobs in certain state")
	_ = fs.Parse(args)
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
		if *retry {
			fmt.Println("Conflict options: -rerun and -retry")
		} else if no_job_args {
			fmt.Println("Please specify jobs to rerun.")
		} else if len(jobs) == 0 {
			fmt.Println("No jobs to rerun.")
		} else {
			for _, job := range jobs {
				fmt.Printf("Rerun job %v: ", job.Id)
				RunJob(ParseHeadnode(*headnode), job.Command, job.Sweep, "", job.NodePattern, job.NodeGroups, job.SpecifiedNodes, job.Arguments, 0, 0, true, false)
			}
		}
		return
	}
	if *retry {
		if no_job_args {
			fmt.Println("Please specify jobs to retry.")
		} else if len(jobs) == 0 {
			fmt.Println("No jobs to retry.")
		} else {
			for _, job := range jobs {
				fmt.Printf("Retry job %v: ", job.Id)
				// TODO when needed: retry same job in server rather than create new job
				if len(job.FailedNodes) == 0 {
					fmt.Println("No failed nodes.")
				} else if len(job.Sweep) > 0 {
					fmt.Println("Can not retry job with sweep option.")
				} else {
					failedNodes := make([]string, 0, len(job.FailedNodes))
					for node := range job.FailedNodes {
						failedNodes = append(failedNodes, node)
					}
					RunJob(ParseHeadnode(*headnode), job.Command, "", "", "", nil, failedNodes, job.Arguments, 0, 0, true, false)
				}
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
				err = fmt.Errorf("Invalid range: %q", id)
				return
			}
			ids := make([]int, 2)
			for i, val := range []string{begin, end} {
				if job_id, e := strconv.Atoi(strings.TrimSpace(val)); e != nil || job_id == 0 || inverse && job_id < 0 {
					err = fmt.Errorf("Invalid id: %q", val)
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
	conn, cancel := ConnectHeadnode(headnode)
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
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
	conn, cancel := ConnectHeadnode(headnode)
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()

	// Get jobs in the cluster
	reply, err := c.GetJobs(ctx, &pb.GetJobsRequest{JobIds: ids}, grpc.UseCompressor("gzip"))
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
		min_console_width_for_create_time := 120
		min_console_width_for_end_time := 150
		max_id_length, max_state_length, max_progress_length, max_create_time_length, max_end_time_length, max_command_length := getJobTableMaxLength(jobs)
		header_id, header_state, header_progress, header_create_time, header_end_time, header_command := "Id", "State", "Progress", "Create Time", "End Time", "Command"
		min_command_length := len(header_command) + gap
		if max_id_length < len(header_id) {
			max_id_length = len(header_id)
		}
		if max_state_length < len(header_state) {
			max_state_length = len(header_state)
		}
		if max_progress_length < len(header_progress) {
			max_progress_length = len(header_progress)
		}
		if max_create_time_length < len(header_create_time) {
			max_create_time_length = len(header_create_time)
		}
		if max_end_time_length < len(header_end_time) {
			max_end_time_length = len(header_end_time)
		}
		id_width, state_width, progress_width, create_time_width, end_time_width := max_id_length+gap, max_state_length+gap, max_progress_length+gap, max_create_time_length+gap, max_end_time_length+gap
		line_length := DefaultLineLength
		if ConsoleWidth > 0 {
			line_length = ConsoleWidth - 1
		}
		remain_length := line_length - id_width - state_width - progress_width
		if line_length > min_console_width_for_create_time {
			remain_length -= create_time_width
		} else {
			header_create_time = ""
			max_create_time_length = 0
			create_time_width = 0
		}
		if line_length > min_console_width_for_end_time {
			remain_length -= end_time_width
		} else {
			header_end_time = ""
			max_end_time_length = 0
			end_time_width = 0
		}
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
		fmt.Printf("%-*s%-*s%-*s%-*s%-*s%-*s\n",
			id_width, header_id,
			state_width, header_state,
			progress_width, header_progress,
			create_time_width, header_create_time,
			end_time_width, header_end_time,
			command_width, header_command)
		fmt.Printf("%-*s%-*s%-*s%-*s%-*s%-*s\n",
			id_width, strings.Repeat("-", max_id_length),
			state_width, strings.Repeat("-", max_state_length),
			progress_width, strings.Repeat("-", max_progress_length),
			create_time_width, strings.Repeat("-", max_create_time_length),
			end_time_width, strings.Repeat("-", max_end_time_length),
			command_width, strings.Repeat("-", max_command_length))
		for _, job := range jobs {
			command := job.Command
			if len(command) > max_command_length {
				padding := "..."
				command = command[:max_command_length-len(padding)]
				command += padding
			}
			create_time := ""
			if create_time_width > 0 {
				create_time = fmt.Sprintf("%v", time.Unix(job.CreateTime, 0))
			}
			end_time := ""
			if end_time_width > 0 && job.EndTime != 0 {
				end_time = fmt.Sprintf("%v", time.Unix(job.EndTime, 0))
			}
			fmt.Printf("%-*v%-*v%-*v%-*v%-*v%-*v\n",
				id_width, job.Id,
				state_width, job.State,
				progress_width, job.Progress,
				create_time_width, create_time,
				end_time_width, end_time,
				command_width, command)
		}
		fmt.Println(strings.Repeat("-", id_width+state_width+progress_width+create_time_width+end_time_width+command_width))
	}
	fmt.Println("Job count:", len(jobs))
}

func jobPrintList(jobs []*pb.Job) {
	item_id, item_state, item_progress, item_createTime, item_endTime, item_nodePattern, item_nodeGroups, item_specifiedNodes, item_nodes, item_failedNodes, item_cancelFailedNodes, item_sweep, item_arguments, item_command :=
		"Id", "State", "Progress", "Create Time", "End Time", "Node Pattern", "Node Grouops", "Specified Nodes", "Nodes", "Failed Nodes", "Cancel Failed Nodes", "Sweep Parameter", "Arguments", "Command"
	maxLength := MaxInt(len(item_id), len(item_state), len(item_progress), len(item_createTime), len(item_endTime), len(item_sweep), len(item_nodePattern),
		len(item_nodeGroups), len(item_specifiedNodes), len(item_nodes), len(item_failedNodes), len(item_cancelFailedNodes), len(item_arguments), len(item_command))
	print := func(name string, value interface{}) {
		fmt.Printf("%-*v : %v\n", maxLength, name, value)
	}
	for _, job := range jobs {
		print(item_id, job.Id)
		print(item_state, job.State)
		if progress := job.Progress; len(progress) > 0 {
			print(item_progress, progress)
		}
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
		if args := job.Arguments; len(args) > 0 {
			print(item_arguments, fmt.Sprintf("%q", args))
		}
		print(item_command, job.Command)
		fmt.Println(GetPaddingLine(""))
	}
	fmt.Println("Job count:", len(jobs))
}

func getJobTableMaxLength(jobs []*pb.Job) (id, state, progress, create_time, end_time, command int) {
	create_time = len(fmt.Sprintf("%v", time.Unix(0, 0)))
	for _, job := range jobs {
		if length := len(strconv.Itoa(int(job.Id))); length > id {
			id = length
		}
		if length := len(job.State.String()); length > state {
			state = length
		}
		if length := len(job.Progress); length > progress {
			progress = length
		}
		if job.EndTime != 0 {
			end_time = create_time
		}
		job.Command = strings.ReplaceAll(strings.ReplaceAll(job.Command, "\r", `\r`), "\n", `\n`)
		if length := len(job.Command); length > command {
			command = length
		}
	}
	return
}
