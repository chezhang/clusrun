package main

import (
	pb "clusrun/protobuf"
	"context"
	"flag"
	"fmt"
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
	SetGlobalParameters(fs)
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
		Fatallnf("%v", err)
	}
	if *cancel {
		if no_job_args {
			Printlnf("Please specify jobs to cancel.")
			return
		}
		cancelJobs(job_ids)
		if !*rerun {
			return
		}
	}
	if no_job_args {
		job_ids[jobId_all] = false
	}
	jobs := getJobs(job_ids)
	if *rerun {
		if *retry {
			Printlnf("Conflict options: -rerun and -retry")
		} else if no_job_args {
			Printlnf("Please specify jobs to rerun.")
		} else if len(jobs) == 0 {
			Printlnf("No jobs to rerun.")
		} else {
			for _, job := range jobs {
				lebal := fmt.Sprintf("Rerun job %v", job.Id)
				fmt.Printf("%v: ", lebal)
				name := fmt.Sprintf("[%v] %v", lebal, job.Name)
				RunJob(job.Command, job.Sweep, "", job.NodePattern, name, job.NodeGroups, job.SpecifiedNodes, job.Arguments, 0, 0, true, false)
			}
		}
		return
	}
	if *retry {
		if no_job_args {
			Printlnf("Please specify jobs to retry.")
		} else if len(jobs) == 0 {
			Printlnf("No jobs to retry.")
		} else {
			for _, job := range jobs {
				lebal := fmt.Sprintf("Retry job %v", job.Id)
				fmt.Printf("%v: ", lebal)
				name := fmt.Sprintf("[%v] %v", lebal, job.Name)
				// TODO when needed: retry same job in server rather than create new job
				if len(job.FailedNodes) == 0 {
					Printlnf("No failed nodes.")
				} else if len(job.Sweep) > 0 {
					Printlnf("Can not retry job with sweep option.")
				} else {
					failedNodes := make([]string, 0, len(job.FailedNodes))
					for node := range job.FailedNodes {
						failedNodes = append(failedNodes, node)
					}
					RunJob(job.Command, "", "", "", name, nil, failedNodes, job.Arguments, 0, 0, true, false)
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
		Printlnf("Invalid format option: %v", *format)
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

func cancelJobs(job_ids map[int32]bool) {
	// Setup connection
	conn, cancel := ConnectHeadnode()
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Cancel jobs in the cluster
	reply, err := c.CancelClusJobs(ctx, &pb.CancelClusJobsRequest{JobIds: job_ids})
	if err != nil {
		Fatallnf("Can not cancel jobs: %v", err)
	}
	result := reply.GetResult()
	if len(result) == 0 {
		Printlnf("No job is cancelled.")
	} else {
		states := map[pb.JobState][]int32{}
		for id, state := range result {
			states[state] = append(states[state], id)
		}
		for state, ids := range states {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			Printlnf("%v jobs: %v", state, ids)
		}
	}
}

func getJobs(ids map[int32]bool) []*pb.Job {
	// Setup connection
	conn, cancel := ConnectHeadnode()
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Get jobs in the cluster
	reply, err := c.GetJobs(ctx, &pb.GetJobsRequest{JobIds: ids}, grpc.UseCompressor("gzip"))
	if err != nil {
		Fatallnf("Can not get jobs: %v", err)
	}
	jobs := reply.GetJobs()
	return jobs
}

func jobPrintTable(jobs []*pb.Job) {
	if len(jobs) > 0 {
		gap := 3
		min_console_width_for_name := 80
		min_console_width_for_create_time := 120
		min_console_width_for_end_time := 150
		max_id_length, max_name_length, max_state_length, max_progress_length, max_create_time_length, max_end_time_length, max_command_length := getJobTableMaxLength(jobs)
		header_id, header_name, header_state, header_progress, header_create_time, header_end_time, header_command := "Id", "Name", "State", "Progress", "Create Time", "End Time", "Command"
		if max_name_length > 20 {
			max_name_length = 20
		}
		min_command_length := len(header_command) + gap
		if max_id_length < len(header_id) {
			max_id_length = len(header_id)
		}
		if max_name_length < len(header_name) {
			max_name_length = len(header_name)
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
		id_width, name_width, state_width, progress_width, create_time_width, end_time_width :=
			max_id_length+gap, max_name_length+gap, max_state_length+gap, max_progress_length+gap, max_create_time_length+gap, max_end_time_length+gap
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
		if line_length > min_console_width_for_name {
			remain_length -= name_width
		} else {
			header_name = ""
			max_name_length = 0
			name_width = 0
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
		Printlnf("%-*s%-*s%-*s%-*s%-*s%-*s%-*s",
			id_width, header_id,
			name_width, header_name,
			state_width, header_state,
			progress_width, header_progress,
			create_time_width, header_create_time,
			end_time_width, header_end_time,
			command_width, header_command)
		Printlnf("%-*s%-*s%-*s%-*s%-*s%-*s%-*s",
			id_width, strings.Repeat("-", max_id_length),
			name_width, strings.Repeat("-", max_name_length),
			state_width, strings.Repeat("-", max_state_length),
			progress_width, strings.Repeat("-", max_progress_length),
			create_time_width, strings.Repeat("-", max_create_time_length),
			end_time_width, strings.Repeat("-", max_end_time_length),
			command_width, strings.Repeat("-", max_command_length))
		for _, job := range jobs {
			padding := "..."
			name := ""
			if name_width > 0 {
				name = job.Name
			}
			if len(name) > max_name_length {
				name = name[:max_name_length-len(padding)]
				name += padding
			}
			command := job.Command
			if len(command) > max_command_length {
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
			Printlnf("%-*v%-*v%-*v%-*v%-*v%-*v%-*v",
				id_width, job.Id,
				name_width, name,
				state_width, job.State,
				progress_width, job.Progress,
				create_time_width, create_time,
				end_time_width, end_time,
				command_width, command)
		}
		Printlnf(strings.Repeat("-", id_width+name_width+state_width+progress_width+create_time_width+end_time_width+command_width))
	}
	Printlnf("Job count: %v", len(jobs))
}

func jobPrintList(jobs []*pb.Job) {
	item_id, item_name, item_state, item_progress, item_createTime, item_endTime, item_nodePattern, item_nodeGroups, item_specifiedNodes, item_nodes, item_failedNodes, item_cancelFailedNodes, item_sweep, item_arguments, item_command :=
		"Id", "Name", "State", "Progress", "Create Time", "End Time", "Node Pattern", "Node Grouops", "Specified Nodes", "Nodes", "Failed Nodes", "Cancel Failed Nodes", "Sweep Parameter", "Arguments", "Command"
	maxLength := MaxInt(len(item_id), len(item_name), len(item_state), len(item_progress), len(item_createTime), len(item_endTime), len(item_sweep), len(item_nodePattern),
		len(item_nodeGroups), len(item_specifiedNodes), len(item_nodes), len(item_failedNodes), len(item_cancelFailedNodes), len(item_arguments), len(item_command))
	print := func(name string, value interface{}) {
		Printlnf("%-*v : %v", maxLength, name, value)
	}
	for _, job := range jobs {
		print(item_id, job.Id)
		print(item_name, job.Name)
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
		Printlnf(GetPaddingLine(""))
	}
	Printlnf("Job count: %v", len(jobs))
}

func getJobTableMaxLength(jobs []*pb.Job) (id, name, state, progress, create_time, end_time, command int) {
	create_time = len(fmt.Sprintf("%v", time.Unix(0, 0)))
	for _, job := range jobs {
		if length := len(strconv.Itoa(int(job.Id))); length > id {
			id = length
		}
		if length := len(job.Name); length > id {
			name = length
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
		job.Command = strings.NewReplacer("\r", `\r`, "\n", `\n`).Replace(job.Command)
		if length := len(job.Command); length > command {
			command = length
		}
	}
	return
}
