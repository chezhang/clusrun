package main

import (
	pb "../protobuf"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

const (
	heartbeat_expire_time = 5 * time.Second
)

var (
	// TODO: use a sync.Map from node to id and 2 arrays instead, only lock when appending
	reported_time   sync.Map
	validate_number sync.Map
)

type headnode_server struct {
	pb.UnimplementedHeadnodeServer
}

func (s *headnode_server) Heartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.Empty, error) {
	defer LogPanic()
	nodename, host := in.GetNodename(), in.GetHost()
	if strings.ContainsAny(nodename, "()") {
		log.Printf("Invalid nodename in heartbeat: %v", nodename)
		return &pb.Empty{}, errors.New("Invalid nodename: " + nodename)
	}
	hostname, port, host, err := ParseHostAddress(host)
	if err != nil {
		log.Printf("Invalid host format in heartbeat: %v", host)
		return &pb.Empty{}, errors.New("Invalid host format: " + host)
	}
	nodename = strings.ToUpper(nodename)
	var display_name string
	if hostname == nodename && port == default_port {
		display_name = nodename
	} else {
		display_name = nodename + "(" + host + ")"
	}
	if last_report, ok := reported_time.Load(display_name); !ok {
		log.Printf("First heartbeat from %v", display_name)
	} else if time.Since(last_report.(time.Time)) > heartbeat_expire_time {
		log.Printf("%v reconnected. Last report time: %v", display_name, last_report)
		validate_number.Delete(display_name)
	}
	reported_time.Store(display_name, time.Now())
	go Validate(display_name, nodename, host)
	return &pb.Empty{}, nil
}

func (s *headnode_server) GetNodes(ctx context.Context, in *pb.GetNodesRequest) (*pb.GetNodesReply, error) {
	defer LogPanic()
	pattern, state, _ := in.GetPattern(), in.GetState(), in.GetGroups()
	nodes := []*pb.GetNodesReply_Node{}
	reported_time.Range(func(key interface{}, val interface{}) bool {
		nodename := key.(string)
		if matched, _ := regexp.MatchString(pattern, nodename); !matched {
			return true
		}
		last_report := val.(time.Time)
		node := pb.GetNodesReply_Node{Name: nodename}
		if time.Since(last_report) > heartbeat_expire_time {
			node.State = pb.NodeState_Lost
		} else {
			if number, ok := validate_number.Load(nodename); ok && number.(int) < 0 {
				node.State = pb.NodeState_Ready
			} else {
				node.State = pb.NodeState_Error
			}
		}
		if state == pb.NodeState_Unknown || state == node.State {
			nodes = append(nodes, &node)
		}
		return true
	})
	log.Printf("GetNodes result:\n%v", nodes)
	return &pb.GetNodesReply{Nodes: nodes}, nil
}

func (s *headnode_server) GetJobs(ctx context.Context, in *pb.GetJobsRequest) (*pb.GetJobsReply, error) {
	defer LogPanic()
	job_ids := in.GetJobIds()
	loaded_jobs, err := LoadJobs()
	if err != nil {
		return nil, err
	}
	jobs := []*pb.Job{}
	for i := range loaded_jobs {
		if _, ok := job_ids[loaded_jobs[i].Id]; len(job_ids) == 0 || ok {
			jobs = append(jobs, &loaded_jobs[i])
		}
	}
	log.Printf("GetJobs result:\n%v", jobs)
	return &pb.GetJobsReply{Jobs: jobs}, nil
}

func (s *headnode_server) StartClusJob(in *pb.StartClusJobRequest, out pb.Headnode_StartClusJobServer) error {
	defer LogPanic()
	log.Println("Received create job request")
	command, nodes, pattern := in.GetCommand(), in.GetNodes(), in.GetPattern()

	// Get nodes
	nodes, invalid_nodes := GetValidNodes(nodes, pattern)
	if len(invalid_nodes) > 0 {
		log.Printf("Invalid nodes to create job: %v", invalid_nodes)
		return errors.New(fmt.Sprintf("Invalid nodes (%v): %v", len(invalid_nodes), invalid_nodes))
	}
	if len(nodes) == 0 {
		message := "No valid nodes to create job"
		log.Printf(message)
		return errors.New(message)
	}

	// Create job
	id, err := CreateNewJob(command, nodes)
	if err != nil {
		log.Printf("Failed to create job: %v", err)
		return err
	}
	if err := out.Send(&pb.StartClusJobReply{JobId: int32(id), Nodes: nodes}); err != nil {
		log.Printf("Failed to send job id of job %v to client: %v", id, err)
		return err
	}

	// Start job on nodes in the cluster
	UpdateJobState(id, pb.JobState_Created, pb.JobState_Dispatching)
	wg := sync.WaitGroup{}
	var job_on_nodes sync.Map
	for _, node := range nodes {
		wg.Add(1)
		go StartJobOnNode(id, command, node, &job_on_nodes, out, &wg)
	}

	// Wait for all jobs finish
	UpdateJobState(id, pb.JobState_Dispatching, pb.JobState_Running)
	wg.Wait()
	EndJob(id, pb.JobState_Running, pb.JobState_Finished) // TODO: Set state failed if job on any node failed
	return nil
}

func (s *headnode_server) CancelClusJobs(ctx context.Context, in *pb.CancelClusJobsRequest) (*pb.CancelClusJobsReply, error) {
	defer LogPanic()
	log.Println("Received cancel job request")
	job_ids := in.GetJobIds()
	db_jobs_lock.Lock()
	jobs, err := LoadJobs()
	if err != nil {
		return nil, err
	}
	if len(job_ids) == 0 && len(jobs) > 0 {
		job_ids = map[int32]bool{jobs[len(jobs)-1].Id: false}
	}
	result := map[int32]pb.JobState{}
	to_cancel := []int32{}
	for i := range jobs {
		if _, ok := job_ids[jobs[i].Id]; ok {
			if IsActiveState(jobs[i].State) {
				jobs[i].State = pb.JobState_Canceling
				to_cancel = append(to_cancel, jobs[i].Id)
			}
			result[jobs[i].Id] = jobs[i].State
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return nil, err
	}
	db_jobs_lock.Unlock()
	for i := range to_cancel {
		go CancelJob(to_cancel[i])
	}
	return &pb.CancelClusJobsReply{Result: result}, nil
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

		reply, err := c.Validate(ctx, &pb.ValidateRequest{Headnode: clusnode_host, Clusnode: host})
		name := strings.ToUpper(reply.GetNodename())
		if err != nil {
			log.Printf("Validation failed: %v", err)
			validate_number.Store(display_name, number+1)
		} else if name != nodename { // in case a clusnode uses a wrong host parameter
			log.Printf("Validation failed: expect nodename %v, replied nodename %v", nodename, name)
			validate_number.Store(display_name, 10)
		} else {
			log.Printf("Clusnode %v is validated that being hosted by %v", display_name, host)
			validate_number.Store(display_name, -1)
		}
	}
}

func GetValidNodes(nodes []string, pattern string) ([]string, []string) {
	ready_nodes := map[string]string{}
	valid_nodes := []string{}
	reported_time.Range(func(key interface{}, val interface{}) bool {
		node := key.(string)
		last_report := val.(time.Time)
		if number, ok := validate_number.Load(node); ok && number.(int) < 0 && time.Since(last_report) <= heartbeat_expire_time {
			if matched, _ := regexp.MatchString(pattern, node); !matched {
				return true
			}
			ready_nodes[node] = node
			ready_nodes[ParseHost(node)] = node
			valid_nodes = append(valid_nodes, node)
		}
		return true
	})
	invalid_nodes := []string{}
	if len(nodes) > 0 {
		valid_nodes = []string{}
		added := map[string]bool{}
		for _, node := range nodes {
			if valid_node, ok := ready_nodes[strings.ToUpper(node)]; ok {
				if _, ok := added[valid_node]; !ok {
					valid_nodes = append(valid_nodes, valid_node)
					added[valid_node] = true
				}
			} else {
				invalid_nodes = append(invalid_nodes, node)
			}
		}
	}
	return valid_nodes, invalid_nodes
}

func ParseHost(display_name string) string {
	segs := strings.Split(display_name, "(")
	if len(segs) <= 1 {
		return display_name + ":" + default_port
	} else {
		return segs[1][:len(segs[1])-1]
	}
}

func StartJobOnNode(id int, command, node string, job_on_nodes *sync.Map, out pb.Headnode_StartClusJobServer, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Start job %v on node %v", id, node)

	// Create file to save output
	stdout, stderr := GetOutputFile(id, node)
	var f_out, f_err *os.File
	var err error
	if f_out, err = os.Create(stdout); err == nil {
		f_err, err = os.Create(stderr)
	}
	if err != nil {
		log.Printf("Failed to create output file for job %v node %v: %v", id, node, err)
		return
	}
	defer f_out.Close()
	defer f_err.Close()
	job_on_nodes.Store(node, pb.JobState_Dispatching)

	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	conn, err := grpc.DialContext(ctx, ParseHost(node), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Can not connect node %v in %v: %v", node, connect_timeout, err)
		return
	}
	defer conn.Close()
	c := pb.NewClusnodeClient(conn)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Start job on clusnode
	stream, err := c.StartJob(ctx, &pb.StartJobRequest{JobId: int32(id), Command: command, Headnode: local_host})
	if err != nil {
		log.Printf("Failed to start job %v on node %v: %v", id, node, err)
		job_on_nodes.Store(node, pb.JobState_Failed)
	} else {
		job_on_nodes.Store(node, pb.JobState_Running)
	}

	// Save and redirect output
	exit_code := 0
	has_stderr := false
	failing_to_redirect := false
	for {
		output, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Job %v on node %v finished with exit code %v", id, node, exit_code)
			if err := out.Send(&pb.StartClusJobReply{Node: node, ExitCode: int32(exit_code)}); err != nil {
				log.Printf("Failed to redirect exit code of job %v on node %v: %v", id, node, err)
			}
			break
		}
		if err != nil {
			log.Printf("Failed to receive output of job %v on node %v: %v", id, node, err)
		} else {
			stdout, stderr := output.GetStdout(), output.GetStderr()
			if stdout != "" {
				if _, err := f_out.WriteString(stdout); err != nil {
					log.Printf("Failed to save stdout of job %v on node %v: %v", id, node, err)
				}
				if err := out.Send(&pb.StartClusJobReply{Node: node, Stdout: stdout}); err != nil {
					if !failing_to_redirect {
						log.Printf("Failed to redirect stdout of job %v on node %v: %v", id, node, err)
					}
					failing_to_redirect = true
				} else {
					failing_to_redirect = false
				}
			}
			if stderr != "" {
				has_stderr = true
				if _, err := f_err.WriteString(stderr); err != nil {
					log.Printf("Failed to save stderr of job %v on node %v: %v", id, node, err)
				}
				if err := out.Send(&pb.StartClusJobReply{Node: node, Stderr: stderr}); err != nil {
					if !failing_to_redirect {
						log.Printf("Failed to redirect stderr of job %v on node %v: %v", id, node, err)
					}
					failing_to_redirect = true
				} else {
					failing_to_redirect = false
				}
			}
			exit_code = int(output.GetExitCode())
		}
	}
	if !has_stderr {
		f_err.Close()
		os.Remove(stderr)
	}
	job_on_nodes.Store(node, pb.JobState_Finished)
}

func CancelJob(id int32) {
	jobs, err := LoadJobs()
	if err != nil {
		log.Printf("Failed to cancel job %v: %v", id, err)
		return
	}
	nodes := []string{}
	for i := range jobs {
		if jobs[i].Id == id && jobs[i].State == pb.JobState_Canceling {
			nodes = jobs[i].Nodes
			break
		}
	}
	wg := sync.WaitGroup{}
	result := sync.Map{}
	for i := range nodes {
		wg.Add(1)
		result.Store(nodes[i], false)
		go CancelJobOnNode(id, nodes[i], &wg, &result)
	}
	wg.Wait()
	cancel_failed_nodes := []string{}
	result.Range(func(node interface{}, canceled interface{}) bool {
		if !canceled.(bool) {
			cancel_failed_nodes = append(cancel_failed_nodes, node.(string))
		}
		return true
	})
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
	jobs, err = LoadJobs()
	for i := range jobs {
		if jobs[i].Id == id {
			if len(cancel_failed_nodes) == 0 {
				jobs[i].State = pb.JobState_Canceled
				log.Printf("Job %v is canceled", id)
			} else {
				jobs[i].State = pb.JobState_CancelFailed
				jobs[i].CancelFailedNodes = cancel_failed_nodes
				log.Printf("Cancellation of job %v failed for nodes: %v", id, cancel_failed_nodes)
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		log.Printf("Failed to save cancellation result of job %v: %v", id, err)
		return
	}
}

func CancelJobOnNode(id int32, node string, wg *sync.WaitGroup, result *sync.Map) {
	defer wg.Done()

	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	conn, err := grpc.DialContext(ctx, ParseHost(node), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Can not connect node %v in %v: %v", node, connect_timeout, err)
		return
	}
	defer conn.Close()
	c := pb.NewClusnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()

	// Cancel job on clusnode
	_, err = c.CancelJob(ctx, &pb.CancelJobRequest{JobId: id, Headnode: local_host})
	if err != nil {
		log.Printf("Failed to cancel job %v on node %v: %v", id, node, err)
	} else {
		result.Store(node, true)
	}
}
