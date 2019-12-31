package main

import (
	pb "../protobuf"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

const (
	heartbeat_interval = 1 * time.Second
	connect_timeout    = 30 * time.Second
	default_nodename   = "Unknown"
)

var (
	clusnode_name       string
	clusnode_host       string
	headnodes_reporting sync.Map
	jobs_cancellation   sync.Map
)

type clusnode_server struct {
	pb.UnimplementedClusnodeServer
}

func (s *clusnode_server) Validate(ctx context.Context, in *pb.ValidateRequest) (*pb.ValidateReply, error) {
	defer LogPanic()
	log.Printf("Received validation request from %v to %v", in.GetHeadnode(), in.GetClusnode())
	return &pb.ValidateReply{Nodename: clusnode_name}, nil
}

func (s *clusnode_server) SetHeadnodes(ctx context.Context, in *pb.SetHeadnodesRequest) (*pb.SetHeadnodesReply, error) {
	defer LogPanic()
	headnodes := in.GetHeadnodes()
	results := make(map[string]string)
	for _, headnode := range headnodes {
		result := "OK"
		if err := AddHeadnode(headnode); err != nil {
			result = err.Error()
		}
		results[headnode] = result
	}
	log.Printf("SetHeadnodes result: %v", results)
	SaveHeadnodes()
	return &pb.SetHeadnodesReply{Results: results}, nil
}

func (s *clusnode_server) GetHeadnodes(ctx context.Context, in *pb.Empty) (*pb.GetHeadnodesReply, error) {
	defer LogPanic()
	headnodes := make(map[string]bool)
	var err error = nil
	headnodes_reporting.Range(func(key, val interface{}) bool {
		headnodes[key.(string)] = val.(bool)
		return true
	})
	log.Printf("GetHeadnodes result: %v", headnodes)
	return &pb.GetHeadnodesReply{Headnodes: headnodes}, err
}

func (s *clusnode_server) StartJob(in *pb.StartJobRequest, out pb.Clusnode_StartJobServer) error {
	defer LogPanic()
	headnode, job_id, command := in.GetHeadnode(), in.GetJobId(), in.GetCommand()
	log.Printf("Receive StartJob from headnode %v to start job %v with command %v", headnode, job_id, command)
	job_label := GetJobLabel(headnode, int(job_id))

	// Create command file
	cmd_file, err := CreateCommandFile(job_label, command)
	if err != nil {
		message := "Failed to create command file"
		log.Printf(message+" for job %v", job_label)
		return errors.New(message)
	}

	// Run command
	start_point := "/bin/bash"
	arg := cmd_file
	if runtime.GOOS == "windows" {
		start_point = cmd_file
		arg = ""
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs_cancellation.Store(job_label, cancel)
	defer CleanupJob(job_label, cmd_file)
	cmd := exec.CommandContext(ctx, start_point, arg)
	var stdout, stderr io.Reader
	if stdout, err = cmd.StdoutPipe(); err == nil {
		if stderr, err = cmd.StderrPipe(); err == nil {
			err = cmd.Start()
		}
	}
	if err != nil {
		message := "Failed to create job"
		log.Printf("%v %v: %v", message, job_label, err)
		return errors.New(message)
	}

	// Send output
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				err = out.Send(&pb.StartJobReply{Stdout: string(buf[:n])})
			}
			if err != nil {
				log.Printf("Sending stdout of job %v finished: %v", job_label, err)
				break
			}
		}
		wg.Done()
	}()
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				err = out.Send(&pb.StartJobReply{Stderr: string(buf[:n])})
			}
			if err != nil {
				log.Printf("Sending stderr of job %v finished: %v", job_label, err)
				break
			}
		}
		wg.Done()
	}()

	// Send exit code
	exit_code := 0
	if err = cmd.Wait(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exit_code = exitError.ExitCode()
		}
	}
	log.Printf("Job %v finished with exit code %v", job_label, exit_code)
	wg.Wait()
	err = out.Send(&pb.StartJobReply{ExitCode: int32(exit_code)})
	if err != nil {
		log.Printf("Failed to send exitcode of job %v", job_label)
	}
	return err
}

func CleanupJob(job_label, cmd_file string) {
	jobs_cancellation.Delete(job_label)
	if err := os.Remove(cmd_file); err != nil {
		log.Printf("Failed to cleanup job %v: %v", job_label, err)
	}
}

func GetJobLabel(headnode string, job_id int) string {
	return strings.ReplaceAll(headnode, ":", ".") + "." + strconv.Itoa(job_id)
}

func CreateCommandFile(job_label, command string) (string, error) {
	file := filepath.Join(db_cmd_dir, job_label)
	if runtime.GOOS == "windows" {
		command = "@echo off\n" + command
		file += ".cmd"
	} else {
		file += ".sh"
	}
	log.Printf("Create file %v", file)
	if err := ioutil.WriteFile(file, []byte(command), 0644); err != nil {
		return file, err
	}
	return file, nil
}

func AddHeadnode(headnode string) error {
	hostname, port, err := ParseHostAddress(headnode)
	if err != nil {
		return errors.New("Failed to parse headnode host address: " + err.Error())
	}
	headnode = hostname + ":" + port
	if reporting, ok := headnodes_reporting.Load(headnode); ok && reporting.(bool) {
		return errors.New("Already reporting")
	}
	headnodes_reporting.Store(headnode, false)
	go HeartBeat(clusnode_host, headnode)
	return nil
}

func HeartBeat(from, to string) {
	for ok := true; ok; _, ok = headnodes_reporting.Load(to) {
		log.Printf("Start heartbeat from %v to %v", from, to)
		ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
		conn, err := grpc.DialContext(ctx, to, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Printf("Can not connect %v in %v: %v", to, connect_timeout, err)
		} else {
			c := pb.NewHeadnodeClient(conn)
			log.Printf("Connected to headnode %v", to)
			for ok := true; ok; _, ok = headnodes_reporting.Load(to) {
				ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
				_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{Nodename: clusnode_name, Host: from})
				if err != nil {
					log.Printf("Can not send heartbeat: %v", err)
					headnodes_reporting.Store(to, false)
					cancel()
					break
				}
				headnodes_reporting.Store(to, true)
				cancel()
				time.Sleep(heartbeat_interval)
			}
			conn.Close()
		}
		cancel()
	}
}
