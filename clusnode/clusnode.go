package main

import (
	pb "../protobuf"
	"./platform"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
	jobs_pid            sync.Map
)

type heartbeat_state struct {
	Connected bool
	Stopped   bool
}

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
	var err error
	for _, headnode := range headnodes {
		result := "Added"
		if _, _, headnode, err = ParseHostAddress(headnode); err == nil {
			err = AddHeadnode(headnode)
		}
		if err != nil {
			result = err.Error()
		}
		results[headnode] = result
	}
	headnodes_reporting.Range(func(key, val interface{}) bool {
		node := key.(string)
		if _, ok := results[node]; !ok {
			result := "Removed"
			if err := RemoveHeadnode(node); err != nil {
				result = err.Error()
			}
			results[node] = result
		}
		return true
	})
	log.Printf("SetHeadnodes result: %v", results)
	SaveHeadnodes()
	return &pb.SetHeadnodesReply{Results: results}, nil
}

func (s *clusnode_server) GetHeadnodes(ctx context.Context, in *pb.Empty) (*pb.GetHeadnodesReply, error) {
	defer LogPanic()
	headnodes := make(map[string]bool)
	var err error = nil
	headnodes_reporting.Range(func(key, val interface{}) bool {
		if state := val.(*heartbeat_state); !state.Stopped {
			headnodes[key.(string)] = state.Connected
		}
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
	defer CleanupJob(job_label, cmd_file)

	// Run command
	start_point := cmd_file
	args := []string{}
	if !run_on_windows {
		start_point = "/bin/bash"
		args = append(args, cmd_file)
	}
	cmd := exec.Command(start_point, args...)
	platform.SetSysProcAttr(cmd)
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
	jobs_pid.Store(job_label, cmd.Process.Pid)

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

func (s *clusnode_server) CancelJob(ctx context.Context, in *pb.CancelJobRequest) (*pb.Empty, error) {
	defer LogPanic()
	headnode, job_id := in.GetHeadnode(), in.GetJobId()
	log.Printf("Receive CancelJob from headnode %v to cancel job %v", headnode, job_id)
	job_label := GetJobLabel(headnode, int(job_id))
	if pid, ok := jobs_pid.Load(job_label); ok {
		pid := pid.(int)
		if run_on_windows {
			cmd := []string{"TASKKILL", "/T", "/F", "/PID", strconv.Itoa(pid)}
			log.Printf("Cancel job %v with command: %v", job_label, strings.Join(cmd, " "))
			output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
			log.Printf("Cancel job %v result: %s", job_label, output)
		} else {
			log.Printf("Cancel job %v by killing process group of process %v", job_label, pid)
			platform.KillProcessGroup(pid)
		}
	} else {
		log.Printf("Job %v is not running", job_label)
	}
	return &pb.Empty{}, nil
}

func CleanupJob(job_label, cmd_file string) {
	jobs_pid.Delete(job_label)
	if err := os.Remove(cmd_file); err != nil {
		log.Printf("Failed to cleanup job %v: %v", job_label, err)
	}
}

func GetJobLabel(headnode string, job_id int) string {
	return strings.ReplaceAll(headnode, ":", ".") + "." + strconv.Itoa(job_id)
}

func CreateCommandFile(job_label, command string) (string, error) {
	file := filepath.Join(db_cmd_dir, job_label)
	if run_on_windows {
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
	_, _, headnode, err := ParseHostAddress(headnode)
	if err != nil {
		return errors.New("Failed to parse headnode host address: " + err.Error())
	}
	if state, ok := headnodes_reporting.LoadOrStore(headnode, &heartbeat_state{Connected: false, Stopped: false}); ok {
		s := state.(*heartbeat_state)
		if s.Stopped {
			s.Stopped = false
			s.Connected = false
		} else {
			if s.Connected {
				return errors.New("Already connected")
			} else {
				return errors.New("Connecting")
			}
		}
	} else {
		go HeartBeat(clusnode_host, headnode)
	}
	return nil
}

func RemoveHeadnode(headnode string) error {
	_, _, headnode, err := ParseHostAddress(headnode)
	if err != nil {
		return errors.New("Failed to parse headnode host address: " + err.Error())
	}
	if state, ok := headnodes_reporting.Load(headnode); ok {
		state.(*heartbeat_state).Stopped = true
	} else {
		return errors.New("Invalid headnode")
	}
	return nil
}

func HeartBeat(from, headnode string) {
	connected := false
	stopped := true
	for {
		// Known data race of heartbeat_state when adding or removing headnode
		if state, ok := headnodes_reporting.Load(headnode); ok && !state.(*heartbeat_state).Stopped {
			if stopped {
				log.Printf("Start heartbeat from %v to %v", from, headnode)
				stopped = false
			}
			ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
			conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Printf("Can not connect %v in %v: %v", headnode, connect_timeout, err)
				connected = false
			} else {
				if !connected {
					log.Printf("Connected to headnode %v", headnode)
					connected = true
				}
				c := pb.NewHeadnodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
				_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{Nodename: clusnode_name, Host: from})
				if err != nil {
					log.Printf("Can not send heartbeat: %v", err)
					connected = false
				}
				cancel()
				conn.Close()
			}
			state.(*heartbeat_state).Connected = connected
			cancel()
		} else if !stopped {
			log.Printf("Stop heartbeat from %v to %v", from, headnode)
			stopped = true
		}
		time.Sleep(heartbeat_interval)
	}
}
