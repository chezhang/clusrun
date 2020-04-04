package main

import (
	pb "../protobuf"
	"./platform"
	grpc "google.golang.org/grpc"

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
)

const (
	connect_timeout  = 30 * time.Second
	default_nodename = "Unknown"
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
	defer LogPanicBeforeExit()
	log.Printf("Received validation request from %v to %v", in.GetHeadnode(), in.GetClusnode())
	return &pb.ValidateReply{Nodename: clusnode_name}, nil
}

func (s *clusnode_server) SetHeadnodes(ctx context.Context, in *pb.SetHeadnodesRequest) (*pb.SetHeadnodesReply, error) {
	defer LogPanicBeforeExit()
	headnodes, mode := in.GetHeadnodes(), in.GetMode()
	results := make(map[string]string)
	if mode == pb.SetHeadnodesMode_Remove {
		for _, headnode := range headnodes {
			if headnode, err := RemoveHeadnode(headnode); err != nil {
				results[headnode] = err.Error()
			} else {
				results[headnode] = "Removed"
			}
		}
	} else {
		for _, headnode := range headnodes {
			if headnode, err := AddHeadnode(headnode); err != nil {
				results[headnode] = err.Error()
			} else {
				results[headnode] = "Added"
			}
		}
		if mode == pb.SetHeadnodesMode_Default {
			headnodes_reporting.Range(func(key, val interface{}) bool {
				if state := val.(*heartbeat_state); !state.Stopped {
					node := key.(string)
					if _, ok := results[node]; !ok {
						if headnode, err := RemoveHeadnode(node); err != nil {
							results[headnode] = err.Error()
						} else {
							results[headnode] = "Removed"
						}
					}
				}
				return true
			})
		}
	}
	log.Printf("SetHeadnodes results: %v", results)
	SaveNodeConfigs()
	return &pb.SetHeadnodesReply{Results: results}, nil
}

func (s *clusnode_server) StartJob(in *pb.StartJobRequest, out pb.Clusnode_StartJobServer) error {
	defer LogPanicBeforeExit()
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
	start_point := "/bin/bash"
	args := []string{cmd_file}
	if run_on_windows {
		start_point = "cmd"
		args = []string{"/q", "/c", cmd_file}
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
	send_output := func(reader io.Reader, t string) {
		buf := make([]byte, 1024)
		for {
			if n, err := reader.Read(buf); n > 0 {
				output := string(buf[:n])
				var reply pb.StartJobReply
				if t == "stdout" {
					reply.Stdout = output
				} else {
					reply.Stderr = output
				}
				if err := out.Send(&reply); err != nil {
					log.Printf("Failed to send %v to headnode: %v", t, err)
					break
				}
			} else {
				if err == io.EOF {
					log.Printf("Sending %v of job %v finished", t, job_label)
				} else if err != nil {
					log.Printf("Failed to get %v of command: %v", t, err)
				} else {
					log.Printf("Unexpected empty %v", t)
				}
				break
			}
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		send_output(stdout, "stdout")
		wg.Done()
	}()
	go func() {
		send_output(stderr, "stderr")
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
	defer LogPanicBeforeExit()
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

func (s *clusnode_server) SetConfigs(ctx context.Context, in *pb.SetConfigsRequest) (*pb.SetConfigsReply, error) {
	defer LogPanicBeforeExit()
	configs := in.GetConfigs()
	results := SetNodeConfigs(Config_Clusnode, configs)
	return &pb.SetConfigsReply{Results: results}, nil
}

func (s *clusnode_server) GetConfigs(ctx context.Context, in *pb.Empty) (*pb.GetConfigsReply, error) {
	defer LogPanicBeforeExit()
	results := GetNodeConfigs(Config_Clusnode)
	return &pb.GetConfigsReply{Configs: results}, nil
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

func AddHeadnode(headnode string) (added string, e error) {
	_, _, headnode, err := ParseHostAddress(headnode)
	if err != nil {
		e = errors.New("Failed to parse headnode host address: " + err.Error())
		return
	}
	if state, ok := headnodes_reporting.LoadOrStore(headnode, &heartbeat_state{Connected: false, Stopped: false}); ok {
		s := state.(*heartbeat_state)
		if s.Stopped {
			s.Stopped = false
			s.Connected = false
		} else {
			if s.Connected {
				e = errors.New("Already connected")
			} else {
				e = errors.New("Connecting")
			}
			return
		}
	} else {
		go Heartbeat(clusnode_host, headnode)
	}
	added = headnode
	return
}

func RemoveHeadnode(headnode string) (removed string, e error) {
	_, _, headnode, err := ParseHostAddress(headnode)
	if err != nil {
		e = errors.New("Failed to parse headnode host address: " + err.Error())
		return
	}
	if state, ok := headnodes_reporting.Load(headnode); ok {
		s := state.(*heartbeat_state)
		if s.Stopped {
			e = errors.New("Already removed")
		} else {
			state.(*heartbeat_state).Stopped = true
			removed = headnode
		}
	} else {
		e = errors.New("Invalid headnode")
	}
	return
}

func GetHeadnodes() (connected, connecting []string) {
	headnodes_reporting.Range(func(key, val interface{}) bool {
		if state := val.(*heartbeat_state); !state.Stopped {
			headnode := key.(string)
			if state.Connected {
				connected = append(connected, headnode)
			} else {
				connecting = append(connecting, headnode)
			}
		}
		return true
	})
	return
}

func Heartbeat(from, headnode string) {
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
		time.Sleep(time.Duration(Config_Clusnode_HeartbeatIntervalSecond.GetInt()) * time.Second)
	}
}
