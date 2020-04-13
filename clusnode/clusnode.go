package main

import (
	"clusrun/clusnode/platform"
	pb "clusrun/protobuf"
	grpc "google.golang.org/grpc"

	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ConnectTimeout = 30 * time.Second
)

var (
	headnodesReporting sync.Map
	jobsPid            sync.Map
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
	LogInfo("Received validation request from %v to %v", in.GetHeadnode(), in.GetClusnode())
	return &pb.ValidateReply{Nodename: NodeName}, nil
}

func (s *clusnode_server) SetHeadnodes(ctx context.Context, in *pb.SetHeadnodesRequest) (*pb.SetHeadnodesReply, error) {
	defer LogPanicBeforeExit()
	headnodes, mode := in.GetHeadnodes(), in.GetMode()
	results := make(map[string]string)
	if mode == pb.SetHeadnodesMode_Remove {
		for _, headnode := range headnodes {
			if headnode, err := removeHeadnode(headnode); err != nil {
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
			headnodesReporting.Range(func(key, val interface{}) bool {
				if state := val.(*heartbeat_state); !state.Stopped {
					node := key.(string)
					if _, ok := results[node]; !ok {
						if headnode, err := removeHeadnode(node); err != nil {
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
	LogInfo("SetHeadnodes results: %v", results)
	SaveNodeConfigs()
	return &pb.SetHeadnodesReply{Results: results}, nil
}

func (s *clusnode_server) StartJob(in *pb.StartJobRequest, out pb.Clusnode_StartJobServer) error {
	defer LogPanicBeforeExit()
	headnode, job_id, command := in.GetHeadnode(), in.GetJobId(), in.GetCommand()
	LogInfo("Receive StartJob from headnode %v to start job %v with command: %v", headnode, job_id, command)
	job_label := getJobLabel(headnode, int(job_id))

	// Create command file
	cmd_file, err := CreateCommandFile(job_label, command)
	if err != nil {
		message := "Failed to create command file"
		LogError(message+" for job %v", job_label)
		return errors.New(message)
	}
	defer cleanupJob(job_label, cmd_file)

	// Run command
	start_point := "/bin/bash"
	args := []string{cmd_file}
	if RunOnWindows {
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
		LogError("%v %v: %v", message, job_label, err)
		return errors.New(message)
	}
	jobsPid.Store(job_label, cmd.Process.Pid)

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
					LogError("Failed to send %v to headnode: %v", t, err)
					break
				}
			} else {
				if err == io.EOF {
					LogInfo("Sending %v of job %v finished", t, job_label)
				} else if err != nil {
					LogError("Failed to get %v of command: %v", t, err)
				} else {
					LogError("Unexpected empty %v", t)
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
	LogInfo("Job %v finished with exit code %v", job_label, exit_code)
	wg.Wait()
	err = out.Send(&pb.StartJobReply{ExitCode: int32(exit_code)})
	if err != nil {
		LogError("Failed to send exitcode of job %v", job_label)
	}
	return err
}

func (s *clusnode_server) CancelJob(ctx context.Context, in *pb.CancelJobRequest) (*pb.Empty, error) {
	defer LogPanicBeforeExit()
	headnode, job_id := in.GetHeadnode(), in.GetJobId()
	LogInfo("Receive CancelJob from headnode %v to cancel job %v", headnode, job_id)
	job_label := getJobLabel(headnode, int(job_id))
	if pid, ok := jobsPid.Load(job_label); ok {
		pid := pid.(int)
		if RunOnWindows {
			cmd := []string{"TASKKILL", "/T", "/F", "/PID", strconv.Itoa(pid)}
			LogInfo("Cancel job %v with command: %v", job_label, strings.Join(cmd, " "))
			output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
			LogInfo("Cancel job %v result: %s", job_label, output)
		} else {
			LogInfo("Cancel job %v by killing process group of process %v", job_label, pid)
			platform.KillProcessGroup(pid)
		}
	} else {
		LogWarning("Job %v is not running", job_label)
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

func cleanupJob(job_label, cmd_file string) {
	jobsPid.Delete(job_label)
	if err := os.Remove(cmd_file); err != nil {
		LogError("Failed to cleanup job %v: %v", job_label, err)
	}
}

func getJobLabel(headnode string, job_id int) string {
	return FileNameFormatHost(headnode) + "." + strconv.Itoa(job_id)
}

func AddHeadnode(headnode string) (string, error) {
	_, _, host, err := ParseHostAddress(headnode)
	if err != nil {
		return headnode, errors.New("Failed to parse headnode host address: " + err.Error())
	}
	if state, ok := headnodesReporting.LoadOrStore(host, &heartbeat_state{Connected: false, Stopped: false}); ok {
		s := state.(*heartbeat_state)
		if s.Stopped {
			s.Stopped = false
			s.Connected = false
		} else {
			if s.Connected {
				return host, errors.New("Already connected")
			} else {
				return host, errors.New("Connecting")
			}
		}
	} else {
		go heartbeat(NodeHost, host)
	}
	return host, nil
}

func removeHeadnode(headnode string) (removed string, e error) {
	_, _, host, err := ParseHostAddress(headnode)
	if err != nil {
		e = errors.New("Failed to parse headnode host address: " + err.Error())
		return
	}
	if state, ok := headnodesReporting.Load(host); ok {
		s := state.(*heartbeat_state)
		if s.Stopped {
			e = errors.New("Already removed")
		} else {
			state.(*heartbeat_state).Stopped = true
			removed = host
		}
	} else {
		e = errors.New("Invalid headnode")
	}
	return
}

func GetHeadnodes() (connected, connecting []string) {
	headnodesReporting.Range(func(key, val interface{}) bool {
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

func heartbeat(from, headnode string) {
	connected := false
	stopped := true
	for {
		// Known data race of heartbeat_state when adding or removing headnode
		if state, ok := headnodesReporting.Load(headnode); ok && !state.(*heartbeat_state).Stopped {
			if stopped {
				LogInfo("Start heartbeat from %v to %v", from, headnode)
				stopped = false
			}
			ctx, cancelConn := context.WithTimeout(context.Background(), ConnectTimeout)
			conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				LogError("Can not connect %v in %v: %v", headnode, ConnectTimeout, err)
				connected = false
			} else {
				if !connected {
					LogInfo("Connected to headnode %v", headnode)
					connected = true
				}
				c := pb.NewHeadnodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
				_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{Nodename: NodeName, Host: from})
				if err != nil {
					LogError("Can not send heartbeat: %v", err)
					connected = false
				}
				cancel()
				conn.Close()
			}
			state.(*heartbeat_state).Connected = connected
			cancelConn()
		} else if !stopped {
			LogInfo("Stop heartbeat from %v to %v", from, headnode)
			stopped = true
		}
		time.Sleep(time.Duration(Config_Clusnode_HeartbeatIntervalSecond.GetInt()) * time.Second)
	}
}
