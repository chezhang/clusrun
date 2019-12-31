package main

import (
	pb "../protobuf"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	min_buffer_size = 30
)

func Run(args []string) {
	fs := flag.NewFlagSet("clus run options", flag.ExitOnError)
	headnode := fs.String("headnode", local_host, "specify the headnode to connect")
	script := fs.String("script", "", "specify the script file containing commands to run")
	dump := fs.Bool("dump", false, "save the output to file")
	nodes := fs.String("nodes", "", "specify certain nodes to run the command")
	pattern := fs.String("pattern", "", "specify nodes matching a certain regular expression pattern to run the command")
	buffer := fs.Int("buffer", 1000, "Specify the size of buffer to store the output of command on each node")
	fs.Parse(args)
	command := strings.Join(fs.Args(), " ")
	if len(*script) > 0 {
		if len(command) > 0 {
			fmt.Printf("[Warning]: The command '%v' will be overwritten by scirpt %v\n", command, *script)
		}
		command = ParseScript(*script)
	} else if len(command) == 0 {
		DisplayRunUsage(fs)
		return
	}
	output_dir := ""
	if *dump {
		output_dir = CreateOutputDir()
	}
	RunJob(ParseHeadnode(*headnode), command, output_dir, *pattern, ParseNodes(*nodes), *buffer)
}

func DisplayRunUsage(fs *flag.FlagSet) {
	fmt.Printf(`
Usage: 
  clus run [options] <command>

Options:
`)
	fs.PrintDefaults()
}

func ParseScript(script string) string {
	command, err := ioutil.ReadFile(script)
	if err != nil {
		fmt.Printf("Failed to read commands in script: %v", err)
		os.Exit(0)
	}
	return string(command)
}

func ParseNodes(s string) []string {
	nodes := []string{}
	for _, node := range strings.Split(s, ",") {
		if len(node) > 0 {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func CreateOutputDir() string {
	cur_dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get working dir: %v", err)
		os.Exit(0)
	}
	output_dir := filepath.Join(cur_dir, "clus.run."+time.Now().Format("20060102150405"))
	if err := os.MkdirAll(output_dir, 0644); err != nil {
		fmt.Printf("Failed to create output dir: %v", err)
		os.Exit(0)
	}
	return output_dir
}

func RunJob(headnode, command, output_dir, pattern string, nodes []string, buffer_size int) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible", headnode)
		return
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()

	// Start job
	stream, err := c.StartClusJob(ctx, &pb.StartClusJobRequest{Command: command, Pattern: pattern, Nodes: nodes})
	if err != nil {
		fmt.Println("Failed to start job:", err)
		return
	}
	var all_nodes []string
	if output, err := stream.Recv(); err != nil {
		fmt.Println("Failed to start job:", err)
		return
	} else {
		all_nodes = output.GetNodes()
		fmt.Printf("Job %v started on %v nodes in cluster %v\n\n", output.GetJobId(), len(all_nodes), headnode)
	}

	// Create output file
	var f_stdout, f_stderr map[string]*os.File
	if len(output_dir) > 0 {
		f_stdout = make(map[string]*os.File, len(all_nodes))
		f_stderr = make(map[string]*os.File, len(all_nodes))
		for _, node := range all_nodes {
			file := filepath.Join(output_dir, strings.ReplaceAll(node, ":", "."))
			stdout := file + ".out"
			stderr := file + ".err"
			if f_stdout[node], err = os.Create(stdout); err == nil {
				f_stderr[node], err = os.Create(stderr)
			}
			if err != nil {
				fmt.Printf("Failed to create output file: %v", err)
				return
			}
			defer f_stdout[node].Close()
			defer f_stderr[node].Close()
		}
	}

	// Receive output
	var finished_nodes, failed_nodes []string
	if buffer_size < min_buffer_size {
		buffer_size = min_buffer_size
	}
	truncate_msg := "(Truncated)\n..."
	buffer := make(map[string][]rune, len(all_nodes))
	for {
		output, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Failed to receive output")
			time.Sleep(time.Second)
		} else {
			node := output.GetNode()
			stdout, stderr := output.GetStdout(), output.GetStderr()
			content := stdout + stderr
			if len(content) == 0 { // EOF
				state := "Finished"
				finished_nodes = append(finished_nodes, node)
				if output.GetExitCode() != 0 {
					state = "Failed"
					failed_nodes = append(failed_nodes, node)
				}
				fmt.Printf("[%v/%v] Command %v on node %v\n\n", len(finished_nodes), len(all_nodes), state, node)
			} else {
				buffer[node] = append(buffer[node], []rune(content)...)
				if over_size := len(buffer[node]) - buffer_size - len(truncate_msg); over_size > 0 {
					buffer[node] = buffer[node][over_size:]
				}
				content = strings.TrimSpace(content)
				if len(content) > 0 { // Print
					fmt.Printf("[%v]: %v\n", node, content)
				}
			}
			if len(output_dir) > 0 { // Save to file
				if _, err = f_stdout[node].WriteString(stdout); err == nil {
					_, err = f_stderr[node].WriteString(stderr)
				}
				if err != nil {
					fmt.Printf("Failed to write file: %v", err)
					return
				}
			}
		}
	}

	// Summary
	fmt.Println("\n-----------Summary-----------")
	for node, output := range buffer {
		if len(output) > buffer_size {
			for i, c := range truncate_msg {
				buffer[node][i] = c
			}
		}
		fmt.Printf("[%v]:\n", node)
		fmt.Println(string(output))
		fmt.Println("-----------------------------")
	}
	if len(failed_nodes) > 0 {
		fmt.Printf("Failed nodes (%v/%v): %v", len(failed_nodes), len(all_nodes), failed_nodes)
	}
	if lost_nodes := len(all_nodes) - len(finished_nodes); lost_nodes > 0 {
		fmt.Printf("Lost %v nodes", lost_nodes)
	}
}
