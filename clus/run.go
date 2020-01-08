package main

import (
	pb "../protobuf"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	min_buffer_size = 30
	line_length     = 60
)

func Run(args []string) {
	fs := flag.NewFlagSet("clus run options", flag.ExitOnError)
	headnode := fs.String("headnode", local_host, "specify the headnode to connect")
	script := fs.String("script", "", "specify the script file containing commands to run")
	dump := fs.Bool("dump", false, "save the output to file")
	nodes := fs.String("nodes", "", "specify certain nodes to run the command")
	pattern := fs.String("pattern", "", "specify nodes matching a certain regular expression pattern to run the command")
	buffer := fs.Int("buffer", 1000, "specify the size of buffer to store the output of command on each node")
	meantime := fs.Int("meantime", 1, "specify the count of nodes, the output of which will be displayed in the meantime of command running")
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
	RunJob(ParseHeadnode(*headnode), command, output_dir, *pattern, ParseNodes(*nodes), *buffer, *meantime)
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

func RunJob(headnode, command, output_dir, pattern string, nodes []string, buffer_size, meantime int) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), connect_timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible.", headnode)
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
	var finished_nodes, failed_nodes, all_nodes []string
	var job_id uint32
	start_time := time.Now()
	job_time := make([]time.Duration, 0, len(all_nodes))
	if output, err := stream.Recv(); err != nil {
		fmt.Println("Failed to start job:", err)
		return
	} else {
		all_nodes = output.GetNodes()
		job_id = output.GetJobId()
		fmt.Printf("Job %v started on %v node(s) in cluster %v.\n", job_id, len(all_nodes), headnode)
		fmt.Println()
		fmt.Println(GetPaddingLine("---Command---"))
		fmt.Println(command)
		fmt.Println(GetPaddingLine(""))
		fmt.Println()
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
				fmt.Printf("Failed to create output file: %v\n", err)
				return
			}
			defer f_stdout[node].Close()
			defer f_stderr[node].Close()
		}
	}

	// Pick nodes whose output will be displayed immediately
	if meantime < 0 {
		meantime = 0
	} else if meantime > len(all_nodes) {
		meantime = len(all_nodes)
	}
	immediate_nodes := make(map[string]bool, meantime)
	for i := 0; i < meantime; i++ {
		immediate_nodes[all_nodes[i]] = true
	}

	// Initialize output buffer
	if buffer_size < min_buffer_size {
		buffer_size = min_buffer_size
	}
	buffer := make(map[string][]rune, len(all_nodes))

	// Handle SIGINT
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		Summary(buffer, finished_nodes, failed_nodes, all_nodes, buffer_size, job_time)
		if len(all_nodes) > len(finished_nodes) {
			fmt.Printf("Job %v is still running.\n", job_id)
		}
		os.Exit(0)
	}()

	// Receive output
	for {
		output, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Failed to receive output.")
			time.Sleep(time.Second)
		} else {
			node := output.GetNode()
			stdout, stderr := output.GetStdout(), output.GetStderr()
			content := stdout + stderr
			if len(content) == 0 { // EOF
				state := "finished"
				finished_nodes = append(finished_nodes, node)
				if output.GetExitCode() != 0 {
					state = "failed"
					failed_nodes = append(failed_nodes, node)
				}
				duration := time.Now().Sub(start_time)
				job_time = append(job_time, duration)
				fmt.Printf("[%v/%v] Command %v on node %v in %v.\n", len(finished_nodes), len(all_nodes), state, node, duration)
			} else {
				buffer[node] = append(buffer[node], []rune(content)...) // Buffer output
				if over_size := len(buffer[node]) - buffer_size - 1; over_size > 0 {
					buffer[node] = buffer[node][over_size:]
				}
				content = strings.TrimSpace(content)
				if _, ok := immediate_nodes[node]; ok && len(content) > 0 { // Print immediately
					fmt.Printf("[%v]: %v\n", node, content)
				}
			}
			if len(output_dir) > 0 { // Save to file
				if _, err = f_stdout[node].WriteString(stdout); err == nil {
					_, err = f_stderr[node].WriteString(stderr)
				}
				if err != nil {
					fmt.Printf("Failed to write file: %v\n", err)
					return
				}
			}
		}
	}
	Summary(buffer, finished_nodes, failed_nodes, all_nodes, buffer_size, job_time)
}

func Summary(buffer map[string][]rune, finished_nodes, failed_nodes, all_nodes []string, buffer_size int, job_time []time.Duration) {
	fmt.Println()
	nodes := make([]string, 0, len(buffer))
	for node := range buffer {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		output := buffer[node]
		heading := fmt.Sprintf("---[%v]---", node)
		fmt.Println(GetPaddingLine(heading))
		if over_size := len(output) - buffer_size; over_size > 0 {
			output = output[over_size:]
			fmt.Printf("(Truncated)\n...")
		}
		fmt.Println(string(output))
	}
	min, max, mean, mid, std_dev := GetTimeStat(job_time)
	fmt.Println(GetPaddingLine(""))
	fmt.Printf("Runtime: Min=%v, Max=%v, Mean=%v, Mid=%v, SD=%v\n", min, max, mean, mid, std_dev)
	fmt.Printf("%v of %v node(s) succeeded.\n", len(finished_nodes)-len(failed_nodes), len(all_nodes))
	if len(failed_nodes) > 0 {
		sort.Strings(failed_nodes)
		fmt.Printf("Failed node(s) (%v/%v): %v\n", len(failed_nodes), len(all_nodes), strings.Join(failed_nodes, ", "))
	}
}

func GetPaddingLine(heading string) string {
	if padding_length := line_length - len(heading); padding_length > 0 {
		padding := strings.Repeat("-", padding_length/2)
		heading = fmt.Sprintf("%v%v%v", padding, heading, padding)
	}
	return heading
}

func GetTimeStat(data []time.Duration) (min, max, mean, mid, std_dev time.Duration) {
	n := len(data)
	if n == 0 {
		return
	}
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
	min, max, mid = data[0], data[n-1], data[n/2]
	if n%2 == 0 {
		mid = (mid + data[n/2-1]) / 2
	}
	var sum int64
	for _, i := range data {
		sum += i.Nanoseconds()
	}
	temp_mean := float64(sum) / float64(n)
	mean = time.Duration(temp_mean)
	var sum_squares float64
	for _, i := range data {
		sum_squares += math.Pow(temp_mean-float64(i), 2)
	}
	std_dev = time.Duration(math.Sqrt(sum_squares / float64(n)))
	return
}
