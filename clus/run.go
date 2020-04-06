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

func Run(args []string) {
	fs := flag.NewFlagSet("clus run options", flag.ExitOnError)
	headnode := fs.String("headnode", LocalHost, "specify the headnode to connect")
	script := fs.String("script", "", "specify the script file containing commands to run")
	dump := fs.Bool("dump", false, "save the output to file")
	nodes := fs.String("nodes", "", "specify certain nodes to run the command")
	pattern := fs.String("pattern", "", "specify nodes matching a certain regular expression pattern to run the command")
	cache := fs.Int("cache", 1000, "specify the number of characters to cache and display for output of command on each node")
	immediate := fs.Int("immediate", 1, "specify the number of nodes, the output of which will be displayed immediately")
	serial := fs.String("serial", "", "replace specified string in the command on each node to a serial number starting from 0")
	// pick := fs.Int("pick", 0, "pick certain number of nodes to run, default 0 means pick all nodes")
	// merge := fs.Bool("merge", false, "specify if merge outputs with the same content for different nodes")
	fs.Parse(args)
	command := strings.Join(fs.Args(), " ")
	if len(*script) > 0 {
		if len(command) > 0 {
			fmt.Printf("[Warning] The command '%v' will be overwritten by scirpt %v\n", command, *script)
		}
		command = ParseScript(*script)
	} else if len(command) <= 0 {
		DisplayRunUsage(fs)
		return
	}
	output_dir := ""
	if *dump {
		output_dir = CreateOutputDir()
	}
	RunJob(ParseHeadnode(*headnode), command, *serial, output_dir, *pattern, ParseNodes(*nodes), *cache, *immediate)
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

func RunJob(headnode, command, serial, output_dir, pattern string, nodes []string, buffer_size, immediate int) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible.", headnode)
		return
	}
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	// Potential grpc bug:
	// 1. call cancel(): out.Send() on headnode get error code = Unavailable
	// 2. set ctx = context.WithTimeout(context.Background(), 1 * time.Second): out.Send() on headnode get error code = DeadlineExceeded
	// 3. set ctx = context.WithTimeout(context.Background(), 10 * time.Second): out.Send() on headnode get error code = Canceled

	// Start job
	stream, err := c.StartClusJob(ctx, &pb.StartClusJobRequest{Command: command, Serial: serial, Pattern: pattern, Nodes: nodes})
	if err != nil {
		fmt.Println("Failed to start job:", err)
		return
	}
	var finished_nodes, failed_nodes, all_nodes []string
	var job_id int32
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
	if immediate < 0 {
		immediate = 0
	} else if immediate > len(all_nodes) {
		immediate = len(all_nodes)
	}
	immediate_nodes := make(map[string]bool, immediate)
	for i := 0; i < immediate; i++ {
		immediate_nodes[all_nodes[i]] = true
	}

	// Initialize output cache
	cache := make(map[string][]rune, len(all_nodes))

	// Handle SIGINT
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		Summary(cache, finished_nodes, failed_nodes, all_nodes, buffer_size, job_time)
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
				exit_code := output.GetExitCode()
				if exit_code != 0 {
					state = fmt.Sprintf("failed with exit code %v", exit_code)
					failed_nodes = append(failed_nodes, node)
				}
				duration := time.Now().Sub(start_time)
				job_time = append(job_time, duration)
				fmt.Printf("[%v/%v] Command %v on node %v in %v.\n", len(finished_nodes), len(all_nodes), state, node, duration)
			} else {
				if buffer_size > 0 {
					// TODO: Consider changing the stdout/stderr type in stream from string to []rune to improve performance
					cache[node] = append(cache[node], []rune(content)...) // Buffer output
					// Use []rune instead of string/[]byte to prevent an unicode character from being splited when truncating the cache
					if over_size := len(cache[node]) - (buffer_size + 1); over_size > 0 {
						cache[node] = cache[node][over_size:]
					}
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
	Summary(cache, finished_nodes, failed_nodes, all_nodes, buffer_size, job_time)
}

func Summary(cache map[string][]rune, finished_nodes, failed_nodes, all_nodes []string, buffer_size int, job_time []time.Duration) {
	if buffer_size > 0 {
		fmt.Println()
		nodes := make([]string, 0, len(cache))
		for node := range cache {
			nodes = append(nodes, node)
		}
		sort.Strings(nodes)
		for _, node := range nodes {
			output := cache[node]
			heading := fmt.Sprintf("---[%v]---", node)
			fmt.Println(GetPaddingLine(heading))
			if over_size := len(output) - buffer_size; over_size > 0 {
				output = output[over_size:]
				fmt.Printf("(Truncated)\n...")
			}
			fmt.Println(string(output))
		}
	}
	min, max, mean, mid, std_dev := GetTimeStat(job_time)
	fmt.Println(GetPaddingLine(""))
	runtime := fmt.Sprintf("Runtime: Min=%v, Max=%v, Mean=%v, Mid=%v, SD=%v", min, max, mean, mid, std_dev)
	if len(runtime) <= ConsoleWidth {
		fmt.Println(runtime)
	} else {
		fmt.Println(strings.ReplaceAll(strings.ReplaceAll(runtime, " ", "\n"), ",", ""))
		fmt.Println()
	}
	fmt.Printf("%v of %v node(s) succeeded.\n", len(finished_nodes)-len(failed_nodes), len(all_nodes))
	if len(failed_nodes) > 0 {
		sort.Strings(failed_nodes)
		fmt.Printf("Failed node(s) (%v/%v): %v\n", len(failed_nodes), len(all_nodes), strings.Join(failed_nodes, ", "))
	}
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
