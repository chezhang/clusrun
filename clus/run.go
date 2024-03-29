package main

import (
	pb "clusrun/protobuf"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

func Run(args []string) {
	fs := flag.NewFlagSet("clus run options", flag.ExitOnError)
	SetGlobalParameters(fs)
	script := fs.String("script", "", "specify the script file containing commands to run")
	// files := fs.String("files", "", "specify the files or directories, which will be copied to the working directory on each node")
	dump := fs.Bool("dump", false, "save the output to file")
	nodes := fs.String("nodes", "", "specify certain nodes to run the command")
	nodes_in_file := fs.String("nodes-in-file", "", "specify a file containg the nodes to run the command")
	pattern := fs.String("pattern", "", "specify nodes matching a certain regular expression pattern to run the command")
	groups := fs.String("groups", "", "specify certain node groups to run the command")
	groups_in_file := fs.String("groups-in-file", "", "specify a file containg the node groups to run the command")
	groups_intersect := fs.Bool("intersect", false, "specify to run the command in intersection (union if not specified) of node groups")
	cache := fs.Int("cache", 1000, "specify the number of characters to cache and display for output of command on each node")
	prompt := fs.Int("prompt", 1, "specify the number of nodes, the output of which will be displayed promptly")
	sweep := fs.String("sweep", "", `perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][:step]}]"`)
	background := fs.Bool("background", false, "run command without printing output")
	name := fs.String("name", "", "specify the job name")
	powershell := fs.Bool("powershell", false, "wrap the command with PowerShell")
	// pick := fs.Int("pick", 0, "pick certain number of nodes to run, default 0 means pick all nodes")
	// merge := fs.Bool("merge", false, "specify if merge outputs with the same content for different nodes")
	_ = fs.Parse(args)
	command := strings.Join(fs.Args(), " ")
	var arguments []string
	if len(*script) > 0 {
		command = ReadFile(*script)
		arguments = fs.Args()
	} else if len(command) <= 0 {
		displayRunUsage(fs)
		return
	}
	output_dir := ""
	if *dump {
		output_dir = createOutputDir()
	}
	RunJob(command, *sweep, output_dir, *pattern, *name, ParseNodesOrGroups(*groups, *groups_in_file), ParseNodesOrGroups(*nodes, *nodes_in_file), arguments, *cache, *prompt, *background, *groups_intersect, *powershell)
}

func displayRunUsage(fs *flag.FlagSet) {
	Printlnf(`
Usage: 
  clus run [options] <command>

Options:
`)
	fs.PrintDefaults()
}

func createOutputDir() string {
	cur_dir, err := os.Getwd()
	if err != nil {
		Fatallnf("Failed to get working dir: %v", err)
	}
	output_dir := filepath.Join(cur_dir, "clus.run."+time.Now().Format("20060102150405.000000000"))
	if err := os.MkdirAll(output_dir, 0644); err != nil {
		Fatallnf("Failed to create output dir: %v", err)
	}
	return output_dir
}

func RunJob(command, sweep, output_dir, pattern, name string, groups, nodes, arguments []string, cache_size, prompt int, background, intersect, powershell bool) {
	dump := len(output_dir) > 0
	if powershell {
		command = fmt.Sprintf("PowerShell -ExecutionPolicy ByPass -Command \"%v\"", command)
	}

	// Setup connection
	conn, cancel := ConnectHeadnode()
	defer cancel()
	defer conn.Close()
	c := pb.NewHeadnodeClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Potential grpc bug:
	// 1. call cancel(): out.Send() on headnode get error code = Unavailable
	// 2. set ctx = context.WithTimeout(context.Background(), 1 * time.Second): out.Send() on headnode get error code = DeadlineExceeded
	// 3. set ctx = context.WithTimeout(context.Background(), 10 * time.Second): out.Send() on headnode get error code = Canceled

	// Start job
	stream, err := c.StartClusJob(ctx, &pb.StartClusJobRequest{Command: command, Arguments: arguments, Sweep: sweep, Pattern: pattern, Groups: groups, GroupsIntersect: intersect, Nodes: nodes, Name: name}, grpc.UseCompressor("gzip"))
	if err != nil {
		Fatallnf("Failed to start job:", err)
	}
	var finished_nodes, failed_nodes, all_nodes []string
	var job_id int32
	start_time := time.Now()
	job_time := make([]time.Duration, 0, len(all_nodes))
	if output, err := stream.Recv(); err != nil {
		Fatallnf(status.Convert(err).Message())
	} else {
		all_nodes = output.GetNodes()
		job_id = output.GetJobId()
		job := fmt.Sprintf("%v", job_id)
		if len(name) > 0 {
			job += fmt.Sprintf(" %q", name)
		}
		Printlnf("Job %v started on %v nodes in cluster %q.", job, len(all_nodes), *Headnode)
		if dump {
			Printlnf("Dumping output to %v", output_dir)
		} else if background {
			return
		}
		if !background {
			Printlnf("")
			if len(sweep) > 0 {
				Printlnf("Sweep parameter: %v", sweep)
			}
			if len(arguments) > 0 {
				Printlnf("Arguments: %q", arguments)
			}
			Printlnf(GetPaddingLine("---Command---"))
			Printlnf(command)
			Printlnf(GetPaddingLine(""))
			Printlnf("")
		}
	}

	// Create output file
	var f_stdout, f_stderr map[string]*os.File
	if dump {
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
				Fatallnf("Failed to create output file: %v", err)
			}
			defer f_stdout[node].Close()
			defer f_stderr[node].Close()
		}
	}

	// Pick nodes whose output will be displayed promptly
	if prompt < 0 {
		prompt = 0
	} else if prompt > len(all_nodes) {
		prompt = len(all_nodes)
	}
	prompt_nodes := make(map[string]bool, prompt)
	for i := 0; i < prompt; i++ {
		prompt_nodes[all_nodes[i]] = true
	}

	// Initialize output cache
	cache := make(map[string][]rune, len(all_nodes))
	for _, node := range all_nodes {
		cache[node] = nil
	}

	// Handle SIGINT
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		summary(cache, finished_nodes, failed_nodes, all_nodes, cache_size, job_time)
		if len(all_nodes) > len(finished_nodes) {
			Printlnf("Job %v is still running.", job_id)
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
			Printlnf("Failed to receive output.")
			time.Sleep(time.Second)
		} else {
			node := output.GetNode()
			stdout, stderr := output.GetStdout(), output.GetStderr()
			content := stdout + stderr

			if !background {
				// End of output of a node
				if len(content) == 0 {
					state := "finished"
					finished_nodes = append(finished_nodes, node)
					exit_code := output.GetExitCode()
					if exit_code != 0 {
						state = fmt.Sprintf("failed with exit code %v", exit_code)
						failed_nodes = append(failed_nodes, node)
					}
					duration := time.Since(start_time)
					job_time = append(job_time, duration)
					Printlnf("[%v/%v] Command %v on node %v in %v.", len(finished_nodes), len(all_nodes), state, node, duration)
				} else {
					// Cache output for summary
					if cache_size > 0 {
						// TODO: Consider changing the stdout/stderr type in stream from string to []rune to improve performance
						cache[node] = append(cache[node], []rune(content)...) // Buffer output
						// Use []rune instead of string/[]byte to prevent an unicode character from being splited when truncating the cache
						if over_size := len(cache[node]) - (cache_size + 1); over_size > 0 {
							cache[node] = cache[node][over_size:]
						}
					}

					// Print output promptly
					content = strings.TrimSpace(content)
					if _, ok := prompt_nodes[node]; ok && len(content) > 0 {
						Printlnf("[%v]: %v", node, content)
					}
				}
			}

			// Save output to file
			if dump {
				if _, err = f_stdout[node].WriteString(stdout); err == nil {
					_, err = f_stderr[node].WriteString(stderr)
				}
				if err != nil {
					Fatallnf("Failed to write file: %v", err)
				}
			}
		}
	}
	if !background {
		summary(cache, finished_nodes, failed_nodes, all_nodes, cache_size, job_time)
	}
	if dump {
		Printlnf("Output is dumped to %v", output_dir)
	}
}

func summary(cache map[string][]rune, finished_nodes, failed_nodes, all_nodes []string, cache_size int, job_time []time.Duration) {
	if cache_size > 0 {
		Printlnf("")
		nodes := make([]string, 0, len(cache))
		for node := range cache {
			nodes = append(nodes, node)
		}
		sort.Strings(nodes)
		for _, node := range nodes {
			output := cache[node]
			heading := fmt.Sprintf("---[%v]---", node)
			Printlnf(GetPaddingLine(heading))
			if over_size := len(output) - cache_size; over_size > 0 {
				output = output[over_size:]
				Printlnf("(Truncated)")
				fmt.Print("...")
			}
			Printlnf(string(output))
		}
	}
	min, max, mean, mid, std_dev := getTimeStat(job_time)
	Printlnf(GetPaddingLine(""))
	runtime := fmt.Sprintf("Runtime: Min=%v, Max=%v, Mean=%v, Mid=%v, SD=%v", min, max, mean, mid, std_dev)
	if len(runtime) <= ConsoleWidth {
		Printlnf(runtime)
	} else {
		Printlnf(strings.NewReplacer(" ", LineEnding, ",", "").Replace(runtime))
		Printlnf("")
	}
	Printlnf("%v of %v nodes succeeded.", len(finished_nodes)-len(failed_nodes), len(all_nodes))
	if len(failed_nodes) > 0 {
		sort.Strings(failed_nodes)
		Printlnf("Failed nodes (%v/%v): %v", len(failed_nodes), len(all_nodes), strings.Join(failed_nodes, ", "))
	}
}

func getTimeStat(data []time.Duration) (min, max, mean, mid, std_dev time.Duration) {
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
