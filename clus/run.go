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
	groups := fs.String("groups", "", "specify certain node groups to run the command")
	groups_intersect := fs.Bool("intersect", false, "specify to run the command in intersection (union if not specified) of node groups")
	cache := fs.Int("cache", 1000, "specify the number of characters to cache and display for output of command on each node")
	prompt := fs.Int("prompt", 1, "specify the number of nodes, the output of which will be displayed promptly")
	sweep := fs.String("sweep", "", `perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][,step]}]"`)
	background := fs.Bool("background", false, "run command without printing output")
	// pick := fs.Int("pick", 0, "pick certain number of nodes to run, default 0 means pick all nodes")
	// merge := fs.Bool("merge", false, "specify if merge outputs with the same content for different nodes")
	fs.Parse(args)
	command := strings.Join(fs.Args(), " ")
	if len(*script) > 0 {
		if len(command) > 0 {
			fmt.Printf("[Warning] The command '%v' will be overwritten by scirpt %v\n", command, *script)
		}
		command = parseScript(*script)
	} else if len(command) <= 0 {
		displayRunUsage(fs)
		return
	}
	output_dir := ""
	if *dump {
		output_dir = createOutputDir()
	}
	RunJob(ParseHeadnode(*headnode), command, *sweep, output_dir, *pattern, parseNodesOrGroups(*groups), parseNodesOrGroups(*nodes), *cache, *prompt, *background, *groups_intersect)
}

func displayRunUsage(fs *flag.FlagSet) {
	fmt.Printf(`
Usage: 
  clus run [options] <command>

Options:
`)
	fs.PrintDefaults()
}

func parseScript(script string) string {
	command, err := ioutil.ReadFile(script)
	if err != nil {
		fmt.Printf("Failed to read commands in script: %v", err)
		os.Exit(1)
	}
	return string(command)
}

func parseNodesOrGroups(s string) (items []string) {
	for _, item := range strings.Split(s, ",") {
		if len(item) > 0 {
			items = append(items, item)
		}
	}
	return
}

func createOutputDir() string {
	cur_dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get working dir: %v", err)
		os.Exit(1)
	}
	output_dir := filepath.Join(cur_dir, "clus.run."+time.Now().Format("20060102150405"))
	if err := os.MkdirAll(output_dir, 0644); err != nil {
		fmt.Printf("Failed to create output dir: %v", err)
		os.Exit(1)
	}
	return output_dir
}

func RunJob(headnode, command, sweep, output_dir, pattern string, groups, nodes []string, cache_size, prompt int, background, intersect bool) {
	dump := len(output_dir) > 0

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
	stream, err := c.StartClusJob(ctx, &pb.StartClusJobRequest{Command: command, Sweep: sweep, Pattern: pattern, Groups: groups, GroupsIntersect: intersect, Nodes: nodes})
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
		fmt.Printf("Job %v started on %v nodes in cluster %v.\n", job_id, len(all_nodes), headnode)
		if dump {
			fmt.Println("Dumping output to", output_dir)
		} else if background {
			return
		}
		if !background {
			fmt.Println()
			if len(sweep) > 0 {
				fmt.Println("Sweep parameter:", sweep)
			}
			fmt.Println(GetPaddingLine("---Command---"))
			fmt.Println(command)
			fmt.Println(GetPaddingLine(""))
			fmt.Println()
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
				fmt.Printf("Failed to create output file: %v\n", err)
				return
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

	// Handle SIGINT
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		summary(cache, finished_nodes, failed_nodes, all_nodes, cache_size, job_time)
		if len(all_nodes) > len(finished_nodes) {
			fmt.Printf("Job %v is still running.\n", job_id)
		}
		os.Exit(1)
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
					duration := time.Now().Sub(start_time)
					job_time = append(job_time, duration)
					fmt.Printf("[%v/%v] Command %v on node %v in %v.\n", len(finished_nodes), len(all_nodes), state, node, duration)
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
						fmt.Printf("[%v]: %v\n", node, content)
					}
				}
			}

			// Save output to file
			if dump {
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
	if !background {
		summary(cache, finished_nodes, failed_nodes, all_nodes, cache_size, job_time)
	}
}

func summary(cache map[string][]rune, finished_nodes, failed_nodes, all_nodes []string, cache_size int, job_time []time.Duration) {
	if cache_size > 0 {
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
			if over_size := len(output) - cache_size; over_size > 0 {
				output = output[over_size:]
				fmt.Printf("(Truncated)\n...")
			}
			fmt.Println(string(output))
		}
	}
	min, max, mean, mid, std_dev := getTimeStat(job_time)
	fmt.Println(GetPaddingLine(""))
	runtime := fmt.Sprintf("Runtime: Min=%v, Max=%v, Mean=%v, Mid=%v, SD=%v", min, max, mean, mid, std_dev)
	if len(runtime) <= ConsoleWidth {
		fmt.Println(runtime)
	} else {
		fmt.Println(strings.ReplaceAll(strings.ReplaceAll(runtime, " ", "\n"), ",", ""))
		fmt.Println()
	}
	fmt.Printf("%v of %v nodes succeeded.\n", len(finished_nodes)-len(failed_nodes), len(all_nodes))
	if len(failed_nodes) > 0 {
		sort.Strings(failed_nodes)
		fmt.Printf("Failed nodes (%v/%v): %v\n", len(failed_nodes), len(all_nodes), strings.Join(failed_nodes, ", "))
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
