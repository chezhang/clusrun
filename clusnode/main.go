package main

import (
	pb "clusrun/proto"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/judwhite/go-svc/svc"
	"google.golang.org/grpc"
)

const (
	default_host = "localhost"
	default_port = "50505"
	default_node = default_host + ":" + default_port
)

func main() {
	if len(os.Args) < 2 {
		DisplayUsage()
		return
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch strings.ToLower(cmd) {
	case "start":
		Start(args)
	case "get":
		Get(args)
	case "set":
		Set(args)
	default:
		DisplayUsage()
	}
}

func DisplayUsage() {
	fmt.Printf(`
Usage: 
	clusnode <command> [arguments]

The commands are:
	start           - start the clusnode
	get             - get settings of the started clusnode
	set             - set the started clusnode

`)
}

func Start(args []string) {
	fs := flag.NewFlagSet("start flag", flag.ExitOnError)
	default_config_file := os.Args[0] + ".config"
	default_log_dir := os.Args[0] + ".log"
	default_log_file_label := default_log_dir + string(filepath.Separator) + "<start time>.log"
	default_log_file := default_log_dir + string(filepath.Separator) + time.Now().Format("20060102150405.log")
	headnodes := fs.String("headnodes", default_node, "specify the host addresses of headnodes for this clusnode to join in")
	host := fs.String("host", default_node, "specify the host address of this clusnode")
	log_file := fs.String("log-file", default_log_file_label, "specify the file for logging")
	config_file := fs.String("config-file", default_config_file, "specify the config file for saving and loading settings")
	// flag.StringVar(&clusnode_hosting, "host", default_node, "specify the host address of this clusnode")
	fs.Parse(args)

	// Setup log file
	if *log_file == default_log_file_label {
		if err := os.MkdirAll(default_log_dir, os.ModePerm); err != nil {
			log.Fatalf("Error creating log dir: %v", err)
		}
		*log_file = default_log_file
	}
	f, err := os.OpenFile(*log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	fmt.Println("Log file:", *log_file)

	// Setup config file
	clusnode_config_file = *config_file

	// Setup the host address of this clusnode
	hostname, port, err := ParseHostAddress(*host)
	if err != nil {
		log.Fatalf("Failed to parse clusnode host address: %v", err)
	}
	clusnode_hosting = hostname + ":" + port

	// Setup the headnodes this clusnode will report to
	if *headnodes != default_node {
		log.Printf("Adding headnodes from parameter: %v", *headnodes)
		for _, headnode := range strings.Split(*headnodes, ",") {
			if err := AddHeadnode(headnode); err != nil {
				log.Fatalf(err.Error())
			}
		}
		SaveHeadnodes()
	} else {
		if headnodes_loaded := ReadHeadnodes(); len(headnodes_loaded) > 0 {
			log.Printf("Adding headnodes loaded from config: %v", headnodes_loaded)
			for _, headnode := range headnodes_loaded {
				if err := AddHeadnode(headnode); err != nil {
					log.Fatalf(err.Error())
				}
			}
		} else {
			log.Printf("Adding default headnode: %v", default_node)
			AddHeadnode(default_node)
		}
	}

	// Start clusnode service
	prg := &program{}
	if err := svc.Run(prg); err != nil {
		log.Fatal(err)
	}
	log.Printf("Exited")
}

func Get(args []string) {
	GetHeadnodes()
}

func Set(args []string) {
	fs := flag.NewFlagSet("set flag", flag.ExitOnError)
	headnodes := fs.String("headnodes", "", "add headnodes for this clusnode to join in")
	fs.Parse(args)
	if fs.NFlag() == 0 {
		fs.PrintDefaults()
		return
	}
	if *headnodes != "" {
		SetHeadnodes(strings.Split(*headnodes, ","))
	}
}

func ParseHostAddress(address string) (hostname string, port string, err error) {
	segs := strings.Split(address, ":")
	if len(segs) > 2 {
		err = errors.New("Incorrect host address: " + address)
		return
	} else {
		hostname = strings.TrimSpace(segs[0])
		if len(segs) == 2 {
			temp_port, temp_err := strconv.ParseUint(segs[1], 10, 16)
			if temp_err != nil {
				err = errors.New("Incorrect port format: " + temp_err.Error())
				return
			}
			port = strconv.Itoa(int(temp_port))
		} else {
			port = default_port
		}
	}
	return hostname, port, nil
}

func GetHeadnodes() {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, default_node, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Println("Please ensure the clusnode is started")
		return
	}
	defer conn.Close()
	c := pb.NewClusnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Get headnodes
	reply, err := c.GetHeadnodes(ctx, &pb.Empty{})
	if err != nil {
		fmt.Println("Get headnodes failed:", err)
	} else {
		var t, f []string
		for k, v := range reply.GetHeadnodes() {
			if v {
				t = append(t, k)
			} else {
				f = append(f, k)
			}
		}
		name, err := os.Hostname()
		if err != nil {
			fmt.Println("Failed to get hostname:", err)
		}
		fmt.Printf("%v is reporting to headnodes: %v\n", name, t)
		if len(f) > 0 {
			fmt.Printf("%v failed to report to headnodes: %v\n", name, f)
		}
	}
}

func SetHeadnodes(headnodes []string) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, default_node, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Println("Please ensure the clusnode is started")
		return
	}
	defer conn.Close()
	c := pb.NewClusnodeClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set headnodes
	reply, err := c.SetHeadnodes(ctx, &pb.SetHeadnodesRequest{Headnodes: headnodes})
	if err != nil {
		fmt.Println("Set headnodes failed:", err)
	} else {
		fmt.Println("Result:")
		for k, v := range reply.GetResults() {
			fmt.Printf("\t%v : %v", k, v)
		}
	}
}
