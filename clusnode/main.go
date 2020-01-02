package main

import (
	pb "../protobuf"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/judwhite/go-svc/svc"
	"google.golang.org/grpc"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	default_port = "50505"
)

var (
	local_host string
)

func main() {
	if len(os.Args) < 2 {
		DisplayUsage()
		return
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}
	clusnode_name = hostname
	local_host = hostname + ":" + default_port

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
	clusnode <command> [options]

The commands are:
	start           - start the clusnode
	get             - get settings of the started clusnode
	set             - set the started clusnode

`)
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func Start(args []string) {
	fs := flag.NewFlagSet("clusnode start options", flag.ExitOnError)
	default_config_file := os.Args[0] + ".config"
	default_log_dir := os.Args[0] + ".log"
	default_log_file_label := filepath.Join(default_log_dir, "<start time>.log")
	default_log_file := filepath.Join(default_log_dir, time.Now().Format("20060102150405.log"))
	headnodes := fs.String("headnodes", local_host, "specify the host addresses of headnodes for this clusnode to join in")
	host := fs.String("host", local_host, "specify the host address of this clusnode")
	log_file := fs.String("log-file", default_log_file_label, "specify the file for logging")
	config_file := fs.String("config-file", default_config_file, "specify the config file for saving and loading settings")
	fs.Parse(args)

	// Setup log file
	if *log_file == default_log_file_label {
		if err := os.MkdirAll(default_log_dir, 0644); err != nil {
			log.Fatalf("Error creating log dir: %v", err)
		}
		*log_file = default_log_file
	}
	f, err := os.OpenFile(*log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	fmt.Println("Log file:", *log_file)

	// Catch and log panic
	defer LogPanic()

	// Setup config file
	clusnode_config_file = *config_file

	// Setup the host address of this clusnode
	hostname, port, err := ParseHostAddress(*host)
	if err != nil {
		log.Fatalf("Failed to parse clusnode host address: %v", err)
	}
	clusnode_host = hostname + ":" + port

	// Setup the headnodes this clusnode will report to
	if reflect.Indirect(reflect.ValueOf(fs)).FieldByName("actual").MapIndex(reflect.ValueOf("headnodes")).IsValid() { // if "headnodes" flag is set
		log.Printf("Adding headnode(s): %v", *headnodes)
		for _, headnode := range strings.Split(*headnodes, ",") {
			if err := AddHeadnode(headnode); err != nil {
				log.Fatalf(err.Error())
			}
		}
		SaveHeadnodes()
	} else {
		if headnodes_loaded := ReadHeadnodes(); len(headnodes_loaded) > 0 {
			log.Printf("Adding loaded headnode(s): %v", headnodes_loaded)
			for _, headnode := range headnodes_loaded {
				if err := AddHeadnode(headnode); err != nil {
					log.Fatalf(err.Error())
				}
			}
		} else {
			log.Printf("Adding default headnode: %v", local_host)
			AddHeadnode(local_host)
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
	fs := flag.NewFlagSet("clusnode set options", flag.ExitOnError)
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
		if len(hostname) == 0 {
			err = errors.New("Empty address")
			return
		}
		if strings.ToLower(hostname) == "localhost" {
			hostname = clusnode_name
		}
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
	return
}

func GetHeadnodes() {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, local_host, grpc.WithInsecure(), grpc.WithBlock())
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
		fmt.Printf("%v is reporting to headnodes: %v\n", clusnode_name, t)
		if len(f) > 0 {
			fmt.Printf("%v failed to report to headnodes: %v\n", clusnode_name, f)
		}
	}
}

func SetHeadnodes(headnodes []string) {
	// Setup connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, local_host, grpc.WithInsecure(), grpc.WithBlock())
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

func LogPanic() {
	if panic := recover(); panic != nil {
		message := fmt.Sprintf("%v\n%v", panic, string(debug.Stack()))
		fmt.Println(message)
		log.Fatalln(message)
	}
}
