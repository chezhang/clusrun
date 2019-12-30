package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	default_port    = "50505"
	local_host      = "localhost:" + default_port
	connect_timeout = 30 * time.Second
)

func main() {
	if len(os.Args) < 2 {
		DisplayUsage()
		return
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch strings.ToLower(cmd) {
	case "node":
		Node(args)
	case "run":
		Run(args)
	case "job":
		Job(args)
	default:
		DisplayUsage()
	}
}

func DisplayUsage() {
	fmt.Printf(`
Usage: 
	clus <command> [arguments]

The commands are:
	node            - list nodes in the cluster
	run             - run a command or script on nodes of the cluster
	job             - list jobs in the cluster

`)
}

func ParseHeadnode(headnode string) string {
	if strings.Contains(headnode, ":") {
		return headnode
	} else {
		return headnode + ":" + default_port
	}
}
