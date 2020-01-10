package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	default_port        = "50505"
	local_host          = "localhost:" + default_port
	connect_timeout     = 30 * time.Second
	default_line_length = 60
)

var (
	console_width = 0
)

func main() {
	if len(os.Args) < 2 {
		DisplayUsage()
		return
	}
	var err error
	if console_width, err = GetConsoleWidth(); err != nil {
		fmt.Printf("[Warning] Failed to get console width: %v", err)
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

func GetPaddingLine(heading string) string {
	padding := "-"
	line_length := default_line_length
	if console_width > 0 {
		line_length = console_width - 1
	}
	if padding_length := line_length - len(heading); padding_length > 0 {
		paddings := strings.Repeat(padding, padding_length/2)
		heading = fmt.Sprintf("%v%v%v", paddings, heading, paddings)
		if len(heading) < line_length {
			heading += padding
		}
	}
	return heading
}
