package main

import (
	"fmt"
	"golang.org/x/crypto/ssh/terminal"
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
	if console_width, _, err = terminal.GetSize(int(os.Stdout.Fd())); err != nil {
		fmt.Printf("[Warning] Failed to get console width: %v\n", err)
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
	node            - list node(s) in the cluster
	run             - run a command or script on node(s) in the cluster
	job             - list job(s) in the cluster

Usage of node:
	clus node [options]
	clus node -h

Usage of run:
	clus run [options] <command>
	clus run -h

Usage of job:
	clus job [options] [jobs]
	clus job -h
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
