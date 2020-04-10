package main

import (
	"fmt"
	"golang.org/x/crypto/ssh/terminal"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		displayUsage()
		return
	}
	var err error
	if ConsoleWidth, _, err = terminal.GetSize(int(os.Stdout.Fd())); err != nil {
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
		displayUsage()
	}
}

func displayUsage() {
	fmt.Printf(`
Usage: 
	clus <command> [arguments]

The commands are:
	node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
	run             - run a command or script on nodes in the cluster
	job             - list, cancel or rerun jobs in the cluster

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
