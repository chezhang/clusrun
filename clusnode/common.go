package main

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	DefaultPort = "50505"
)

var (
	ExecutablePath string
	RunOnWindows   bool
	NodeHost       string
	NodeName       string
)

func ParseHostAddress(address string) (hostname, port, host string, err error) {
	segs := strings.Split(address, ":")
	if len(segs) > 2 {
		err = errors.New("Incorrect host address: " + address)
		return
	} else {
		hostname = strings.ToUpper(strings.TrimSpace(segs[0]))
		if len(hostname) == 0 {
			err = errors.New("Empty address")
			return
		}
		if hostname == "LOCALHOST" {
			hostname = strings.ToUpper(NodeName)
		}
		if len(segs) == 2 {
			temp_port, temp_err := strconv.ParseUint(segs[1], 10, 16)
			if temp_err != nil {
				err = errors.New("Incorrect port format: " + temp_err.Error())
				return
			}
			port = strconv.Itoa(int(temp_port))
		} else {
			port = DefaultPort
		}
	}
	host = hostname + ":" + port
	return
}

func LogPanicBeforeExit() {
	if panic := recover(); panic != nil {
		message := fmt.Sprintf("%v\n%v", panic, string(debug.Stack()))
		LogFatality(message)
	}
}
