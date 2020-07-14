package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	Max_Int        = int(^uint(0) >> 1)
	Min_Int        = -Max_Int - 1
	DefaultPort    = "50505"
	ConnectTimeout = 30 * time.Second
)

var (
	LineEnding     string
	RunOnWindows   bool
	ExecutablePath string
	NodeHost       string
	NodeName       string
	Tls            struct {
		Enabled  bool
		CertFile string
		KeyFile  string
	}
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

func FileNameFormatHost(host string) string {
	return strings.ReplaceAll(host, ":", ".")
}

func ConnectNode(host string) (*grpc.ClientConn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	secureOption := grpc.WithInsecure()
	if Tls.Enabled {
		config := &tls.Config{
			InsecureSkipVerify: true,
		}
		secureOption = grpc.WithTransportCredentials(credentials.NewTLS(config))
	}
	conn, err := grpc.DialContext(ctx, host, secureOption, grpc.WithBlock())
	if err != nil {
		LogError("Can not connect %v in %v: %v", host, ConnectTimeout, err)
	}
	return conn, cancel
}

func Printlnf(format string, v ...interface{}) {
	fmt.Printf(format+LineEnding, v...)
}

func Fatallnf(format string, v ...interface{}) {
	Printlnf(format, v...)
	os.Exit(1)
}
