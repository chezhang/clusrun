package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	Max_Int           = int(^uint(0) >> 1)
	Min_Int           = -Max_Int - 1
	DefaultPort       = "50505"
	LocalHost         = "localhost:" + DefaultPort
	ConnectTimeout    = 10 * time.Second
	DefaultLineLength = 60
)

var (
	ConsoleWidth = 0
	Headnode     *string
	insecure     *bool
)

func SetGlobalParameters(fs *flag.FlagSet) {
	Headnode = fs.String("headnode", LocalHost, "specify the headnode to connect")
	insecure = fs.Bool("insecure", false, "specify to connect headnode with insecure connection")
}

func ParseHeadnode(headnode string) string {
	if strings.Contains(headnode, ":") {
		return headnode
	} else {
		return headnode + ":" + DefaultPort
	}
}

func GetPaddingLine(heading string) string {
	padding := "-"
	line_length := DefaultLineLength
	if ConsoleWidth > 0 {
		line_length = ConsoleWidth - 1
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

func MaxInt(array ...int) int {
	max := Min_Int
	for _, i := range array {
		if i > max {
			max = i
		}
	}
	return max
}

func ConnectHeadnode() (*grpc.ClientConn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	secureOption := grpc.WithInsecure()
	if !*insecure {
		config := &tls.Config{
			InsecureSkipVerify: true,
		}
		secureOption = grpc.WithTransportCredentials(credentials.NewTLS(config))
	}
	conn, err := grpc.DialContext(ctx, ParseHeadnode(*Headnode), secureOption, grpc.WithBlock())
	if err != nil {
		fmt.Printf("Can not connect %v in %v: %v\n", *Headnode, ConnectTimeout, err)
		fmt.Println("Please ensure the headnode is started and accessible.")
		os.Exit(1)
	}
	return conn, cancel
}
