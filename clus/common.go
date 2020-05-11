package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"os"
	"strings"
	"time"
)

const (
	Max_Int           = int(^uint(0) >> 1)
	Min_Int           = -Max_Int - 1
	DefaultPort       = "50505"
	LocalHost         = "localhost:" + DefaultPort
	ConnectTimeout    = 30 * time.Second
	DefaultLineLength = 60
)

var (
	ConsoleWidth = 0
)

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

func ConnectHeadnode(headnode string) (*grpc.ClientConn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ConnectTimeout)
	conn, err := grpc.DialContext(ctx, headnode, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Can not connect:", err)
		fmt.Printf("Please ensure the headnode %v is started and accessible.", headnode)
		os.Exit(1)
	}
	return conn, cancel
}
