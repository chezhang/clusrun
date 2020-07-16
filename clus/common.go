package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"golang.org/x/net/html/charset"
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
	LineEnding   string
	ConsoleWidth int
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

func ReadFile(f string) string {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		Fatallnf("Failed to read file %q: %v", f, err)
	}
	if _, e, _ := charset.DetermineEncoding(b, ""); e != "utf-8" && e != "windows-1252" {
		Fatallnf("Invalid encoding %q of file %q", e, f)
	}
	return string(b)
}

func ParseNodesOrGroups(s, f string) []string {
	// if len(s) > 0 && len(file) > 0 {
	// 	Fatallnf("Please only specify one of the string or file to load nodes or node groups.")
	// }
	items := strings.Split(s, ",")
	if len(f) > 0 {
		items = append(strings.Split(strings.NewReplacer("\r\n", ",", "\n", ",").Replace(ReadFile(f)), ","), items...)
	}
	set := map[string]bool{}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) > 0 {
			set[item] = true
		}
	}
	items = make([]string, 0, len(set))
	for item := range set {
		items = append(items, item)
	}
	return items
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
		Printlnf("Can not connect %v in %v: %v", *Headnode, ConnectTimeout, err)
		Fatallnf("Please ensure the headnode is started and accessible.")
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
