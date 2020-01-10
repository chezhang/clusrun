package main

import (
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
)

func GetConsoleWidth() (int, error) {
	if runtime.GOOS == "windows" {
		output, err := exec.Command("mode", "con").CombinedOutput()
		if err != nil {
			return 0, err
		}
		re := regexp.MustCompile(`\d+`)
		rs := re.FindAllString(string(output), -1)
		width, err := strconv.Atoi(rs[1])
		if err != nil {
			return 0, err
		}
		return width, nil
	} else {
		// TODO
		return 0, nil
	}
}
