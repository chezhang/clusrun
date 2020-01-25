// +build windows

package platform

import (
	"os/exec"
)

func SetSysProcAttr(cmd *exec.Cmd) {
	_ = cmd
}

func KillProcessGroup(pid int) {
	_ = pid
}
