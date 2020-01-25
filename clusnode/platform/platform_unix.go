// +build linux darwin

package platform

import (
	"os/exec"
	"syscall"
)

func SetSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func KillProcessGroup(pid int) {
	syscall.Kill(-pid, syscall.SIGKILL)
}
