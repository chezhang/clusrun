package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func SetupFireWall() {
	_, port, _, _ := ParseHostAddress(NodeHost)
	LogInfo("Setup firewall of port %v", port)
	var cmds [][]string
	if RunOnWindows {
		rule_name := fmt.Sprintf("name=clusnode-%v", port)
		cmd_del_fw := []string{"netsh", "advfirewall", "firewall", "delete", "rule", rule_name}
		cmd_add_fw := []string{"netsh", "advfirewall", "firewall", "add", "rule", rule_name, fmt.Sprintf("localport=%v", port),
			"dir=in", "protocol=tcp", "enable=yes", "action=allow", "profile=private,domain,public"}
		cmds = [][]string{cmd_del_fw, cmd_add_fw}
	} else {
		cmd_add_fw := []string{"firewall-cmd", "--permanent", fmt.Sprintf("--add-port=%v/tcp", port)}
		cmd_reload := []string{"firewall-cmd", "--reload"}
		cmds = [][]string{cmd_add_fw, cmd_reload}
	}
	for _, cmd := range cmds {
		LogInfo("Run command: %v", strings.Join(cmd, " "))
		output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
		LogInfo("Command output: %s", output)
	}
}
