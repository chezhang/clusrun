package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func SetupFireWall() {
	if RunOnWindows {
		_, port, _, _ := ParseHostAddress(NodeHost)
		LogInfo("Setup in-bound firewall of port %v", port)
		rule_name := fmt.Sprintf("name=clusnode-%v", port)
		allow_port := fmt.Sprintf("localport=%v", port)
		cmd_del_fw := []string{"netsh", "advfirewall", "firewall", "delete", "rule", rule_name}
		cmd_add_fw := []string{"netsh", "advfirewall", "firewall", "add", "rule", rule_name, allow_port,
			"dir=in", "protocol=tcp", "enable=yes", "action=allow", "profile=private,domain,public"}
		for _, cmd := range [][]string{cmd_del_fw, cmd_add_fw} {
			LogInfo("Run command: %v", strings.Join(cmd, " "))
			output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
			LogInfo("Command output: %s", output)
		}
	} else {
		// TODO
	}
}
