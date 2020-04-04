package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func SetupFireWall() {
	if RunOnWindows {
		for _, dir := range []string{"in", "out"} {
			LogInfo("Setup %v-bound firewall", dir)
			rule_name := fmt.Sprintf("name=clusnode-%v", dir)
			program := fmt.Sprintf("program=\"%v\"", ExecutablePath)
			bound_dir := fmt.Sprintf("dir=%v", dir)
			cmd_del_fw := []string{"netsh", "advfirewall", "firewall", "delete", "rule", rule_name}
			cmd_add_fw := []string{"netsh", "advfirewall", "firewall", "add", "rule", rule_name, bound_dir,
				"protocol=tcp", "enable=yes", "action=allow", "profile=private,domain,public"}
			cmd_add_fw = append(cmd_add_fw, strings.Split(program, " ")...)
			for _, cmd := range [][]string{cmd_del_fw, cmd_add_fw} {
				LogInfo("Run command: %v", strings.Join(cmd, " "))
				output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
				LogInfo("Command output: %s", output)
			}
		}
	} else {
		// TODO
	}
}
