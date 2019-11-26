package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

func SetupFireWall() {
	program_path, err := os.Executable()
	if err != nil {
		log.Printf("Failed to get current executable path, skip fire wall setting: %v", err)
		return
	}
	for _, dir := range []string{"in", "out"} {
		log.Printf("Setup %v-bound firewall", dir)
		rule_name := fmt.Sprintf("name=clusnode-%v", dir)
		program := fmt.Sprintf("program=\"%v\"", program_path)
		bound_dir := fmt.Sprintf("dir=%v", dir)
		cmd_del_fw := []string{"netsh", "advfirewall", "firewall", "delete", "rule", rule_name}
		cmd_add_fw := []string{"netsh", "advfirewall", "firewall", "add", "rule", rule_name, program,
			"protocol=tcp", bound_dir, "enable=yes", "action=allow", "profile=private,domain,public"}
		for _, cmd := range [][]string{cmd_del_fw, cmd_add_fw} {
			log.Printf("Run command: %v", strings.Join(cmd, " "))
			output, _ := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
			log.Printf("Output: %s", output)
		}
	}
}
