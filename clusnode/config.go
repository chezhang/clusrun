package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

// TODO: handle simultaneous access by multiple clusnodes(different host) -> use seperate config file

var clusnode_config_file string

func ReadConfig() (config map[string]interface{}, err error) {
	log.Printf("Loading config file: %v", clusnode_config_file)
	json_string, err := ioutil.ReadFile(clusnode_config_file)
	if err == nil {
		err = json.Unmarshal(json_string, &config)
	}
	return
}

func SaveConfig(config map[string]interface{}) error {
	log.Printf("Saving config file: %v", clusnode_config_file)
	json_string, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(clusnode_config_file, json_string, 0644)
}

func ReadHeadnodes() []string {
	headnodes := []string{}
	config, err := ReadConfig()
	if err != nil {
		log.Printf("Failed to load config: %v", err)
		return headnodes
	}
	c := config[clusnode_host]
	if c == nil {
		log.Printf("No config loaded for clusnode: %v", clusnode_host)
		return headnodes
	}
	h := c.(map[string]interface{})["headnodes"]
	if h == nil {
		log.Printf("No headnodes config loaded for clusnode: %v", clusnode_host)
		return headnodes
	}
	for _, headnode := range h.([]interface{}) {
		headnodes = append(headnodes, headnode.(string))
	}
	return headnodes
}

func SaveHeadnodes() {
	config, err := ReadConfig()
	if err != nil {
		config = make(map[string]interface{})
	}
	if config[clusnode_host] == nil {
		config[clusnode_host] = make(map[string]interface{})
	}
	headnodes := []string{}
	headnodes_reporting.Range(func(key, val interface{}) bool {
		headnodes = append(headnodes, key.(string))
		return true
	})
	config[clusnode_host].(map[string]interface{})["headnodes"] = headnodes
	if err = SaveConfig(config); err != nil {
		log.Printf("Failed to save config: %v", err)
	}
}
