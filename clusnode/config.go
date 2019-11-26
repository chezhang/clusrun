package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

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
	json_string, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(clusnode_config_file, json_string, os.ModePerm)
}

func ReadHeadnodes() []string {
	headnodes := []string{}
	config, err := ReadConfig()
	if err != nil {
		log.Printf("Failed to load config: %v", err)
		return headnodes
	}
	for _, headnode := range config["headnodes"].([]interface{}) {
		headnodes = append(headnodes, headnode.(string))
	}
	return headnodes
}

func SaveHeadnodes() {
	config, err := ReadConfig()
	if err != nil {
		config = make(map[string]interface{})
	}
	headnodes := []string{}
	headnodes_reporting.Range(func(key, val interface{}) bool {
		headnodes = append(headnodes, key.(string))
		return true
	})
	config["headnodes"] = headnodes
	if err = SaveConfig(config); err != nil {
		log.Printf("Failed to save config: %v", err)
	}
}
