package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juju/fslock"
	"io/ioutil"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	Config_Clusnode                = "clusnode role"
	Config_Headnode                = "headnode role"
	Config_Clusnode_Headnodes_Name = "headnodes to report"
)

var (
	node_config_file       string
	positive_int_validator = func(value interface{}) error {
		if v, ok := value.(int); !ok {
			return errors.New("Invalid type")
		} else if v <= 0 {
			return errors.New("Value should be positive")
		}
		return nil
	}

	Config_Clusnode_HeartbeatIntervalSecond = ConfigItem{
		Name:      "heartbeat interval in seconds",
		Value:     1,
		Validator: positive_int_validator,
	}
	Config_Headnode_HeartbeatTimeoutSecond = ConfigItem{
		Name:      "mark node lost after no heartbeat for seconds",
		Value:     5,
		Validator: positive_int_validator,
	}
	Config_Headnode_MaxJobCount = ConfigItem{
		Name:      "max job count",
		Value:     100,
		Validator: positive_int_validator,
	}
	Config_Headnode_OutputMaxSingleSizeKb = ConfigItem{
		Name:      "max size for output of one job and one node in KB",
		Value:     1000,
		Validator: positive_int_validator,
	}
	Config_Headnode_OutputMaxTotalSizeMb = ConfigItem{
		Name:      "max size for all job output in MB",
		Value:     1000,
		Validator: positive_int_validator,
	}
	Config_Headnode_PurgeLostForSecond = ConfigItem{
		Name:      "purge nodes lost for seconds",
		Value:     3600,
		Validator: positive_int_validator,
	}
	Config_Headnode_StoreOutput = ConfigItem{
		Name:  "store output",
		Value: true,
	}

	configs_clusnode = map[string]*ConfigItem{
		Config_Clusnode_HeartbeatIntervalSecond.Name: &Config_Clusnode_HeartbeatIntervalSecond,
	}
	configs_headnode = map[string]*ConfigItem{
		Config_Headnode_HeartbeatTimeoutSecond.Name: &Config_Headnode_HeartbeatTimeoutSecond,
		Config_Headnode_MaxJobCount.Name:            &Config_Headnode_MaxJobCount,
		Config_Headnode_StoreOutput.Name:            &Config_Headnode_StoreOutput,
	}
)

func SaveNodeConfigs() {
	log.Printf("Saving node configs")

	// Use the process file as a lock to enable multi-process access of the config file
	lock := fslock.New(executable_path)
	if err := lock.LockWithTimeout(3 * time.Second); err != nil {
		log.Printf("Failed to lock the process file %v when saving node configs", executable_path)
		return
	}
	defer lock.Unlock()

	// Read config file and check format
	config, err := readConfigFile()
	if err != nil {
		log.Printf("Failed to load config file: %v\nRebuild it", err)
		config = make(map[string]interface{})
	}
	if _, ok := config[clusnode_host]; !ok {
		config[clusnode_host] = make(map[string]interface{})
	}
	node_config, ok := config[clusnode_host].(map[string]interface{})
	if !ok {
		log.Printf("Failed to parse config of node %v, rebuild it", clusnode_host)
		node_config = make(map[string]interface{})
		config[clusnode_host] = node_config
	}
	if _, ok := node_config[Config_Clusnode]; !ok {
		node_config[Config_Clusnode] = make(map[string]interface{})
	}
	clusnode_config, ok := node_config[Config_Clusnode].(map[string]interface{})
	if !ok {
		log.Printf("Failed to parse clusnode config of node %v, rebuild it", clusnode_host)
		clusnode_config = make(map[string]interface{})
		node_config[Config_Clusnode] = clusnode_config
	}
	if _, ok := node_config[Config_Headnode]; !ok {
		node_config[Config_Headnode] = make(map[string]interface{})
	}
	headnode_config, ok := node_config[Config_Headnode].(map[string]interface{})
	if !ok {
		log.Printf("Failed to parse headnode config of node %v, rebuild it", clusnode_host)
		headnode_config = make(map[string]interface{})
		node_config[Config_Headnode] = headnode_config
	}

	// Save node configs
	connected, connecting := GetHeadnodes()
	clusnode_config[Config_Clusnode_Headnodes_Name] = append(connected, connecting...)
	for _, config := range configs_clusnode {
		clusnode_config[config.Name] = config.Value
	}
	for _, config := range configs_headnode {
		headnode_config[config.Name] = config.Value
	}

	// Save config file
	if err = saveConfigFile(config); err != nil {
		log.Printf("Failed to save config file: %v", err)
	}
}

func LoadNodeConfigs() {
	log.Printf("Loading node configs")
	config, err := readConfigFile()
	if err != nil {
		log.Printf("Failed to load config file: %v", err)
		return
	}
	if _, ok := config[clusnode_host]; !ok {
		log.Printf("No config loaded for node: %v", clusnode_host)
		return
	}
	node_config, ok := config[clusnode_host].(map[string]interface{})
	if !ok {
		log.Printf("Failed to parse config of node %v", clusnode_host)
		return
	}

	if clusnode_config, ok := node_config[Config_Clusnode].(map[string]interface{}); ok {
		if headnodes, ok := clusnode_config[Config_Clusnode_Headnodes_Name].([]interface{}); ok {
			log.Printf("Add loaded headnode(s): %v", headnodes)
			for _, headnode := range headnodes {
				if h, ok := headnode.(string); !ok {
					log.Printf("Can not parse headnode as string: %v", headnode)
				} else if _, err := AddHeadnode(h); err != nil {
					log.Printf(err.Error())
				}
			}
		}
		for _, config := range configs_clusnode {
			if value, ok := clusnode_config[config.Name]; ok {
				if err := config.Set(value); err != nil {
					log.Printf("Failed to set %q for clusnode to %v: %v", config.Name, value, err)
				}
			}
		}
	}
	if headnode_config, ok := node_config[Config_Headnode].(map[string]interface{}); ok {
		for _, config := range configs_headnode {
			if value, ok := headnode_config[config.Name]; ok {
				if err := config.Set(value); err != nil {
					log.Printf("Failed to set %q for headnode to %v: %v", config.Name, value, err)
				}
			}
		}
	}
}

func SetNodeConfigs(role string, configs map[string]string) map[string]string {
	log.Printf("SetConfigs: %v", configs)
	var configs_role map[string]*ConfigItem
	if role == Config_Clusnode {
		configs_role = configs_clusnode
	} else if role == Config_Headnode {
		configs_role = configs_headnode
	} else {
		log.Panicf("Invalid config role: %v", role)
	}
	results := make(map[string]string)
	for k, v := range configs {
		if config, ok := configs_role[k]; !ok {
			results[k] = "Invalid config name"
		} else if err := config.Set(v); err != nil {
			results[k] = err.Error()
		} else {
			results[k] = v
		}
	}
	log.Printf("SetConfigs results: %v", results)
	SaveNodeConfigs()
	return results
}

func GetNodeConfigs(role string) map[string]string {
	configs := map[string]string{}
	var configs_role map[string]*ConfigItem
	if role == Config_Clusnode {
		configs_role = configs_clusnode
		connected, connecting := GetHeadnodes()
		if len(connected) > 0 {
			configs[Config_Clusnode_Headnodes_Name+" (connected)"] = strings.Join(connected, ", ")
		}
		if len(connecting) > 0 {
			configs[Config_Clusnode_Headnodes_Name+" (connecting)"] = strings.Join(connecting, ", ")
		}
	} else if role == Config_Headnode {
		configs_role = configs_headnode
	} else {
		log.Panicf("Invalid config role: %v", role)
	}
	for _, config := range configs_role {
		configs[config.Name] = fmt.Sprintf("%v", config.Value)
	}
	log.Printf("GetConfigs results: %v", configs)
	return configs
}

func readConfigFile() (config map[string]interface{}, err error) {
	json_string, err := ioutil.ReadFile(node_config_file)
	if err == nil {
		err = json.Unmarshal(json_string, &config)
	}
	return
}

func saveConfigFile(config map[string]interface{}) error {
	json_string, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(node_config_file, json_string, 0644)
}

type ConfigItem struct {
	Name      string
	Value     interface{}
	Validator func(interface{}) error
}

func (c *ConfigItem) Set(value interface{}) error {
	v, err := convertType(value, reflect.TypeOf(c.Value).Kind())
	if err != nil {
		return err
	}
	if c.Validator != nil {
		if err := c.Validator(v); err != nil {
			return err
		}
	}
	c.Value = v
	log.Printf("Set config %q to %v", c.Name, v)
	return nil
}

func (c *ConfigItem) GetBool() (value bool) {
	if v, err := convertType(c.Value, reflect.Bool); err != nil {
		log.Panic(err.Error())
	} else {
		value = v.(bool)
	}
	return
}

func (c *ConfigItem) GetInt() (value int) {
	if v, err := convertType(c.Value, reflect.Int); err != nil {
		log.Panic(err.Error())
	} else {
		value = v.(int)
	}
	return
}

func convertType(from interface{}, t reflect.Kind) (to interface{}, err error) {
	err = errors.New(fmt.Sprintf("Failed to parse %v as type %v", from, t))
	switch v := from.(type) {
	case bool:
		switch t {
		case reflect.Bool:
			to = v
			err = nil
		}
	case int:
		switch t {
		case reflect.Int:
			to = v
			err = nil
		}
	case float64:
		switch t {
		case reflect.Int:
			to = int(v)
			err = nil
		}
	case string:
		switch t {
		case reflect.Int:
			if i, e := strconv.Atoi(v); e == nil {
				to = i
				err = nil
			}
		case reflect.Bool:
			if b, e := strconv.ParseBool(v); e == nil {
				to = b
				err = nil
			}
		}
	}
	return
}
