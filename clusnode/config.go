package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juju/fslock"
)

const (
	Config_Clusnode                = "clusnode role"
	Config_Headnode                = "headnode role"
	Config_Clusnode_Headnodes_Name = "headnodes"
)

var (
	NodeConfigFile       string
	positiveIntValidator = func(value interface{}) error {
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
		Validator: positiveIntValidator,
	}
	Config_Headnode_HeartbeatTimeoutSecond = ConfigItem{
		Name:      "mark node lost after no heartbeat for seconds",
		Value:     5,
		Validator: positiveIntValidator,
	}
	Config_Headnode_MaxJobCount = ConfigItem{
		Name:      "max job count",
		Value:     100,
		Validator: positiveIntValidator,
	}
	Config_Headnode_OutputMaxSingleSizeKb = ConfigItem{
		Name:      "max size for output of one job and one node in KB",
		Value:     1000,
		Validator: positiveIntValidator,
	}
	Config_Headnode_OutputMaxTotalSizeMb = ConfigItem{
		Name:      "max size for all job output in MB",
		Value:     1000,
		Validator: positiveIntValidator,
	}
	Config_Headnode_PurgeLostForSecond = ConfigItem{
		Name:      "purge nodes lost for seconds",
		Value:     3600,
		Validator: positiveIntValidator,
	}
	Config_Headnode_StoreOutput = ConfigItem{
		Name:  "store output",
		Value: false,
	}
	Config_LogGoId = ConfigItem{
		Name:  "add go id in logs",
		Value: false,
	}

	configs_clusnode = map[string]*ConfigItem{
		Config_Clusnode_HeartbeatIntervalSecond.Name: &Config_Clusnode_HeartbeatIntervalSecond,
	}
	configs_headnode = map[string]*ConfigItem{
		Config_Headnode_HeartbeatTimeoutSecond.Name: &Config_Headnode_HeartbeatTimeoutSecond,
		Config_Headnode_MaxJobCount.Name:            &Config_Headnode_MaxJobCount,
		Config_Headnode_StoreOutput.Name:            &Config_Headnode_StoreOutput,
	}
	configs_common = []*ConfigItem{
		&Config_LogGoId,
	}
)

func SaveNodeConfigs() {
	LogInfo("Saving node configs")

	// Use the process file as a lock to enable multi-process access of the config file
	lock := fslock.New(ExecutablePath)
	if err := lock.LockWithTimeout(3 * time.Second); err != nil {
		LogError("Failed to lock the process file %v when saving node configs", ExecutablePath)
		return
	}
	defer lock.Unlock()

	// Read config file and check format
	config, err := readConfigFile()
	if err != nil {
		if os.IsNotExist(err) {
			LogInfo("Config file doesn't exist, build it")
		} else {
			LogWarning("Failed to parse config file: %v%vRebuild it", err, LineEnding)
		}
		config = make(map[string]interface{})
	}
	if _, ok := config[NodeHost]; !ok {
		config[NodeHost] = make(map[string]interface{})
	}
	node_config, ok := config[NodeHost].(map[string]interface{})
	if !ok {
		LogWarning("Incorrect config format of node %v, rebuild it", NodeHost)
		node_config = make(map[string]interface{})
		config[NodeHost] = node_config
	}
	if _, ok := node_config[Config_Clusnode]; !ok {
		node_config[Config_Clusnode] = make(map[string]interface{})
	}
	clusnode_config, ok := node_config[Config_Clusnode].(map[string]interface{})
	if !ok {
		LogWarning("Incorrect clusnode config format of node %v, rebuild it", NodeHost)
		clusnode_config = make(map[string]interface{})
		node_config[Config_Clusnode] = clusnode_config
	}
	if _, ok := node_config[Config_Headnode]; !ok {
		node_config[Config_Headnode] = make(map[string]interface{})
	}
	headnode_config, ok := node_config[Config_Headnode].(map[string]interface{})
	if !ok {
		LogWarning("Incorrect headnode config format of node %v, rebuild it", NodeHost)
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
	for _, config := range configs_common {
		node_config[config.Name] = config.Value
	}

	// Save config file
	if err = saveConfigFile(config); err != nil {
		LogError("Failed to save config file: %v", err)
	}
}

func LoadNodeConfigs() {
	LogInfo("Loading node configs")
	config, err := readConfigFile()
	if err != nil {
		if os.IsNotExist(err) {
			LogInfo("Config file doesn't exist, use default configs")
		} else {
			LogFatality("Failed to parse config file: %v", err)
		}
		return
	}
	if _, ok := config[NodeHost]; !ok {
		LogWarning("No config loaded for node %v, use default configs", NodeHost)
		return
	}
	node_config, ok := config[NodeHost].(map[string]interface{})
	if !ok {
		LogWarning("Incorrect config format of node %v, use default configs", NodeHost)
		return
	}

	if clusnode_config, ok := node_config[Config_Clusnode].(map[string]interface{}); !ok {
		LogWarning("Incorrect config format for clusnode role of node %v, use default configs", NodeHost)
	} else {
		if headnodes, ok := clusnode_config[Config_Clusnode_Headnodes_Name].([]interface{}); !ok {
			LogWarning("Incorrect headnodes config format for clusnode role, skip")
		} else {
			LogInfo("Adding loaded headnodes: %v", headnodes)
			for _, headnode := range headnodes {
				if h, ok := headnode.(string); !ok {
					LogWarning("Headnode %v is not string format, skip", headnode)
				} else if _, err := AddHeadnode(h); err != nil {
					LogError("Failed to add headnode: %v", err)
				}
			}
		}
		for _, config := range configs_clusnode {
			if value, ok := clusnode_config[config.Name]; ok {
				if err := config.Set(value); err != nil {
					LogError("Failed to set %q for clusnode role to %v: %v", config.Name, value, err)
				}
			}
		}
	}
	if headnode_config, ok := node_config[Config_Headnode].(map[string]interface{}); !ok {
		LogWarning("Incorrect config format for headnode role of node %v, use default configs", NodeHost)
	} else {
		for _, config := range configs_headnode {
			if value, ok := headnode_config[config.Name]; ok {
				if err := config.Set(value); err != nil {
					LogError("Failed to set %q for headnode role to %v: %v", config.Name, value, err)
				}
			}
		}
	}
	for _, config := range configs_common {
		if value, ok := node_config[config.Name]; ok {
			if err := config.Set(value); err != nil {
				LogError("Failed to set %q to %v: %v", config.Name, value, err)
			}
		}
	}
}

func SetNodeConfigs(role string, configs map[string]string) map[string]string {
	LogInfo("SetConfigs: %v", configs)
	var configs_role map[string]*ConfigItem
	if role == Config_Clusnode {
		configs_role = configs_clusnode
	} else if role == Config_Headnode {
		configs_role = configs_headnode
	} else {
		panic(fmt.Sprintf("Invalid config role: %v", role))
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
	LogInfo("SetConfigs results: %v", results)
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
			configs["(connected) "+Config_Clusnode_Headnodes_Name] = strings.Join(connected, ", ")
		}
		if len(connecting) > 0 {
			configs["(connecting) "+Config_Clusnode_Headnodes_Name] = strings.Join(connecting, ", ")
		}
	} else if role == Config_Headnode {
		configs_role = configs_headnode
	} else {
		panic(fmt.Sprintf("Invalid config role: %v", role))
	}
	for _, config := range configs_role {
		configs[config.Name] = fmt.Sprintf("%v", config.Value)
	}
	LogInfo("GetConfigs results: %v", configs)
	return configs
}

func readConfigFile() (config map[string]interface{}, err error) {
	json_string, err := ioutil.ReadFile(NodeConfigFile)
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
	return ioutil.WriteFile(NodeConfigFile, json_string, 0644)
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
	LogInfo("Set config %q to %v", c.Name, v)
	return nil
}

func (c *ConfigItem) GetBool() (value bool) {
	if v, err := convertType(c.Value, reflect.Bool); err != nil {
		panic(err)
	} else {
		value = v.(bool)
	}
	return
}

func (c *ConfigItem) GetInt() (value int) {
	if v, err := convertType(c.Value, reflect.Int); err != nil {
		panic(err)
	} else {
		value = v.(int)
	}
	return
}

func convertType(from interface{}, t reflect.Kind) (to interface{}, err error) {
	err = fmt.Errorf("Failed to parse %v as type %v", from, t)
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
