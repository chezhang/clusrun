package main

import (
	"bytes"
	pb "clusrun/protobuf"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	JobId_All = 0
)

var (
	db_outputDir      string
	db_cmdDir         string
	db_jobs           string
	db_jobsLock       sync.Mutex
	db_nodeGroups     string
	db_nodeGroupsLock sync.Mutex
)

func InitDatabase() {
	LogInfo("Initializing database")
	default_db_dir := ExecutablePath + ".db"
	headnode := filepath.Join(default_db_dir, FileNameFormatHost(NodeHost))
	db_outputDir = headnode + ".output"
	db_cmdDir = headnode + ".command" // This directory is for clusnode not headnode, can be moved to other place when necessary
	db_jobs = headnode + ".jobs"
	db_nodeGroups = headnode + ".groups"
	if err := os.MkdirAll(db_outputDir, 0644); err != nil {
		LogFatality("Failed to create output dir: %v", err)
	}
	if err := os.MkdirAll(db_cmdDir, 0644); err != nil {
		LogFatality("Failed to create command dir for clusnode: %v", err)
	}
	if _, err := os.Stat(db_jobs); os.IsNotExist(err) {
		if err = saveJobs([]*pb.Job{}); err != nil {
			LogFatality("Failed to create database jobs file: %v", err)
		}
	} else {
		// Cancel active jobs
		jobs, err := LoadJobs()
		if err != nil {
			LogFatality("Failed to load jobs: %v", err)
		}
		jobs_id := make(map[int32]bool, len(jobs))
		for _, job := range jobs {
			if isActiveState(job.State) {
				job.State = pb.JobState_Canceling
				// TODO: add job to cancel list
			}
			jobs_id[job.Id] = true
		}
		if err := saveJobs(jobs); err != nil {
			LogFatality("Failed to save jobs: %v", err)
		}

		// Cleanup output dir
		output_dirs, err := ioutil.ReadDir(db_outputDir)
		if err != nil {
			LogFatality("Failed to read output dir: %v", err)
		}
		for _, f := range output_dirs {
			job_id := f.Name()
			if id, err := strconv.Atoi(job_id); err != nil || !f.IsDir() {
				LogFatality("Unexpected database item %v in %v", job_id, db_outputDir)
			} else if _, ok := jobs_id[int32(id)]; !ok {
				cleanupOutputDir(int32(id))
			}
		}
	}
	if _, err := os.Stat(db_nodeGroups); os.IsNotExist(err) {
		if err = ioutil.WriteFile(db_nodeGroups, []byte("{}"), 0644); err != nil {
			LogFatality("Failed to create database groups file: %v", err)
		}
	} else if err := loadNodeGroups(); err != nil {
		LogFatality("Failed to load node groups: %v", err)
	}
}

func CreateNewJob(command, sweep, pattern, name string, groups, specifiedNodes, nodes, args []string) (int32, error) {
	// Add new job in job list
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return -1, err
	}
	var last_id int32 = 0
	if len(jobs) > 0 {
		last_id = jobs[len(jobs)-1].Id
	}
	var olds []int32
	if jobs, olds, err = cleanupOldJobs(jobs); err != nil {
		return -1, err
	}
	new_id := last_id + 1
	new_job := &pb.Job{
		Id:             new_id,
		Command:        command,
		Sweep:          sweep,
		Arguments:      args,
		CreateTime:     time.Now().Unix(),
		State:          pb.JobState_Created,
		SpecifiedNodes: specifiedNodes,
		NodePattern:    pattern,
		NodeGroups:     groups,
		Nodes:          nodes,
		Name:           name,
	}
	jobs = append(jobs, new_job)
	if err := saveJobs(jobs); err != nil {
		return -1, err
	}

	// Cleanup output dir of old jobs
	for _, id := range olds {
		go cleanupOutputDir(id)
	}

	// Create output dir of new job
	if err := os.MkdirAll(getOutputDir(new_id), 0644); err != nil {
		return -1, err
	}

	return new_id, nil
}

func cleanupOutputDir(job_id int32) {
	LogInfo("Clean up output dir of job %v", job_id)
	if err := os.RemoveAll(getOutputDir(job_id)); err != nil {
		LogWarning("Failed to cleanup output dir of job %v: %v", job_id, err)
	}
}

func cleanupOldJobs(jobs []*pb.Job) ([]*pb.Job, []int32, error) {
	max_job_count := Config_Headnode_MaxJobCount.GetInt()
	var active []*pb.Job
	var to_clean []int32
	for remain := len(jobs) - max_job_count + 1; remain > 0; {
		if len(jobs) == 0 {
			message := fmt.Sprintf("Job count reaches the capacity %v and all %v jobs are active", max_job_count, len(active))
			return nil, nil, errors.New(message)
		}
		if isActiveState(jobs[0].State) {
			active = append(active, jobs[0])
		} else {
			to_clean = append(to_clean, jobs[0].Id)
			remain--
		}
		jobs = jobs[1:]
	}
	if len(active) > 0 {
		jobs = append(active, jobs...)
	}
	return jobs, to_clean, nil
}

func saveJobs(jobs []*pb.Job) error {
	j, err := json.MarshalIndent(jobs, "", "    ")
	if err != nil {
		return err
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(j); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := ioutil.WriteFile(db_jobs, b.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}

func LoadJobs() ([]*pb.Job, error) {
	b, err := ioutil.ReadFile(db_jobs)
	if err != nil {
		return nil, err
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	j, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var jobs []*pb.Job
	if err = json.Unmarshal(j, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func isActiveState(state pb.JobState) bool {
	return state == pb.JobState_Dispatching || state == pb.JobState_Running || state == pb.JobState_Canceling
}

func UpdateJobState(id int32, from, to pb.JobState) error {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Id == id {
			if from == job.State {
				job.State = to
			} else {
				LogWarning("Skip changing job %v state from %v to %v (Current state: %v)", id, from, to, job.State)
				return nil
			}
			break
		}
	}
	if err := saveJobs(jobs); err != nil {
		return err
	}
	LogInfo("Job %v state changed from %v to %v", id, from, to)
	return nil
}

func UpdateFinishedJob(id int32) {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		LogError("Failed to load jobs when finishing job %v: %v", id, err)
		return
	}
	for _, job := range jobs {
		if job.Id == id {
			if job.State == pb.JobState_Running {
				job.EndTime = time.Now().Unix()
				job.State = pb.JobState_Finished
			}
			break
		}
	}
	if err := saveJobs(jobs); err != nil {
		LogError("Failed to save jobs when finishing job %v: %v", id, err)
		return
	}
	LogInfo("Job %v finished", id)
}

func UpdateFailedJob(id int32, exitCodes map[string]int32) {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		LogError("Failed to load jobs when failing job %v: %v", id, err)
		return
	}
	for _, job := range jobs {
		if job.Id == id {
			if job.State == pb.JobState_Running {
				job.EndTime = time.Now().Unix()
				job.State = pb.JobState_Failed
			}
			job.FailedNodes = exitCodes
			break
		}
	}
	if err := saveJobs(jobs); err != nil {
		LogError("Failed to save jobs when failing job %v: %v", id, err)
		return
	}
	LogInfo("Job %v failed", id)
}

func CancelJobs(job_ids map[int32]bool) (map[int32]pb.JobState, map[int32][]string, error) {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return nil, nil, err
	}
	job_ids = NormalizeJobIds(job_ids, jobs)
	cancel_all := false
	if _, ok := job_ids[JobId_All]; ok {
		cancel_all = true
	}
	result := map[int32]pb.JobState{}
	to_cancel := map[int32][]string{}
	for _, job := range jobs {
		id := job.Id
		if _, ok := job_ids[id]; ok || cancel_all {
			if isActiveState(job.State) {
				job.State = pb.JobState_Canceling
				to_cancel[id] = job.Nodes
			}
			result[id] = job.State
		}
	}
	if err := saveJobs(jobs); err != nil {
		return nil, nil, err
	}
	return result, to_cancel, nil
}

func UpdateCancelledJob(id int32, cancel_failed_nodes []string) {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		LogError("Failed to load jobs when cancelling job %v: %v", id, err)
		return
	}
	for _, job := range jobs {
		if job.Id == id {
			job.EndTime = time.Now().Unix()
			if len(cancel_failed_nodes) == 0 {
				job.State = pb.JobState_Canceled
			} else {
				job.State = pb.JobState_CancelFailed
				job.CancelFailedNodes = cancel_failed_nodes
				LogWarning("Cancellation of job %v failed on nodes: %v", id, cancel_failed_nodes)
			}
			break
		}
	}
	if err := saveJobs(jobs); err != nil {
		LogError("Failed to save jobs when cancelling job %v: %v", id, err)
		return
	}
	LogInfo("Job %v cancelled", id)
}

func CreateCommandFile(job_label, command string) (string, error) {
	file := filepath.Join(db_cmdDir, job_label)
	if RunOnWindows {
		file += ".cmd"
	} else {
		file += ".sh"
	}
	LogInfo("Create file %v", file)
	if err := ioutil.WriteFile(file, []byte(command), 0644); err != nil {
		return file, err
	}
	return file, nil
}

func getOutputDir(id int32) string {
	return filepath.Join(db_outputDir, strconv.Itoa(int(id)))
}

func GetOutputFile(id int32, node string) (string, string) {
	file := filepath.Join(getOutputDir(id), FileNameFormatHost(node))
	return file + ".out", file + ".err"
}

func NormalizeJobIds(job_ids map[int32]bool, jobs []*pb.Job) map[int32]bool {
	var last_job_id int32
	if len(jobs) > 0 {
		last_job_id = jobs[len(jobs)-1].Id
	}
	positive_job_ids := map[int32]bool{}
	for id, val := range job_ids {
		if id < 0 {
			positive_job_ids[id+last_job_id+1] = val
		} else {
			positive_job_ids[id] = val
		}
	}
	return positive_job_ids
}

func SaveNodeGroups() error {
	db_nodeGroupsLock.Lock()
	defer db_nodeGroupsLock.Unlock()
	groups := map[string][]string{}
	NodeGroups.Range(func(k, v interface{}) bool {
		group := k.(string)
		nodes := v.(*sync.Map)
		var n []string
		nodes.Range(func(k, v interface{}) bool {
			node := k.(string)
			n = append(n, node)
			return true
		})
		groups[group] = n
		return true
	})
	if json_string, err := json.MarshalIndent(groups, "", "    "); err != nil {
		return err
	} else if err := ioutil.WriteFile(db_nodeGroups, json_string, 0644); err != nil {
		return err
	}
	return nil
}

func loadNodeGroups() error {
	db_nodeGroupsLock.Lock()
	defer db_nodeGroupsLock.Unlock()
	json_string, err := ioutil.ReadFile(db_nodeGroups)
	if err != nil {
		return err
	}
	var nodeGroups map[string][]string
	if err = json.Unmarshal(json_string, &nodeGroups); err != nil {
		return err
	}
	for k, v := range nodeGroups {
		nodes := sync.Map{}
		for _, node := range v {
			nodes.Store(node, false)
		}
		NodeGroups.Store(k, &nodes)
	}
	return nil
}
