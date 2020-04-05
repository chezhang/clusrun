package main

import (
	pb "../protobuf"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	JobId_All = 0
)

var (
	db_outputDir string
	db_cmdDir    string
	db_jobs      string
	db_jobsLock  sync.Mutex
)

func InitDatabase() {
	LogInfo("Initializing database")
	default_db_dir := ExecutablePath + ".db"
	headnode := filepath.Join(default_db_dir, strings.ReplaceAll(NodeHost, ":", "."))
	db_outputDir = headnode + ".output"
	db_cmdDir = headnode + ".command" // This directory is for clusnode not headnode, can be moved to other place when necessary
	db_jobs = headnode + ".jobs"
	if err := os.MkdirAll(db_outputDir, 0644); err != nil {
		LogFatality("Failed to create output dir: %v", err)
	}
	if err := os.MkdirAll(db_cmdDir, 0644); err != nil {
		LogFatality("Failed to create command dir for clusnode: %v", err)
	}
	if _, err := os.Stat(db_jobs); os.IsNotExist(err) {
		if err = ioutil.WriteFile(db_jobs, []byte("[]"), 0644); err != nil {
			LogFatality("Failed to create database jobs file: %v", err)
		}
	} else {
		// Cancel active jobs
		jobs, err := LoadJobs()
		if err != nil {
			LogFatality("Failed to load jobs: %v", err)
		}
		jobs_id := make(map[int32]bool, len(jobs))
		for i := range jobs {
			if IsActiveState(jobs[i].State) {
				jobs[i].State = pb.JobState_Canceling
				// TODO: add job to cancel list
			}
			jobs_id[jobs[i].Id] = true
		}
		if err := SaveJobs(jobs); err != nil {
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
				CleanupOutputDir(id)
			}
		}
	}
}

func CreateNewJob(command string, serial string, nodes []string) (int, error) {
	// Add new job in job list
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return -1, err
	}
	last_id := 0
	if len(jobs) > 0 {
		last_job := jobs[len(jobs)-1]
		last_id = int(last_job.Id)
	}
	var olds []int
	if jobs, olds, err = CleanupOldJobs(jobs); err != nil {
		return -1, err
	}
	new_id := last_id + 1
	new_job := pb.Job{
		Id:         int32(new_id),
		Command:    command,
		CreateTime: time.Now().Unix(),
		State:      pb.JobState_Created,
		Nodes:      nodes,
	}
	if len(serial) > 0 {
		new_job.Serial = serial
	}
	jobs = append(jobs, new_job)
	if err := SaveJobs(jobs); err != nil {
		return -1, err
	}

	// Cleanup output dir of old jobs
	for _, id := range olds {
		go CleanupOutputDir(id)
	}

	// Create output dir of new job
	if err := os.MkdirAll(GetOutputDir(new_id), 0644); err != nil {
		return -1, err
	}

	return new_id, nil
}

func CleanupOutputDir(job_id int) {
	LogInfo("Clean up output dir of job %v", job_id)
	if err := os.RemoveAll(GetOutputDir(job_id)); err != nil {
		LogWarning("Failed to cleanup output dir of job %v: %v", job_id, err)
	}
}

func CleanupOldJobs(jobs []pb.Job) ([]pb.Job, []int, error) {
	max_job_count := Config_Headnode_MaxJobCount.GetInt()
	active := []pb.Job{}
	to_clean := []int{}
	for remain := len(jobs) - max_job_count + 1; remain > 0; {
		if len(jobs) == 0 {
			message := fmt.Sprintf("Job count reaches the capacity %v and all %v jobs are active", max_job_count, len(active))
			return nil, nil, errors.New(message)
		}
		if IsActiveState(jobs[0].State) {
			active = append(active, jobs[0])
		} else {
			to_clean = append(to_clean, int(jobs[0].Id))
			remain--
		}
		jobs = jobs[1:]
	}
	if len(active) > 0 {
		jobs = append(active, jobs...)
	}
	return jobs, to_clean, nil
}

// TODO: Compress nodes
func SaveJobs(jobs []pb.Job) error {
	if json_string, err := json.MarshalIndent(jobs, "", "    "); err != nil {
		return err
	} else if err := ioutil.WriteFile(db_jobs, json_string, 0644); err != nil {
		return err
	}
	return nil
}

func LoadJobs() ([]pb.Job, error) {
	json_string, err := ioutil.ReadFile(db_jobs)
	if err != nil {
		return nil, err
	}
	var jobs []pb.Job
	if err = json.Unmarshal(json_string, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func IsActiveState(state pb.JobState) bool {
	return state == pb.JobState_Dispatching || state == pb.JobState_Running || state == pb.JobState_Canceling
}

func UpdateJobState(id int, from, to pb.JobState) error {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	for i := range jobs {
		if int(jobs[i].Id) == id {
			if from == jobs[i].State {
				jobs[i].State = to
			} else {
				LogWarning("Skip changing job %v state from %v to %v (Current state: %v)", id, from, to, jobs[i].State)
				return nil
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	LogInfo("Job %v state changed from %v to %v", id, from, to)
	return nil
}

func GetOutputDir(id int) string {
	return filepath.Join(db_outputDir, strconv.Itoa(id))
}

func GetOutputFile(id int, node string) (string, string) {
	file := filepath.Join(GetOutputDir(id), strings.ReplaceAll(node, ":", "."))
	return file + ".out", file + ".err"
}

func EndJob(id int, from, to pb.JobState) error {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	for i := range jobs {
		if int(jobs[i].Id) == id {
			jobs[i].EndTime = time.Now().Unix()
			if jobs[i].State == from {
				jobs[i].State = to
			} else {
				LogWarning("Skip changing job %v state from %v to %v (Current state: %v)", id, from, to, jobs[i].State)
				return nil
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	LogInfo("Job %v ended with state %v", id, to)
	return nil
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
	for i := range jobs {
		id := jobs[i].Id
		if _, ok := job_ids[id]; ok || cancel_all {
			if IsActiveState(jobs[i].State) {
				jobs[i].State = pb.JobState_Canceling
				to_cancel[id] = jobs[i].Nodes
			}
			result[id] = jobs[i].State
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return nil, nil, err
	}
	return result, to_cancel, nil
}

func UpdateCancelledJob(id int32, cancel_failed_nodes []string) {
	db_jobsLock.Lock()
	defer db_jobsLock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		LogError("Failed to load jobs for saving cancellation result of job %v: %v", id, err)
		return
	}
	for i := range jobs {
		if jobs[i].Id == id {
			if len(cancel_failed_nodes) == 0 {
				jobs[i].State = pb.JobState_Canceled
				LogInfo("Job %v is canceled", id)
			} else {
				jobs[i].State = pb.JobState_CancelFailed
				jobs[i].CancelFailedNodes = cancel_failed_nodes
				LogWarning("Cancellation of job %v failed for nodes: %v", id, cancel_failed_nodes)
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		LogError("Failed to save cancellation result of job %v: %v", id, err)
		return
	}
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

func NormalizeJobIds(job_ids map[int32]bool, jobs []pb.Job) map[int32]bool {
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
