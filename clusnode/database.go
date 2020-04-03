package main

import (
	pb "../protobuf"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Label_Last_Job = -1
	Label_All_Jobs = -2
)

var (
	db_output_dir string
	db_cmd_dir    string
	db_jobs       string
	db_jobs_lock  sync.Mutex
)

func InitDatabase() {
	default_db_dir := executable_path + ".db"
	headnode := filepath.Join(default_db_dir, strings.ReplaceAll(clusnode_host, ":", "."))
	db_output_dir = headnode + ".output"
	db_cmd_dir = headnode + ".command" // This directory is for clusnode not headnode, can be moved to other place when necessary
	db_jobs = headnode + ".jobs"
	if err := os.MkdirAll(db_output_dir, 0644); err != nil {
		log.Fatalf("Error creating database dir: %v", err)
	}
	if err := os.MkdirAll(db_cmd_dir, 0644); err != nil {
		log.Fatalf("Error creating database dir: %v", err)
	}
	if _, err := os.Stat(db_jobs); os.IsNotExist(err) {
		if err = ioutil.WriteFile(db_jobs, []byte("[]"), 0644); err != nil {
			log.Fatalf("Error creating database file: %v", err)
		}
	} else {
		// Cancel active jobs
		jobs, err := LoadJobs()
		if err != nil {
			log.Fatalf("Error loading database file: %v", err)
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
			log.Fatalf("Error saving database file: %v", err)
		}

		// Cleanup output dir
		output_dirs, err := ioutil.ReadDir(db_output_dir)
		if err != nil {
			log.Fatalf("Error listing database dir: %v", err)
		}
		for _, f := range output_dirs {
			job_id := f.Name()
			if id, err := strconv.Atoi(job_id); err != nil || !f.IsDir() {
				log.Fatalf("Unexpected database item %v in %v", job_id, db_output_dir)
			} else if _, ok := jobs_id[int32(id)]; !ok {
				CleanupOutputDir(id)
			}
		}
	}
}

func CreateNewJob(command string, nodes []string) (int, error) {
	// Add new job in job list
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
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
	log.Printf("Clean up output dir of job %v", job_id)
	if err := os.RemoveAll(GetOutputDir(job_id)); err != nil {
		log.Printf("Failed to cleanup output dir of job %v: %v", job_id, err)
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
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	for i := range jobs {
		if int(jobs[i].Id) == id {
			if from == jobs[i].State {
				jobs[i].State = to
			} else {
				log.Printf("Skip changing job %v state from %v to %v (Current state: %v)", id, from, to, jobs[i].State)
				return nil
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	log.Printf("Job %v state changed from %v to %v", id, from, to)
	return nil
}

func GetOutputDir(id int) string {
	return filepath.Join(db_output_dir, strconv.Itoa(id))
}

func GetOutputFile(id int, node string) (string, string) {
	file := filepath.Join(GetOutputDir(id), strings.ReplaceAll(node, ":", "."))
	return file + ".out", file + ".err"
}

func EndJob(id int, from, to pb.JobState) error {
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
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
				log.Printf("Skip changing job %v state from %v to %v (Current state: %v)", id, from, to, jobs[i].State)
				return nil
			}
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	log.Printf("Job %v ended with state %v", id, to)
	return nil
}

func CancelJobs(job_ids map[int32]bool) (map[int32]pb.JobState, map[int32][]string, error) {
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return nil, nil, err
	}
	cancel_all := false
	if _, ok := job_ids[Label_Last_Job]; ok && len(jobs) > 0 {
		job_ids = map[int32]bool{jobs[len(jobs)-1].Id: false}
	} else if _, ok := job_ids[Label_All_Jobs]; ok {
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
