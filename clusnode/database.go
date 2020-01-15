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

var (
	db_output_dir    string
	db_cmd_dir       string
	db_jobs          string
	db_jobs_lock     sync.Mutex
	db_max_job_count int
)

func InitDatabase() {
	default_db_dir := os.Args[0] + ".db"
	db_max_job_count = 3 // TODO: get from config
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

	// Create output dir of new job
	if err := os.MkdirAll(GetOutputDir(new_id), 0644); err != nil {
		return -1, err
	}

	// Cleanup output dir of old jobs
	for _, id := range olds {
		go CleanupOutputDir(id)
	}
	return new_id, nil
}

func CleanupOutputDir(job_id int) {
	if err := os.RemoveAll(GetOutputDir(job_id)); err != nil {
		log.Printf("Failed to cleanup output dir of job %v: %v", job_id, err)
	} else {
		log.Printf("Cleaned up output dir of job %v", job_id)
	}
}

func CleanupOldJobs(jobs []pb.Job) ([]pb.Job, []int, error) {
	active := []pb.Job{}
	to_clean := []int{}
	for remain := len(jobs) - db_max_job_count + 1; remain > 0; {
		if len(jobs) == 0 {
			message := fmt.Sprintf("Job count reaches the capacity %v and all %v jobs are active", db_max_job_count, len(active))
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

func UpdateJobState(id int, state pb.JobState) error {
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	var previous pb.JobState
	for i := range jobs {
		if int(jobs[i].Id) == id {
			previous = jobs[i].State
			jobs[i].State = state
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	log.Printf("Job %v state changed from %v to %v", id, previous, state)
	return nil
}

func GetOutputDir(id int) string {
	return filepath.Join(db_output_dir, strconv.Itoa(id))
}

func GetOutputFile(id int, node string) (string, string) {
	file := filepath.Join(GetOutputDir(id), strings.ReplaceAll(node, ":", "."))
	return file + ".out", file + ".err"
}

func EndJob(id int, state pb.JobState) error {
	db_jobs_lock.Lock()
	defer db_jobs_lock.Unlock()
	jobs, err := LoadJobs()
	if err != nil {
		return err
	}
	for i := range jobs {
		if int(jobs[i].Id) == id {
			jobs[i].EndTime = time.Now().Unix()
			jobs[i].State = state
			break
		}
	}
	if err := SaveJobs(jobs); err != nil {
		return err
	}
	log.Printf("Job %v ended with state %v", id, state)
	return nil
}
