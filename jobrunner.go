package jobrunner

import (
	"bytes"
	"log"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

type Job struct {
	Name    string     `json:"name"`
	Outer   []byte     `json:"outer"`
	inner   cron.Job   `json:"inner"`
	status  uint32     `json:"statusCode,omitempty"`
	Status  string     `json:"status,omitempty"`
	Latency string     `json:"latency,omitempty"`
	running sync.Mutex `json:"running,omitempty"`
	Spec    string     `json:"spec"`
}

const UNNAMED = "(unnamed)"

func New(job cron.Job, jobName string, obj []byte, spec string) *Job {
	var name string
	if len(jobName) > 0 {
		name = jobName
	} else {
		name = reflect.TypeOf(job).Name()
	}
	if name == "Func" {
		name = UNNAMED
	}
	return &Job{
		Name:  name,
		inner: job,
		Outer: obj,
		Spec:  spec,
	}
}

func (j *Job) StatusUpdate() string {
	if atomic.LoadUint32(&j.status) > 0 {
		j.Status = "RUNNING"
		return j.Status
	}
	j.Status = "IDLE"
	return j.Status

}

func (j *Job) Run() {
	start := time.Now()
	// If the job panics, just print a stack trace.
	// Don't let the whole process die.
	defer func() {
		if err := recover(); err != nil {
			var buf bytes.Buffer
			logger := log.New(&buf, "JobRunner Log: ", log.Lshortfile)
			logger.Panic(err, "\n", string(debug.Stack()))
		}
	}()

	if !selfConcurrent {
		j.running.Lock()
		defer j.running.Unlock()
	}

	if workPermits != nil {
		workPermits <- struct{}{}
		defer func() { <-workPermits }()
	}

	atomic.StoreUint32(&j.status, 1)
	j.StatusUpdate()

	defer j.StatusUpdate()
	defer atomic.StoreUint32(&j.status, 0)

	j.inner.Run()

	end := time.Now()
	j.Latency = end.Sub(start).String()

}
