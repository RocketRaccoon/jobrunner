// A job runner for executing scheduled or ad-hoc tasks asynchronously from HTTP requests.
//
// It adds a couple of features on top of the Robfig cron package:
// 1. Protection against job panics.  (They print to ERROR instead of take down the process)
// 2. (Optional) Limit on the number of jobs that may run simulatenously, to
//    limit resource consumption.
// 3. (Optional) Protection against multiple instances of a single job running
//    concurrently.  If one execution runs into the next, the next will be queued.
// 4. Cron expressions may be defined in app.conf and are reusable across jobs.
// 5. Job status reporting. [WIP]
package jobrunner

import (
	"log"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Callers can use jobs.Func to wrap a raw func.
// (Copying the type to this package makes it more visible)
//
// For example:
//    jobrunner.Schedule("cron.frequent", jobs.Func(myFunc))
type Func func()

func (r Func) Run() { r() }

const (
	every       = "@every "
	emptyString = ""
	now         = "now"
	in          = "in "
)

func Schedule(spec string, job cron.Job, jobName string, obj []byte) (*Job, error) {
	if strings.HasPrefix(spec, every) {
		d, err := time.ParseDuration(strings.ReplaceAll(spec, every, emptyString))
		if err != nil {
			log.Panic(err)
		}
		return Every(d, job, jobName, obj), nil
	} else if strings.HasPrefix(spec, now) {
		return Now(job, jobName, obj), nil
	} else if strings.HasPrefix(spec, in) {
		d, err := time.ParseDuration(strings.ReplaceAll(spec, every, emptyString))
		if err != nil {
			log.Panic(err)
		}
		return In(d, job, jobName, obj), nil
	}
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, err
	}
	j := New(job, jobName, obj, spec)
	MainCron.Schedule(schedule, j)
	return j, nil
}

// Run the given job at a fixed interval.
// The interval provided is the time between the job ending and the job being run again.
// The time that the job takes to run is not included in the interval.
func Every(duration time.Duration, job cron.Job, jobName string, obj []byte) *Job {
	spec := every + duration.String()
	j := New(job, jobName, obj, spec)
	MainCron.Schedule(cron.Every(duration), j)
	return j
}

// Run the given job right now.
func Now(job cron.Job, jobName string, obj []byte) *Job {
	spec := "now"
	j := New(job, jobName, obj, spec)
	go j.Run()
	return j
}

// Run the given job once, after the given delay.
func In(duration time.Duration, job cron.Job, jobName string, obj []byte) *Job {
	spec := "in " + duration.String()
	j := New(job, jobName, obj, spec)
	go func() {
		time.Sleep(duration)
		j.Run()
	}()
	return j
}
