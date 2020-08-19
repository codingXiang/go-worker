package main

import (
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"log"
)

//define a custom job
type CustomJob struct {
	customerID int64
	Pool       *work.WorkerPool
	Cloud      string `json:"cloud"`
	Area       string `json:"area"`
	Namespace  string `json:"namespace"`
	Spec       string `json:"spec"`
}

//custom must to implement `Do(job *work.Job) error` function
func (c *CustomJob) Do(job *work.Job) error {
	log.Println(job.Name)
	return nil
}


func main() {
	//make a redis connection pool
	redisPool := &redis.Pool{
		MaxActive: 5,
		MaxIdle:   5,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", ":6379", redis.DialPassword("a12345"))
		},
	}
	//make a worker instance
	worker := go_worker.NewWorker(10, "demo", redisPool)
	//make a CustomJob instance
	customJob := &CustomJob{}
	//define job to worker with name, instance and options
	worker.AddJob("send_email", customJob, nil)
	//start worker
	worker.Start()
	select {}
}

