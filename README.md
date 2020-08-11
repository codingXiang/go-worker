# codingXiang/go-worker

## Introduction
go-worker is a `distributed task system` for `Go` which implement master-worker architecture, there are few feature in this project:
- high available architecture
- schedule task with [cron expression](https://godoc.org/github.com/robfig/cron#hdr-CRON_Expression_Format)
- dynamic task assign
- simple restful api
- command line interface

## Architecture
### Single Master
![](https://i.imgur.com/Flkc5a6.png)


### Multiple Master
![](https://i.imgur.com/AeJlZVz.png)


## install
```
go get github.com/codingXiang/go-worker
```

## Usage
### master
```go
package main

import (
	"github.com/codingXiang/go-worker"
	"github.com/coreos/etcd/clientv3"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"time"
)

func main() {
    //make a redis connection pool
    redisPool := &redis.Pool{
        MaxActive: 5,
        MaxIdle:   5,
        Wait:      true,
        Dial: func() (redis.Conn, error) {
            return redis.Dial("tcp", "127.0.0.1:6379", redis.DialPassword("a12345"))
        },
    }
    //create single master node
    //master := go_worker.NewMaster(redisPool, "demo", &go_worker.MasterOption{
    //	IsCluster: false,
    //})
    
    //create multiple master node with options
    master := go_worker.NewMaster(redisPool, "demo", &go_worker.MasterOption{
        IsCluster: true,
        ETCDConfig: clientv3.Config{
            Endpoints:   []string{"localhost:32773", "localhost:32769", "localhost:32772"},
            DialTimeout: 5 * time.Second,
        },
    })
    //initialize master settings
    master.Init()
    //add task with cron spec
    id, _ := master.AddTask("*/3 * * * * *", "send_email", work.Q{"address": "test@example.com", "subject": "hello world", "customer_id": 4})
    //exec task by id
    master.ExecTask(id)
    select {}
}

```

### worker
```go
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


```



## Dependencies
- [gocraft/work](https://github.com/gocraft/work)
- [robfig/cron](https://github.com/robfig/cron)