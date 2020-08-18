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
		BasePath:  "worker",
		ETCDConfig: clientv3.Config{
			Endpoints:   []string{"localhost:32773", "localhost:32769", "localhost:32772"},
			DialTimeout: 5 * time.Second,
		},
	})
	//initialize master settings
	master.Init()
	//add task with cron spec
	task, _ := master.AddTask("now", "send_email", work.Q{"address": "test@example.com", "subject": "hello world", "customer_id": 4})
	//exec task by id
	master.ExecTask(task.ID)
	select {}
}
