package core

import (
	go_worker "github.com/codingXiang/go-worker"
	"github.com/codingXiang/go-worker/model"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
)

type WorkerCoreEntity struct {
	Core      go_worker.Worker
	Namespace string
	Jobs      []*model.JobInfo
}

type WorkerCore interface {
	AddJob(services ...model.Service) WorkerCore
	Start()
}

func NewWorkerCore(config *viper.Viper) WorkerCore {
	redisPool := &redis.Pool{
		MaxActive: config.GetInt("redis.maxActive"),
		MaxIdle:   config.GetInt("redis.maxIdle"),
		Wait:      config.GetBool("redis.wait"),
		Dial: func() (redis.Conn, error) {
			return redis.Dial(config.GetString("redis.network"), config.GetString("redis.url")+":"+config.GetString("redis.port"), redis.DialPassword(config.GetString("redis.password")))
		},
	}
	//make a worker instance
	worker := go_worker.NewWorker(config.GetUint("worker.concurrency"), config.GetString("worker.namespace"), redisPool)
	return &WorkerCoreEntity{
		Core:      worker,
		Namespace: config.GetString("worker.namespace"),
		Jobs:      make([]*model.JobInfo, 0),
	}
}

func (g *WorkerCoreEntity) AddJob(services ...model.Service) WorkerCore {
	for _, service := range services {
		g.Jobs = append(g.Jobs, service.GetRegisterInfo())
	}
	return g
}

func (g *WorkerCoreEntity) Start() {
	for _, job := range g.Jobs {
		g.Core.AddJob(job.Name, job.Job, job.Option)
	}
	g.Core.Start()
}
