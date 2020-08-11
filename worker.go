package go_worker

import (
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

type WorkerEntity struct {
	pool *work.WorkerPool
}

type Job interface {
	Do(job *work.Job) error
}

type Worker interface {
	AddJob(name string, job Job, option *work.JobOptions) Worker
	Start() Worker
	Stop() Worker
}

func NewWorker(concurrency uint, namespace string, pool *redis.Pool) Worker {
	return &WorkerEntity{
		pool: work.NewWorkerPool(WorkerEntity{}, concurrency, namespace, pool),
	}
}

func (g *WorkerEntity) AddJob(name string, job Job, option *work.JobOptions) Worker {
	if option == nil {
		g.pool.Job(name, job.Do)
	} else {
		g.pool.JobWithOptions(name, *option, job.Do)
	}
	return g
}

func (g *WorkerEntity) Start() Worker {
	g.pool.Start()
	return g
}

func (g *WorkerEntity) Stop() Worker {
	g.pool.Stop()
	return g
}
