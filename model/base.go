package model

import (
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
)

type JobInfo struct {
	Name   string
	Job    go_worker.Job
	Option *work.JobOptions
}

type Service interface {
	GetRegisterInfo() *JobInfo
	Do(job *work.Job) error
}
