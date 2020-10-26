package model

import (
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
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

type ServiceEntity struct {
	taskName string
	config   *viper.Viper
}

func NewService(taskName string, config *viper.Viper) Service {
	return &ServiceEntity{
		taskName: taskName,
		config:   config,
	}
}
func (g *ServiceEntity) Do(job *work.Job) error {
	return nil
}

func (g *ServiceEntity) GetRegisterInfo() *JobInfo {
	return &JobInfo{
		Name: g.taskName,
		Job:  g,
	}
}
