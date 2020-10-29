package model

import (
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
)

type Status int

const (
	Pending Status = iota
	Running
	Failed
	Succeeded
)

func (p Status) String() string {
	switch p {
	case Pending:
		return "Pending"
	case Running:
		return "Running"
	case Failed:
		return "Failed"
	case Succeeded:
		return "Succeeded"
	default:
		return "Unknown"
	}
}

type CallbackSender struct {
	CallbackReceiver
	Namespace string `json:"namespace"`
}

type CallbackReceiver struct {
	Status     string                 `json:"status"`
	IsComplete bool                   `json:"isComplete"`
	Args       map[string]interface{} `json:"args"`
}

type JobInfo struct {
	Name   string
	Job    go_worker.Job
	Option *work.JobOptions
}

type Service interface {
	GetRegisterInfo() *JobInfo
	Do(job *work.Job) error
	GetTaskName() string
	GetConfig() *viper.Viper
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

func (g *ServiceEntity) GetTaskName() string {
	return g.taskName
}

func (g *ServiceEntity) GetConfig() *viper.Viper {
	return g.config
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
