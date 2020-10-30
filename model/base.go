package model

import (
	"encoding/json"
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
	Identity  string `json:"identity"`
}

func (s *CallbackSender) InterfaceToObject(in interface{}) (*CallbackSender, error) {
	var err error
	args, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(args, &s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

type CallbackReceiver struct {
	IdentityID string                 `json:"identityId"`
	Status     string                 `json:"status"`
	IsComplete bool                   `json:"isComplete"`
	Body       string                 `json:"body"`
	ErrMsg     string                 `json:"errMsg"`
	Args       map[string]interface{} `json:"args"`
}

func (s *CallbackReceiver) InterfaceToObject(in interface{}) (*CallbackReceiver, error) {
	var err error
	args, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(args, &s)
	if err != nil {
		return nil, err
	}
	return s, nil
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
	TaskName string
	Config   *viper.Viper
}

func NewService(TaskName string, Config *viper.Viper) *ServiceEntity {
	return &ServiceEntity{
		TaskName: TaskName,
		Config:   Config,
	}
}

func (g *ServiceEntity) GetTaskName() string {
	return g.TaskName
}

func (g *ServiceEntity) GetConfig() *viper.Viper {
	return g.Config
}

func (g *ServiceEntity) Do(job *work.Job) error {
	return nil
}

func (g *ServiceEntity) GetRegisterInfo() *JobInfo {
	return &JobInfo{
		Name: g.TaskName,
		Job:  g,
	}
}
