package model

import (
	"encoding/json"
	"github.com/codingXiang/go-orm/v2/mongo"
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2/bson"
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

type BaseJobData struct {
	Identity string `json:"identity"`
}

type Service interface {
	GetRegisterInfo() *JobInfo
	Do(job *work.Job) error
	GetTaskName() string
	GetConfig() *viper.Viper
	GetMongoClient() *mongo.Client
}

type ServiceEntity struct {
	TaskName    string
	Config      *viper.Viper
	MongoClient *mongo.Client
}

func NewService(TaskName string, Config *viper.Viper) *ServiceEntity {
	return &ServiceEntity{
		TaskName:    TaskName,
		Config:      Config,
		MongoClient: mongo.New(Config),
	}
}

func Callback(g Service, identity string, err error) error {
	status := go_worker.STATUS_COMPLETE
	if err != nil {
		status = go_worker.STATUS_FAILED
	}
	_, err1 := g.GetMongoClient().C(g.GetTaskName()).Update(bson.M{
		mongo.TAG: bson.M{
			mongo.IDENTITY: identity,
		},
	}, bson.M{
		go_worker.UPDATE: bson.M{
			mongo.TAG: bson.M{
				go_worker.STATUS: status,
			}, "fail_reason": err.Error(),
		},
	})
	if err != nil {
		return err
	}
	if err1 != nil {
		return err1
	}
	return nil
}

func (g *ServiceEntity) GetTaskName() string {
	return g.TaskName
}

func (g *ServiceEntity) GetConfig() *viper.Viper {
	return g.Config
}

func (g *ServiceEntity) GetMongoClient() *mongo.Client {
	return g.MongoClient
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
