package model

import (
	"encoding/json"
	"github.com/codingXiang/go-orm/v2/mongo"
	go_worker "github.com/codingXiang/go-worker"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2/bson"
	"time"
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

type ExecHistory struct {
	Status     string    `json:"history"`
	Message    string    `json:"message"`
	CostTime   int64     `json:"costTime"`
	CompleteAt time.Time `json:"completeAt"`
}

type CostTime struct {
	Start time.Time
	Since time.Duration
}

func NewCostTime() *CostTime {
	return &CostTime{
		Start: time.Now(),
	}
}

func (c *CostTime) GetMillionSecond() int64 {
	return int64(time.Since(c.Start) / time.Millisecond)
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

func get(client *mongo.Client, namespace, taskName, identity string) (*go_worker.TaskInfo, error) {
	if data, err := client.C(namespace + "." + taskName).First(bson.M{
		mongo.IDENTITY: identity,
	}); err != nil {
		return nil, err
	} else {
		tmp, err := json.Marshal(data.Raw)
		if err != nil {
			return nil, err
		}
		result := new(go_worker.TaskInfo)
		err = json.Unmarshal(tmp, &result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
}

func update(client *mongo.Client, namespace, taskName, identity string, costTime int64, err error) error {
	if data, e := client.C(namespace + "." + taskName).First(bson.M{
		mongo.IDENTITY: identity,
	}); e != nil {
		return e
	} else {
		tag := data.Tag
		tag[go_worker.STATUS] = go_worker.STATUS_COMPLETE
		if err != nil {
			tag[go_worker.STATUS] = go_worker.STATUS_FAILED
		}
		data.Tag = tag
		if _, err1 := client.C(namespace+"."+taskName).Update(bson.M{
			mongo.IDENTITY: identity,
		}, data); err1 != nil {
			return err1
		} else {
			addBuildHistory(client, data, namespace, taskName, costTime, err)
		}
	}
	return err
}

func addBuildHistory(client *mongo.Client, data *mongo.RawData, namespace, taskName string, costTime int64, err error) error {
	build := &ExecHistory{
		Status:     go_worker.STATUS_COMPLETE,
		Message:    "執行完成",
		CostTime:   costTime,
		CompleteAt: time.Now(),
	}
	if err != nil {
		build.Status = go_worker.STATUS_FAILED
		build.Message = err.Error()
	}

	if err1 := client.C(namespace + "." + taskName + "." + go_worker.HISTORY).Insert(mongo.NewRawData("", map[string]interface{}{
		mongo.IDENTITY: data.Identity,
	}, build)); err1 != nil {
		return err1
	}

	return err
}

func EnableExecute(g Service, namespace, identity string) bool {
	info, _ := get(g.GetMongoClient(), namespace, g.GetTaskName(), identity)
	if info.Spec == "now" {
		return true
	}
	if info.Active {
		if info.DisableTimeRange != nil {
			now := time.Now()
			if now.Before(info.DisableTimeRange.Start) && now.After(info.DisableTimeRange.End) {
				return true
			}
			return false
		} else {
			return true
		}
	} else {
		return false
	}
}

func Callback(g Service, namespace string, identity string, costTime *CostTime, err error) error {
	err = update(g.GetMongoClient(), namespace, g.GetTaskName(), identity, costTime.GetMillionSecond(), err)
	return err
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
