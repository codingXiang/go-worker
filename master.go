package go_worker

import (
	"context"
	"errors"
	"github.com/codingXiang/go-orm/v2/mongo"
	"github.com/coreos/etcd/clientv3"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	cronV3 "github.com/robfig/cron/v3"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"
)

const (
	Now = "now"
)

const (
	IDENTITY          = "identity"
	NAMESPACE         = "namespace"
	JOB_NAME          = "jobName"
	STATUS            = "status"
	UPDATE            = "$set"
	STATUS_SCHEDULING = "scheduling"
	STATUS_REMOVE     = "remove"
	STATUS_PENDING    = "pending"
	STATUS_RUNNING    = "running"
	STATUS_COMPLETE   = "complete"
	STATUS_FAILED     = "failed"
	HISTORY           = "history"
)

//EnqueueEntity 實例
type EnqueueEntity struct {
	ID       string                 `json:"id"`
	Location *time.Location         `json:"-"`
	Cron     *cronV3.Cron           `json:"-"`
	Engine   *work.Enqueuer         `json:"-"`
	Spec     string                 `json:"Spec"`
	EntryID  cronV3.EntryID         `json:"EntryID"`
	JobName  string                 `json:"JobName"`
	Args     map[string]interface{} `json:"Args"`
}

//NewEnqueue 建立一個新的 Enqueue instance
func NewEnqueue(Engine *work.Enqueuer, location *time.Location, Spec, JobName string, Args map[string]interface{}) *EnqueueEntity {
	if Args == nil {
		Args = make(map[string]interface{})
	}

	e := &EnqueueEntity{
		ID:      uuid.NewV4().String(),
		Engine:  Engine,
		Spec:    Spec,
		JobName: JobName,
		Args:    Args,
	}

	options := []cronV3.Option{cronV3.WithSeconds()}
	if location != nil {
		e.Location = location
		options = append(options, cronV3.WithLocation(e.Location))
	}
	e.Cron = cronV3.New(options...)
	return e
}

func (g *EnqueueEntity) GetInstance() *EnqueueEntity {
	return g
}

func (g *EnqueueEntity) GetID() string {
	return g.ID
}

func (g *EnqueueEntity) GetEntryID() cronV3.EntryID {
	return g.EntryID
}

func (g *EnqueueEntity) SetEntryID(id cronV3.EntryID) *EnqueueEntity {
	g.EntryID = id
	return g
}

func (g *EnqueueEntity) GetSpec() string {
	return g.Spec
}

func (g *EnqueueEntity) SetSpec(Spec string) *EnqueueEntity {
	g.Spec = Spec
	return g
}

func (g *EnqueueEntity) GetJobName() string {
	return g.JobName
}

func (g *EnqueueEntity) SetJobName(JobName string) *EnqueueEntity {
	g.JobName = JobName
	return g
}

func (g *EnqueueEntity) GetArgs() map[string]interface{} {
	return g.Args
}

func (g *EnqueueEntity) AddArgs(key string, value interface{}) *EnqueueEntity {
	g.Args[key] = value
	return g
}

func (g *EnqueueEntity) RemoveArgs(key string) *EnqueueEntity {
	delete(g.Args, key)
	return g
}

func (g *EnqueueEntity) Do() (*work.Job, error) {
	return g.Engine.Enqueue(g.JobName, g.Args)
}

//Run 排程專用
func (g *EnqueueEntity) Run() {
	_, err := g.Do()
	if err != nil {
		panic(err)
	}
}

//Master 實例
type MasterEntity struct {
	id string
	//cron         *cronV3.Cron
	core         *work.Enqueuer
	workerClient *work.Client
	mongoClient  *mongo.Client
	tasks        map[string]*EnqueueEntity
	redisPool    *redis.Pool
	hostname     string
	namespace    string
	context      context.Context
}

type MasterOption struct {
	IsCluster   bool
	ETCDConfig  clientv3.Config
	MongoClient *mongo.Client
	BasePath    string
}

type Master interface {
	Init() Master
	GetID() string
	AddTask(info *TaskInfo) (*TaskInfo, error)
	GetEnqueues() map[string]*EnqueueEntity
	GetEnqueue(id string) (*EnqueueEntity, error)
	GetWorkerHeartbeats() ([]*work.WorkerPoolHeartbeat, error)
	GetBusyWorkers() ([]*work.WorkerObservation, error)
	GetQueues() ([]*work.Queue, error)
	ExecTask(id string) error
	WaitTask(id string, onChange func(data *mongo.RawData) (bool, error), onDelete func()) error
	RemoveTask(id string) error
	RemoveTaskRecord(id string) error
}

//NewMaster 建立 Master 實例
func NewMaster(pool *redis.Pool, namespace string, option *MasterOption) Master {
	master := &MasterEntity{
		id: uuid.NewV4().String(),
		//cron:         cronV3.New(cronV3.WithSeconds()),
		core:         work.NewEnqueuer(namespace, pool),
		workerClient: work.NewClient(namespace, pool),
		tasks:        make(map[string]*EnqueueEntity),
		namespace:    namespace,
		redisPool:    pool,
		context:      context.Background(),
	}
	hostname, err := os.Hostname()
	if err == nil {
		master.hostname = hostname
	} else {
		panic("can not get hostname, reason is " + err.Error())
		return nil
	}
	if option != nil {
		if option.MongoClient != nil {
			master.mongoClient = option.MongoClient
		}

		if option.IsCluster {
			return NewMasterCluster(master, option)
		} else {
			return master
		}
	} else {
		panic("master option can not be null")
		return nil
	}
}

func (g *MasterEntity) Init() Master {
	return g
}

func (g *MasterEntity) GetID() string {
	return g.id
}

//AddTask 加入任務
func (g *MasterEntity) AddTask(info *TaskInfo) (*TaskInfo, error) {
	if info.TimeZone == "" {
		info.TimeZone = "UTC"
	}

	var enqueue *EnqueueEntity

	if loc, err := time.LoadLocation(info.TimeZone); err == nil {
		enqueue = NewEnqueue(g.core, loc, info.Spec, info.JobName, info.Args)
	} else {
		NewEnqueue(g.core, nil, info.Spec, info.JobName, info.Args)
	}

	args := enqueue.GetArgs()
	args[mongo.IDENTITY] = enqueue.GetID()
	info.MasterID = g.GetID()
	info.ID = enqueue.GetID()

	if info.Spec != Now {
		if id, err := enqueue.Cron.AddJob(info.Spec, enqueue); err == nil {
			enqueue.SetEntryID(id)
		} else {
			return nil, err
		}
	}

	g.tasks[enqueue.GetID()] = enqueue

	//info := &TaskInfo{
	//	MasterID: g.GetID(),
	//	ID:       enqueue.GetID(),
	//	Spec:     enqueue.GetSpec(),
	//	JobName:  enqueue.GetJobName(),
	//	Args:     args,
	//}
	return info, g.mongoClient.C(g.namespace + "." + info.JobName).Insert(mongo.NewRawData(info.ID, bson.M{
		NAMESPACE: g.namespace,
		JOB_NAME:  info.JobName,
		STATUS:    STATUS_PENDING,
	}, info))
}

func (g *MasterEntity) GetEnqueues() map[string]*EnqueueEntity {
	return g.tasks
}

func (g *MasterEntity) GetEnqueue(id string) (*EnqueueEntity, error) {
	if enqueue, ok := g.tasks[id]; ok {
		return enqueue, nil
	} else {
		return nil, errors.New("task " + id + " is not exist")
	}
}

//GetWorkerHeartbeats 取得 worker heartbeats 陣列
func (g *MasterEntity) GetWorkerHeartbeats() ([]*work.WorkerPoolHeartbeat, error) {
	return g.workerClient.WorkerPoolHeartbeats()
}

func (g *MasterEntity) GetQueues() ([]*work.Queue, error) {
	return g.workerClient.Queues()
}

func (g *MasterEntity) GetBusyWorkers() ([]*work.WorkerObservation, error) {
	observations, err := g.workerClient.WorkerObservations()
	if err != nil {
		return nil, err
	}

	var busyObservations []*work.WorkerObservation
	for _, ob := range observations {
		if ob.IsBusy {
			busyObservations = append(busyObservations, ob)
		}
	}
	return busyObservations, nil
}

func (g *MasterEntity) updateTask(task *EnqueueEntity, status string) error {
	_, err := g.mongoClient.C(g.namespace+"."+task.GetJobName()).Update(bson.M{
		mongo.IDENTITY: task.GetID(),
	}, bson.M{
		UPDATE: bson.M{
			mongo.TAG: bson.M{
				NAMESPACE: g.namespace,
				JOB_NAME:  task.GetJobName(),
				STATUS:    status,
			},
		},
	})
	return err
}

//啟動排程
func (g *MasterEntity) ExecTask(id string) error {
	if task, ok := g.tasks[id]; ok {
		if task.GetSpec() == Now {
			task.Run()
			if err := g.updateTask(task, STATUS_RUNNING); err != nil {
				return err
			}
			return g.RemoveTask(task.GetID())
		} else {
			task.Cron.Start()
			return g.updateTask(task, STATUS_SCHEDULING)
		}
	} else {
		return errors.New("task " + id + " is not exist")
	}
}

//移除排程
func (g *MasterEntity) RemoveTask(id string) error {
	if task, ok := g.tasks[id]; ok {
		task.Cron.Stop()
		delete(g.tasks, id)
		task = nil
		return nil
	} else {
		return errors.New("task " + id + " is not exist")
	}
}

func (g *MasterEntity) RemoveTaskRecord(id string) error {
	if task, ok := g.tasks[id]; ok {
		//if task.GetSpec() != Now {
		//
		//	if EntryID := task.GetEntryID(); EntryID != 0 {
		//		task.Cron.Stop()
		//		defer delete(g.tasks, id)
		//		//g.cron.Remove(EntryID)
		//		//g.cron.Start()
		//	} else {
		//		return errors.New("task " + id + " is not execute")
		//	}
		//}

		task.Cron.Stop()
		delete(g.tasks, id)
		task = nil
		if err := g.updateTask(task, STATUS_REMOVE); err != nil {
			return err
		}
		return nil
	} else {
		return errors.New("task " + id + " is not exist")
	}
}

func (g *MasterEntity) WaitTask(id string, onChange func(data *mongo.RawData) (bool, error), onDelete func()) error {
	if task, ok := g.tasks[id]; ok {
		return g.mongoClient.WaitForChange(g.context, func() (*mongo.RawData, error) {
			return g.mongoClient.C(g.namespace + "." + task.GetJobName()).First(bson.M{
				mongo.IDENTITY: task.GetID(),
			})
		}, onChange, onDelete)
	}
	return errors.New("task " + id + " is not exist")
}
