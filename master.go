package go_worker

import (
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	cronV3 "github.com/robfig/cron/v3"
	uuid "github.com/satori/go.uuid"
	"os"
)

//EnqueueEntity 實例
type EnqueueEntity struct {
	ID      string                 `json:"id"`
	Engine  *work.Enqueuer         `json:"-"`
	Spec    string                 `json:"Spec"`
	EntryID cronV3.EntryID         `json:"EntryID"`
	JobName string                 `json:"JobName"`
	Args    map[string]interface{} `json:"Args"`
}

//Enqueue 封裝 EnqueueEntity 方法的 interface
type Enqueue interface {
	GetInstance() *EnqueueEntity
	GetID() string
	GetEntryID() cronV3.EntryID
	SetEntryID(id cronV3.EntryID) Enqueue
	GetSpec() string
	SetSpec(string) Enqueue
	GetJobName() string
	SetJobName(string) Enqueue
	GetArgs() map[string]interface{}
	AddArgs(key string, value interface{}) Enqueue
	RemoveArgs(key string) Enqueue
	Do() (*work.Job, error)
	Run()
}

//NewEnqueue 建立一個新的 Enqueue instance
func NewEnqueue(Engine *work.Enqueuer, Spec string, JobName string, Args map[string]interface{}) Enqueue {
	if Args == nil {
		Args = make(map[string]interface{})
	}
	return &EnqueueEntity{
		ID:      uuid.NewV4().String(),
		Engine:  Engine,
		Spec:    Spec,
		JobName: JobName,
		Args:    Args,
	}
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

func (g *EnqueueEntity) SetEntryID(id cronV3.EntryID) Enqueue {
	g.EntryID = id
	return g
}

func (g *EnqueueEntity) GetSpec() string {
	return g.Spec
}

func (g *EnqueueEntity) SetSpec(Spec string) Enqueue {
	g.Spec = Spec
	return g
}

func (g *EnqueueEntity) GetJobName() string {
	return g.JobName
}

func (g *EnqueueEntity) SetJobName(JobName string) Enqueue {
	g.JobName = JobName
	return g
}

func (g *EnqueueEntity) GetArgs() map[string]interface{} {
	return g.Args
}

func (g *EnqueueEntity) AddArgs(key string, value interface{}) Enqueue {
	g.Args[key] = value
	return g
}

func (g *EnqueueEntity) RemoveArgs(key string) Enqueue {
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
	id           string
	cron         *cronV3.Cron
	core         *work.Enqueuer
	workerClient *work.Client
	tasks        map[string]Enqueue
	redisPool    *redis.Pool
	hostname     string
}

type MasterOption struct {
	IsCluster  bool
	ETCDConfig clientv3.Config
	BasePath   string
}

type Master interface {
	Init() Master
	GetID() string
	AddTask(Spec string, JobName string, Args map[string]interface{}) (string, error)
	GetEnqueues() map[string]Enqueue
	GetEnqueue(id string) (Enqueue, error)
	GetWorkerHeartbeats() ([]*work.WorkerPoolHeartbeat, error)
	GetBusyWorkers() ([]*work.WorkerObservation, error)
	GetQueues() ([]*work.Queue, error)
	ExecTask(id string) error
	RemoveTask(id string) error
}

//NewMaster 建立 Master 實例
func NewMaster(pool *redis.Pool, namespace string, option *MasterOption) Master {
	master := &MasterEntity{
		id:           uuid.NewV4().String(),
		cron:         cronV3.New(cronV3.WithSeconds()),
		core:         work.NewEnqueuer(namespace, pool),
		workerClient: work.NewClient(namespace, pool),
		tasks:        make(map[string]Enqueue),
		redisPool:    pool,
	}
	hostname, err := os.Hostname()
	if err == nil {
		master.hostname = hostname
	} else {
		panic("can not get hostname, reason is " + err.Error())
		return nil
	}
	if option != nil {
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
func (g *MasterEntity) AddTask(Spec string, JobName string, Args map[string]interface{}) (string, error) {
	enqueue := NewEnqueue(g.core, Spec, JobName, Args)
	g.tasks[enqueue.GetID()] = enqueue
	return enqueue.GetID(), nil
}

func (g *MasterEntity) GetEnqueues() map[string]Enqueue {
	return g.tasks
}

func (g *MasterEntity) GetEnqueue(id string) (Enqueue, error) {
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

//啟動排程
func (g *MasterEntity) ExecTask(id string) error {
	if task, ok := g.tasks[id]; ok {
		if id, err := g.cron.AddJob(task.GetSpec(), task); err == nil {
			task.SetEntryID(id)
		} else {
			return err
		}
		g.cron.Start()
		return nil
	} else {
		return errors.New("task " + id + " is not exist")
	}
}

//移除排程
func (g *MasterEntity) RemoveTask(id string) error {
	if task, ok := g.tasks[id]; ok {
		if EntryID := task.GetEntryID(); EntryID != 0 {
			g.cron.Remove(EntryID)
			g.cron.Start()
		} else {
			return errors.New("task " + id + " is not execute")
		}
		return nil
	} else {
		return errors.New("task " + id + " is not exist")
	}
}
