package go_worker

import (
	"context"
	"encoding/json"
	"github.com/codingXiang/go-orm/v2/mongo"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gomodule/redigo/redis"
	"github.com/wendal/errors"
	"gopkg.in/mgo.v2/bson"
	"log"
	"strings"
	"sync"
)

const (
	MASTER = "master"
	TASK   = "task"
	LOCK   = "lock"
)
const (
	NAMESPACE       = "namespace"
	JOB_NAME        = "jobName"
	STATUS          = "status"
	STATUS_PENDING  = "pending"
	STATUS_RUNNING  = "running"
	STATUS_COMPLETE = "complete"
	STATUS_FAILED   = "failed"
)

const ()

type ETCDAuth struct {
	Endpoints []string
	Username  string
	Password  string
}

type TaskInfo struct {
	MasterID string                 `json:"masterId"`
	ID       string                 `json:"id"`
	Spec     string                 `json:"spec"`
	JobName  string                 `json:"jobName"`
	Args     map[string]interface{} `json:"args"`
}

type MasterClusterEntity struct {
	*MasterEntity
	key         map[string]string
	etcdConfig  clientv3.Config
	client      *clientv3.Client
	mongoClient *mongo.Client
	lock        sync.Mutex
}

//NewMasterCluster 建立集群版本 Master Instance
func NewMasterCluster(base *MasterEntity, option *MasterOption) Master {
	config := option.ETCDConfig
	cluster := new(MasterClusterEntity)
	cluster.MasterEntity = base
	cluster.key = make(map[string]string)
	cluster.etcdConfig = config
	basePath := "/" + option.BasePath
	cluster.key[MASTER] = BuildKeyPath(basePath, MASTER, base.core.Namespace)
	cluster.key[TASK] = BuildKeyPath(basePath, TASK, base.core.Namespace)
	cluster.key[LOCK] = BuildKeyPath(basePath, LOCK, base.core.Namespace)
	if client, err := clientv3.New(cluster.etcdConfig); err == nil {
		cluster.client = client
	} else {
		log.Fatal("etcd client create failed, reason is ", err.Error())
	}

	if option.MongoClient != nil {
		cluster.mongoClient = option.MongoClient
	}

	go cluster.WatchMaster()
	go cluster.WatchTask()
	return cluster
}

//Init 初始化
func (g *MasterClusterEntity) Init() Master {
	//go func() {
	//	signalChan := make(chan os.Signal, 1)
	//	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	//	<-signalChan
	//	g.retired()
	//}()
	if err := g.register(); err == nil {
		return g
	} else {
		return nil
	}
}

func (g *MasterClusterEntity) getMasterPath() string {
	return g.key[MASTER]
}
func (g *MasterClusterEntity) getTaskPath() string {
	return g.key[TASK]
}
func (g *MasterClusterEntity) getMasterPathWithID() string {
	return BuildKeyPath(g.key[MASTER], g.GetID())
}
func (g *MasterClusterEntity) getTaskPathWithID() string {
	return BuildKeyPath(g.key[TASK], g.GetID())
}
func (g *MasterClusterEntity) getMasterPathWithCustomPath(path ...string) string {
	return BuildKeyPath(g.key[MASTER], path...)
}
func (g *MasterClusterEntity) getTaskPathWithCustomPath(path ...string) string {
	return BuildKeyPath(g.key[TASK], path...)
}

//register 註冊 Master 節點
func (g *MasterClusterEntity) register() error {
	//设置租约时间
	resp, err := g.client.Grant(context.Background(), 5)
	if err != nil {
		return err
	}
	_, err = g.client.Put(context.Background(), g.getMasterPathWithID(), g.GetID(), clientv3.WithLease(resp.ID))
	//设置续租 定期发送需求请求
	_, err = g.client.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		return err
	}
	return err
}

////retired 節點退役
//func (g *MasterClusterEntity) retired() error {
//	defer g.cron.Stop()
//	client, err := clientv3.New(g.etcdConfig)
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	_, err = client.Delete(context.Background(), g.getMasterPathWithID())
//	if err != nil {
//		return err
//	}
//	return nil
//}

//setLockKey 設定鎖定的 key
func (g *MasterClusterEntity) setLockKey(key string) error {
	conn, err := g.redisPool.Dial()
	defer conn.Close()
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", key, g.GetID())
	return err
}

//getLockKey 取得鎖定的 key
func (g *MasterClusterEntity) getLockKey(key string) (string, error) {
	conn, err := g.redisPool.Dial()
	defer conn.Close()
	if err != nil {
		return "", err
	}
	return redis.String(conn.Do("GET", key))
}

func (g *MasterClusterEntity) checkMasterIsExist() error {
	resp, err := g.client.Get(context.TODO(), g.getMasterPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		return nil
	} else {
		g.client.Delete(context.TODO(), g.getTaskPath(), clientv3.WithPrefix())
		return errors.New("")
	}
}

//handleRetiredMaster 處理退役的 Master
func (g *MasterClusterEntity) handleRetiredMaster(id string) error {
	if err := g.checkMasterIsExist(); err != nil {
		return err
	}
	if id != g.GetID() {
		var handleTask = false
		log.Println("master", id, " was retired")

		key := g.key[LOCK] + id
		ctx := context.TODO()
		s, err := concurrency.NewSession(g.client)
		if err != nil {
			log.Fatal(err)
			return err
		}
		l := concurrency.NewMutex(s, key)

		if err := l.Lock(ctx); err != nil {
			log.Fatal(err)
			return err
		}

		//從 redis 中取得此任務是否已被賦予值
		if _, err := g.getLockKey(key); err != nil {
			//如果任務尚未被賦予值，則將其賦予此 instance 的 id
			if strings.Contains(err.Error(), "nil") {
				handleTask = true
				if err := g.setLockKey(key); err != nil {
					log.Fatal(err)
					return err
				}
			} else {
				log.Fatal(err)
				return err
			}
		}

		//接收所有來自退役 master 的任務
		if handleTask {
			if err := g.handleRetiredMasterTasks(id); err != nil {
				panic(err)
				return err
			}
		}

		//處理完畢後解鎖
		if err := l.Unlock(ctx); err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

//handleRetiredMasterTasks 接收處理退役 master 的任務
func (g *MasterClusterEntity) handleRetiredMasterTasks(key string) error {
	resp, err := g.client.Get(context.TODO(), g.getTaskPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		for _, kv := range resp.Kvs {
			task := new(TaskInfo)
			err := json.Unmarshal(kv.Value, &task)
			if err != nil {
				return err
			}
			if task.MasterID == key {
				info, err := g.AddTask(task.Spec, task.JobName, task.Args)
				if err != nil {
					return err
				}
				err = g.ExecTask(info.ID)
				if err != nil {
					return err
				}
				_, err = g.client.Delete(context.TODO(), string(kv.Key))
				if err != nil {
					return err
				}
			}
		}
	} else {
		log.Println("not found any tasks")
	}
	return nil
}

//WatchMaster 集群監聽 Master
func (g *MasterClusterEntity) WatchMaster() {
	rch := g.client.Watch(context.Background(), g.getMasterPath(), clientv3.WithPrefix())
	log.Printf("watching master prefix: %s now...", g.getMasterPath())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			kv := ev.Kv
			key := strings.ReplaceAll(string(kv.Key), g.getMasterPath()+"/", "")
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				g.handleRetiredMaster(key)
			case mvccpb.DELETE: //删除
				log.Println("master ", key, " was joined!")
			}
		}
	}
	log.Println("stop watch master ", g.getMasterPath())
}

//WatchTask 集群監聽任務
func (g *MasterClusterEntity) WatchTask() {
	taskPath := g.getTaskPath()
	rch := g.client.Watch(context.Background(), taskPath, clientv3.WithPrefix())
	log.Printf("watching task prefix: %s now...", taskPath)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			kv := ev.Kv
			key := strings.ReplaceAll(string(kv.Key), taskPath+"/", "")
			switch ev.Type {
			case mvccpb.DELETE: //删除
				g.MasterEntity.RemoveTask(key)
			}
		}
	}
	log.Println("stop watch task ", taskPath)

}

//AddTask 新增任務
func (g *MasterClusterEntity) AddTask(Spec string, JobName string, Args map[string]interface{}) (*TaskInfo, error) {
	//设置租约时间
	resp, err := g.client.Grant(context.Background(), 5)
	info, err := g.MasterEntity.AddTask(Spec, JobName, Args)
	tmp, _ := json.Marshal(info)
	_, err = g.client.Put(context.Background(), g.getTaskPathWithCustomPath(info.ID), string(tmp), clientv3.WithLease(resp.ID))
	if err != nil {
		return nil, err
	}
	//设置续租 定期发送需求请求
	_, err = g.client.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		return nil, err
	}

	return info, g.mongoClient.C(TASK).Insert(mongo.NewRawData(info.ID, bson.M{
		NAMESPACE: g.namespace,
		JOB_NAME:  JobName,
		STATUS:    STATUS_PENDING,
	}, info))
}
func (g *MasterClusterEntity) ExecTask(id string) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if task, ok := g.tasks[id]; ok {
		if task.GetSpec() == "now" {
			task.Run()
			_, err := g.mongoClient.C(TASK).Update(mongo.NewSearchCondition("", task.GetID(), bson.M{
				NAMESPACE: g.namespace,
				JOB_NAME:  task.GetJobName(),
			}, nil), bson.M{
				STATUS: STATUS_RUNNING,
			})
			return err
			//g.RemoveTask(task.GetID())
		} else {
			if id, err := g.cron.AddJob(task.GetSpec(), task); err == nil {
				task.SetEntryID(id)
			} else {
				return err
			}
			g.cron.Start()
		}
		return nil
	} else {
		return errors.New("task " + id + " is not exist")
	}
}

//RemoveTask 移除任務
func (g *MasterClusterEntity) RemoveTask(id string) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if _, err := g.client.Delete(context.TODO(), g.getTaskPathWithCustomPath(id)); err == nil {
		return g.MasterEntity.RemoveTask(id)
	} else {
		return err
	}
}
