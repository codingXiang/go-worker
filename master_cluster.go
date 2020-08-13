package go_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"os/signal"
	"strings"
)

const (
	MASTER = "master"
	TASK   = "task"
	LOCK   = "lock"
)

type TaskInfo struct {
	MasterID string                 `json:"masterId"`
	ID       string                 `json:"id"`
	Spec     string                 `json:"spec"`
	JobName  string                 `json:"jobName"`
	Args     map[string]interface{} `json:"args"`
}

type MasterClusterEntity struct {
	*MasterEntity
	key map[string]string

	etcdConfig clientv3.Config
}

//NewMasterCluster 建立集群版本 Master Instance
func NewMasterCluster(base *MasterEntity, option *MasterOption) Master {
	config := option.ETCDConfig
	cluster := new(MasterClusterEntity)
	cluster.MasterEntity = base
	cluster.key = make(map[string]string)
	cluster.etcdConfig = config
	basePath := "/" + option.BasePath + "/" + base.core.Namespace
	cluster.key[MASTER] = BuildKeyPath(basePath, MASTER)
	cluster.key[TASK] = BuildKeyPath(basePath, TASK)
	cluster.key[LOCK] = BuildKeyPath(basePath, LOCK)
	cluster.WatchMaster()
	cluster.WatchTask()
	return cluster
}

//Init 初始化
func (g *MasterClusterEntity) Init() Master {
	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, os.Kill)
		<-signalChan
		g.retired()
	}()
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
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = client.Put(context.Background(), g.getMasterPathWithID(), g.GetID())
	return err
}

//retired 節點退役
func (g *MasterClusterEntity) retired() error {
	defer g.cron.Stop()
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = client.Delete(context.Background(), g.getMasterPathWithID())
	if err != nil {
		return err
	}
	return nil
}

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

//handleRetiredMaster 處理退役的 Master
func (g *MasterClusterEntity) handleRetiredMaster(id string) error {
	if id != g.GetID() {
		client, err := clientv3.New(g.etcdConfig)
		if err != nil {
			return err
		}
		defer client.Close()
		var handleTask = false
		log.Println("master", id, " was retired")

		key := g.key[LOCK] + id
		ctx := context.TODO()
		s, err := concurrency.NewSession(client)
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
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	defer client.Close()
	resp, err := client.Get(context.TODO(), g.getTaskPathWithCustomPath(key), clientv3.WithPrefix())
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
			info, err := g.AddTask(task.Spec, task.JobName, task.Args)
			if err != nil {
				return err
			}
			err = g.ExecTask(info.ID)
			if err != nil {
				return err
			}
		}
	} else {
		log.Println("not found any tasks")
	}
	return nil
}

//WatchMaster 集群監聽任務
func (g *MasterClusterEntity) WatchMaster() error {
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	channel := client.Watch(context.TODO(), g.getMasterPath(), clientv3.WithPrefix())
	go func() {
		defer client.Close()
		for res := range channel {
			event := res.Events[0]
			kv := event.Kv
			key := strings.ReplaceAll(string(kv.Key), g.getMasterPath()+"/", "")
			switch event.Type {
			case clientv3.EventTypeDelete:
				g.handleRetiredMaster(key)
				break
			case clientv3.EventTypePut:
				log.Println("master", key, " was joined!")
				break
			}
		}
	}()
	return nil
}

//WatchTask 集群監聽任務
func (g *MasterClusterEntity) WatchTask() error {
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	channel := client.Watch(context.TODO(), g.getTaskPathWithID(), clientv3.WithPrefix())
	go func() {
		defer client.Close()
		for res := range channel {
			if len(res.Events) > 0 {
				event := res.Events[0]
				kv := event.Kv
				key := string(kv.Key)
				value := string(kv.Value)
				fmt.Println(event.Type.String() + " - " + key + " : " + value)
			}
		}
	}()
	return nil
}

//AddTask 新增任務
func (g *MasterClusterEntity) AddTask(Spec string, JobName string, Args map[string]interface{}) (*TaskInfo, error) {
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	info, err := g.MasterEntity.AddTask(Spec, JobName, Args)
	tmp, _ := json.Marshal(info)
	_, err = client.Put(context.TODO(), g.getTaskPathWithCustomPath(g.GetID(), info.ID), string(tmp))
	if err != nil {
		return "", err
	}
	return info, nil
}

//RemoveTask 移除任務
func (g *MasterClusterEntity) RemoveTask(id string) error {
	client, err := clientv3.New(g.etcdConfig)
	if err != nil {
		return err
	}
	defer client.Close()
	if _, err := client.Delete(context.TODO(), g.getTaskPathWithCustomPath(id)); err == nil {
		return g.MasterEntity.RemoveTask(id)
	} else {
		return err
	}
}
