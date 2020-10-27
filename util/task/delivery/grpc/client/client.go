package client

import (
	"context"
	"encoding/json"
	task "github.com/codingXiang/go-worker/util/task/delivery/grpc/pb"
	_struct "github.com/golang/protobuf/ptypes/struct"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
)

type TaskGRPCClient struct {
	config *viper.Viper
}

func NewTaskGRPCClient(config *viper.Viper) *TaskGRPCClient {
	return &TaskGRPCClient{
		config: config,
	}
}

func (g *TaskGRPCClient) AddTask(namespace, jobName string, spec string, args map[string]interface{}) (*task.TaskReply, error) {
	conn, err := grpc.Dial(g.config.GetString("grpc.url")+":"+g.config.GetString("grpc.port"), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("連線失敗：%v", err)
	}
	defer conn.Close()
	c := task.NewTaskServerClient(conn)

	var in = new(task.TaskRequest)
	in.Namespace = namespace
	in.JobName = jobName
	in.Spec = spec
	tmp, _ := json.Marshal(args)
	in.Args = new(_struct.Struct)
	in.Args.UnmarshalJSON(tmp)

	return c.AddTask(context.TODO(), in)
}
