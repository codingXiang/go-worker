package callback

import (
	"context"
	"encoding/json"
	go_worker "github.com/codingXiang/go-worker"
	"github.com/codingXiang/go-worker/model"
	task "github.com/codingXiang/go-worker/util/task/delivery/grpc/pb"
	"github.com/gocraft/work"
	_struct "github.com/golang/protobuf/ptypes/struct"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	Callback = "callback"
)

type Service struct {
	model.Service
	grpcConn *grpc.ClientConn
}

func NewService(config *viper.Viper) model.Service {
	return model.NewService("callback", config)
}

func (g *Service) init(c *viper.Viper) error {
	var err error
	path := c.GetString("grpc.url") + ":" + c.GetString("grpc.port")
	g.grpcConn, err = grpc.Dial(path, grpc.WithInsecure())
	return err
}

func (g *Service) Do(job *work.Job) error {
	err := g.init(g.GetConfig())
	if err != nil {
		return err
	}
	m := new(model.Callback)
	args, _ := json.Marshal(job.Args)
	err = json.Unmarshal(args, &m)
	if err != nil {
		return err
	}

	return g.send(m)
}

func (g *Service) send(m *model.Callback) error {
	client := task.NewTaskServerClient(g.grpcConn)
	req := new(task.TaskRequest)
	req.Namespace = m.Namespace
	req.JobName = Callback
	req.Spec = go_worker.Now
	req.Args = new(_struct.Struct)
	tmp, _ := json.Marshal(m)
	req.Args.UnmarshalJSON(tmp)
	_, err := client.AddTask(context.TODO(), req)
	return err
}
