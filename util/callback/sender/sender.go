package sender

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

type Sender struct {
	grpcConn *grpc.ClientConn
}

func NewSender(config *viper.Viper) (*Sender, error) {
	return new(Sender).init(config)
}

func (s *Sender) init(c *viper.Viper) (*Sender, error) {
	var err error
	path := c.GetString("grpc.url") + ":" + c.GetString("grpc.port")
	s.grpcConn, err = grpc.Dial(path, grpc.WithInsecure())
	return s, err
}
func (s *Sender) Do(in interface{}) error {
	m, err := new(model.CallbackSender).InterfaceToObject(in)
	if err != nil {
		return err
	}
	client := task.NewTaskServerClient(s.grpcConn)
	req := new(task.TaskRequest)
	req.Namespace = m.Namespace
	req.JobName = Callback
	req.Spec = go_worker.Now
	req.Args = new(_struct.Struct)
	tmp, _ := json.Marshal(m)
	req.Args.UnmarshalJSON(tmp)
	_, err = client.AddTask(context.TODO(), req)
	return err
}

//此 Service 於 master 實作
type Service struct {
	model.Service
	sender *Sender
}

func NewService(config *viper.Viper) model.Service {
	return model.NewService("callback", config)
}

func (g *Service) init(c *viper.Viper) error {
	if s, err := NewSender(c); err != nil {
		return err
	} else {
		g.sender = s
		return nil
	}
}

func (g *Service) Do(job *work.Job) error {
	err := g.init(g.GetConfig())
	if err != nil {
		return err
	}
	m := new(model.CallbackSender)
	args, _ := json.Marshal(job.Args)
	err = json.Unmarshal(args, &m)
	if err != nil {
		return err
	}
	return g.sender.Do(job.Args)
}