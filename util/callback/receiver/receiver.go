package callback_receiver

import (
	"encoding/json"
	"github.com/codingXiang/go-worker/model"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
)

const (
	Callback = "callback"
)

//此 Service 於 master 實作
type Service struct {
	*model.ServiceEntity
	handler func(receiver *model.CallbackReceiver) error
}

func NewService(config *viper.Viper, handler func(receiver *model.CallbackReceiver) error) *Service {
	return new(Service).SetHandler(handler).Init(config)
}

func (g *Service) SetHandler(h func(receiver *model.CallbackReceiver) error) *Service {
	g.handler = h
	return g
}

func (g *Service) Init(config *viper.Viper) *Service {
	g.ServiceEntity = model.NewService(Callback, config)
	return g
}

func (g *Service) Do(job *work.Job) error {
	m := new(model.CallbackReceiver)
	args, _ := json.Marshal(job.Args)
	err := json.Unmarshal(args, &m)
	if err != nil {
		return err
	}
	return g.handler(m)
}

func (g *Service) GetRegisterInfo() *model.JobInfo {
	return &model.JobInfo{
		Name: g.GetTaskName(),
		Job:  g,
	}
}
