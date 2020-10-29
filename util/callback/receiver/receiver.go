package callback_receiver

import (
	"encoding/json"
	"github.com/codingXiang/go-worker/model"
	"github.com/gocraft/work"
	"github.com/spf13/viper"
)

//此 Service 於 master 實作
type Service struct {
	model.Service
	handler func(receiver *model.CallbackReceiver) error
}

func NewService(config *viper.Viper) model.Service {
	return model.NewService("callback", config)
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
