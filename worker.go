package main

import (
	_ "github.com/mailru/go-clickhouse"
	"github.com/mimuret/dtap"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Worker struct {
	config *Config
	rBuf   *dtap.RBuf
	sub    *nats.Subscription
}

func NewWorker(c *Config) *Worker {
	return &Worker{
		config: c,
		rBuf:   dtap.NewRbuf(uint(c.GetQueueSize()), prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{})),
	}
}
func (w *Worker) Run() error {
	sub, err := w.subscribe()
	if err != nil {
		return err
	}
	w.sub = sub
	return nil
}
func (w *Worker) Stop() {
	w.sub.Unsubscribe()
	w.sub.Drain()
}

func (w *Worker) subscribe() (*nats.Subscription, error) {
	var err error
	c := w.config
	var con *nats.Conn
	if c.Nats.Token != "" {
		con, err = nats.Connect(c.Nats.Host, nats.Token(c.Nats.Token))
	} else if c.Nats.User != "" {
		con, err = nats.Connect(c.Nats.Host, nats.UserInfo(c.Nats.User, c.Nats.Password))
	} else {
		con, err = nats.Connect(c.Nats.Host)
	}
	if err != nil {
		return nil, errors.Errorf("can't connect nats: %v", err)
	}
	return con.Subscribe(c.Nats.Subject, w.subscribeCB)
}

func (w *Worker) subscribeCB(msg *nats.Msg) {
	w.rBuf.Write(msg.Data)
}
