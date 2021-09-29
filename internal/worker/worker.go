package worker

import (
	"github.com/mimuret/dtap"
	"github.com/mimuret/tapcat/internal/config"
	nats "github.com/nats-io/nats.go"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)
var (
	NatsClientQeueuLen = 4096
)
type Worker struct {
	RBuf *dtap.RBuf
	Err  error
	config *config.Config
	con *nats.Conn
	sub    *nats.Subscription
}

func NewWorker(c *config.Config,inCounter,lostCounter prometheus.Counter) *Worker {
	return &Worker{
		config: c,
		RBuf:   dtap.NewRbuf(uint(c.GetQueueSize()), inCounter, lostCounter),
	}
}

func (w *Worker) Run() error {
	sub, err := w.subscribe()
	if err != nil {
		w.Err = err
		return err
	}
	w.sub = sub
	return nil
}

func (w *Worker) Stop() {
	w.sub.Unsubscribe()
	w.sub.Drain()
}

func (w *Worker) Stats() nats.Statistics {
	if w.con == nil {
		return nats.Statistics{}
	}
	return w.con.Stats()
}

func (w *Worker) subscribe() (*nats.Subscription, error) {
	var err error
	c := w.config
	if c.Nats.Token != "" {
		w.con, err = nats.Connect(c.Nats.Host, nats.Token(c.Nats.Token),nats.SyncQueueLen(NatsClientQeueuLen))
	} else if c.Nats.User != "" {
		w.con, err = nats.Connect(c.Nats.Host, nats.UserInfo(c.Nats.User, c.Nats.Password),nats.SyncQueueLen(NatsClientQeueuLen))
	} else {
		w.con, err = nats.Connect(c.Nats.Host, nats.SyncQueueLen(NatsClientQeueuLen))
	}
	if err != nil {
		return nil, errors.Errorf("can't connect nats: %v", err)
	}
	return w.con.Subscribe(c.Nats.Subject, w.subscribeCB)
}

func (w *Worker) subscribeCB(msg *nats.Msg) {
	w.RBuf.Write(msg.Data)
}
