package worker

import (
	"math"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Counter = &Counter{}

type Counter struct {
	sync.Mutex
	prometheus.Counter
	v uint64
}

func NewCounter() *Counter {
	return &Counter{Counter: prometheus.NewCounter(prometheus.CounterOpts{})}
}

func (c *Counter) Get() uint64 {
	return c.v
}

func (c *Counter) Inc() {
	c.Lock()
	defer c.Unlock()
	c.v++
	c.Counter.Inc()
}

func DiffCount(a, b uint64) uint64 {
	if a <= b {
		return b - a
	}
	return math.MaxUint64 - a + b
}
