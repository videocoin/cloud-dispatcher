package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "github.com/reality-lab-networks/liveplanet-api/gengo/transcoder/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"golang.org/x/net/context"
)

type Metrics struct {
	jobsTotalCount  *prometheus.GaugeVec
	tasksTotalCount *prometheus.GaugeVec
}

func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		tasksTotalCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "tasks_total",
				Help:      "Total count of tasks",
			},
			[]string{"status", "machine_type"},
		),
	}
}

func (m *Metrics) RegisterAll() {
	prometheus.MustRegister(m.tasksTotalCount)
}

type Collector struct {
	mutex   sync.RWMutex
	metrics *Metrics
	dm      *datastore.DataManager
	ticker  *time.Ticker
}

func NewCollector(namespace string, dm *datastore.DataManager) *Collector {
	metrics := NewMetrics(namespace)
	metrics.RegisterAll()
	return &Collector{
		metrics: metrics,
		dm:      dm,
		ticker:  time.NewTicker(time.Second * 5),
	}
}

func (mc *Collector) Collect() {
	for range mc.ticker.C {
		mc.mutex.Lock()
		mc.collectMetrics()
		mc.mutex.Unlock()
	}
}

func (mc *Collector) collectMetrics() {
	statuses := []string{
		v1.StatusPending.String(),
		v1.StatusEncoding.String(),
		v1.StatusCompleted.String(),
		v1.StatusFailed.String(),
		v1.StatusCanceled.String(),
		v1.StatusOnAir.String(),
	}

	tasksStat := map[string]float64{}

	ctx := context.Background()

	mts := []string{""}
	tasks, err := mc.dm.GetTasks(ctx)
	if err == nil {
		for _, task := range tasks {
			k := fmt.Sprintf("%s/%s", task.Status, task.MachineType.String)
			tasksStat[k]++
			mts = append(mts, task.MachineType.String)
		}
	}

	for _, status := range statuses {
		for _, mt := range mts {
			k := fmt.Sprintf("%s/%s", status, mt)
			mc.metrics.tasksTotalCount.WithLabelValues(status, mt).Set(tasksStat[k])
		}
	}
}

func (mc *Collector) Start() error {
	go mc.Collect()
	return nil
}

func (mc *Collector) Stop() error {
	mc.ticker.Stop()
	return nil
}
