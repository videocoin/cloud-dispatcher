package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"golang.org/x/net/context"
)

type Metrics struct {
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
			[]string{"status", "machine_type", "type"},
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
		v1.TaskStatusCreated.String(),
		v1.TaskStatusPending.String(),
		v1.TaskStatusAssigned.String(),
		v1.TaskStatusEncoding.String(),
		v1.TaskStatusFailed.String(),
		v1.TaskStatusCompleted.String(),
		v1.TaskStatusCanceled.String(),
	}

	tasksStat := map[string]float64{}

	ctx := context.Background()

	mts := []string{""}
	types := []string{v1.TaskTypeVOD.String(), v1.TaskTypeLive.String()}
	tasks, err := mc.dm.GetTasks(ctx)
	if err == nil {
		for _, task := range tasks {
			if task.Status == v1.TaskStatusPending && task.IsLock {
				continue
			}
			taskType := v1.TaskTypeVOD
			if task.IsOutputHLS() {
				taskType = v1.TaskTypeLive
			}

			if taskType == v1.TaskTypeVOD {
				continue
			}

			k := fmt.Sprintf("%s/%s/%s", task.Status, task.MachineType.String, taskType.String())
			tasksStat[k]++
			mts = append(mts, task.MachineType.String)
		}
	}

	mc.metrics.tasksTotalCount.Reset()

	for _, status := range statuses {
		for _, mt := range mts {
			for _, t := range types {
				k := fmt.Sprintf("%s/%s/%s", status, mt, t)
				mc.metrics.tasksTotalCount.WithLabelValues(status, mt, t).Set(tasksStat[k])
			}
		}
	}
}

func (mc *Collector) Start() {
	go mc.Collect()
}

func (mc *Collector) Stop() {
	mc.ticker.Stop()
}
