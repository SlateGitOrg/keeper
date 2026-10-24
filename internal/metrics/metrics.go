// Package metrics defines the operator's Prometheus metrics, registered with
// the controller-runtime registry so they are served on the manager's
// metrics endpoint next to the built-in controller metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// BackupsTotal counts Backups reaching a terminal phase.
	BackupsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keeper_backups_total",
		Help: "Backups that reached a terminal phase, by driver and result.",
	}, []string{"driver", "result"})

	// BackupDuration observes start-to-completion time of successful Backups.
	BackupDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "keeper_backup_duration_seconds",
		Help:    "Wall time from first reconcile to completion of successful Backups.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"driver"})

	// BackupBytes counts bytes recorded by completed Backups.
	BackupBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keeper_backup_bytes_total",
		Help: "Bytes recorded in completed Backups (archive bytes for FileCopy, restoreSize for CSI).",
	}, []string{"driver"})

	// HookExecutions counts hook executions.
	HookExecutions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keeper_hook_executions_total",
		Help: "Hook executions by stage and result.",
	}, []string{"stage", "result"})

	// RetentionPruned counts Backups deleted by retention.
	RetentionPruned = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keeper_retention_pruned_total",
		Help: "Backups deleted by BackupPolicy retention, by kind (completed or failed).",
	}, []string{"kind"})

	// RestoresTotal counts Restores reaching a terminal phase.
	RestoresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keeper_restores_total",
		Help: "Restores that reached a terminal phase, by result.",
	}, []string{"result"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(BackupsTotal, BackupDuration, BackupBytes, HookExecutions, RetentionPruned, RestoresTotal)
}
