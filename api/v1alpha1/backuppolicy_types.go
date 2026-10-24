package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConcurrencyPolicy says whether scheduled Backups of one policy may overlap.
// +kubebuilder:validation:Enum=Forbid;Allow
type ConcurrencyPolicy string

const (
	// ConcurrencyForbid delays a scheduled Backup while another is running.
	ConcurrencyForbid ConcurrencyPolicy = "Forbid"
	// ConcurrencyAllow starts scheduled Backups regardless.
	ConcurrencyAllow ConcurrencyPolicy = "Allow"
)

// Retention is a grandfather-father-son policy over completed Backups. Each
// keepX rule keeps the newest Backup of each of the last X distinct periods
// that contain a Backup; a Backup is kept if any rule keeps it. Periods are
// computed in the policy's time zone; weeks are ISO-8601 weeks.
//
// +kubebuilder:validation:XValidation:rule="self.keepLast + self.keepHourly + self.keepDaily + self.keepWeekly + self.keepMonthly + self.keepYearly > 0",message="retention must keep at least one backup"
type Retention struct {
	// KeepLast keeps the N most recent Backups.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepLast int32 `json:"keepLast"`
	// KeepHourly keeps the newest Backup of each of the last N hours.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepHourly int32 `json:"keepHourly"`
	// KeepDaily keeps the newest Backup of each of the last N days.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepDaily int32 `json:"keepDaily"`
	// KeepWeekly keeps the newest Backup of each of the last N ISO weeks.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepWeekly int32 `json:"keepWeekly"`
	// KeepMonthly keeps the newest Backup of each of the last N months.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepMonthly int32 `json:"keepMonthly"`
	// KeepYearly keeps the newest Backup of each of the last N years.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	KeepYearly int32 `json:"keepYearly"`
}

// BackupTemplate is stamped into each scheduled Backup.
type BackupTemplate struct {
	// Labels are added to created Backups.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Spec of created Backups.
	Spec BackupSpec `json:"spec"`
}

// BackupPolicySpec defines a schedule and retention for Backups.
//
// +kubebuilder:validation:XValidation:rule="!has(self.startingDeadlineSeconds) || self.startingDeadlineSeconds >= 10",message="startingDeadlineSeconds must be at least 10"
type BackupPolicySpec struct {
	// Schedule is a standard 5-field cron expression or a descriptor such as
	// @daily or "@every 6h". It is evaluated in TimeZone.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:XValidation:rule="!self.contains('TZ=')",message="set the time zone with spec.timeZone, not CRON_TZ/TZ in the schedule"
	// +kubebuilder:validation:XValidation:rule="self.startsWith('@') || self.trim().split(' ').filter(f, f != '').size() == 5",message="schedule must have exactly 5 fields or be an @descriptor"
	Schedule string `json:"schedule"`

	// TimeZone is an IANA zone name used for the schedule and retention periods.
	// +optional
	// +kubebuilder:default=UTC
	// +kubebuilder:validation:MaxLength=64
	TimeZone string `json:"timeZone,omitempty"`

	// Suspend stops new Backups from being scheduled; retention still runs.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// ConcurrencyPolicy controls overlapping Backups.
	// +optional
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds skips a scheduled run that could not start within
	// this many seconds of its slot. Unset means "never too late".
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// Retention prunes completed Backups.
	Retention Retention `json:"retention"`

	// FailedBackupsHistoryLimit keeps this many failed Backups.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	FailedBackupsHistoryLimit *int32 `json:"failedBackupsHistoryLimit,omitempty"`

	// Template is copied into each created Backup.
	Template BackupTemplate `json:"template"`
}

// RetentionStatus summarises the last retention pass.
type RetentionStatus struct {
	// Kept is the number of completed Backups retained.
	// +optional
	Kept int32 `json:"kept,omitempty"`
	// PrunedTotal counts Backups this policy has pruned.
	// +optional
	PrunedTotal int64 `json:"prunedTotal,omitempty"`
	// LastPruneTime is when a Backup was last pruned.
	// +optional
	LastPruneTime *metav1.Time `json:"lastPruneTime,omitempty"`
}

// BackupPolicyStatus is the observed state of a BackupPolicy.
type BackupPolicyStatus struct {
	// LastScheduleTime is the most recent schedule slot that was handled
	// (a Backup was created, or the slot was skipped).
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// LastSuccessfulTime is the snapshot time of the newest completed Backup.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// NextScheduleTime is the next schedule slot.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// Active lists non-terminal Backups.
	// +optional
	// +listType=atomic
	Active []corev1.LocalObjectReference `json:"active,omitempty"`

	// Retention summarises pruning.
	// +optional
	Retention RetentionStatus `json:"retention,omitempty"`

	// ObservedGeneration is the generation last acted upon.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions follow the Kubernetes condition conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BackupPolicy schedules Backups and prunes them with a GFS retention policy.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=bp,categories=keeper
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="Last",type=date,JSONPath=`.status.lastScheduleTime`
// +kubebuilder:printcolumn:name="Kept",type=integer,JSONPath=`.status.retention.kept`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 50",message="BackupPolicy names are limited to 50 characters so Backup names fit"
type BackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupPolicySpec   `json:"spec"`
	Status BackupPolicyStatus `json:"status,omitempty"`
}

// BackupPolicyList is a list of BackupPolicies.
//
// +kubebuilder:object:root=true
type BackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupPolicy `json:"items"`
}
