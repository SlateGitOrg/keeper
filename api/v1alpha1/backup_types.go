package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupPhase is the coarse lifecycle state of a Backup.
// +kubebuilder:validation:Enum=Pending;PreHooks;Snapshotting;PostHooks;Finalizing;Completed;Failed
type BackupPhase string

// Backup phases, in lifecycle order.
const (
	BackupPending      BackupPhase = "Pending"
	BackupPreHooks     BackupPhase = "PreHooks"
	BackupSnapshotting BackupPhase = "Snapshotting"
	BackupPostHooks    BackupPhase = "PostHooks"
	BackupFinalizing   BackupPhase = "Finalizing"
	BackupCompleted    BackupPhase = "Completed"
	BackupFailed       BackupPhase = "Failed"
)

// Terminal reports whether the phase is final.
func (p BackupPhase) Terminal() bool { return p == BackupCompleted || p == BackupFailed }

// VolumeBackup is the per-claim record of a Backup.
type VolumeBackup struct {
	// PVCName is the source claim.
	PVCName string `json:"pvcName"`

	// Capacity requested by the source claim; restores request the same.
	// +optional
	Capacity resource.Quantity `json:"capacity,omitempty"`

	// StorageClassName of the source claim.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// AccessModes of the source claim.
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`

	// VolumeMode of the source claim.
	// +optional
	VolumeMode *corev1.PersistentVolumeMode `json:"volumeMode,omitempty"`

	// SnapshotName is the VolumeSnapshot (CSISnapshot driver).
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`

	// JobName is the mover Job (FileCopy driver).
	// +optional
	JobName string `json:"jobName,omitempty"`

	// ObjectKey is the archive's key in storage (FileCopy driver).
	// +optional
	ObjectKey string `json:"objectKey,omitempty"`

	// SizeBytes is the stored size: compressed archive size for FileCopy,
	// restoreSize for CSISnapshot.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// Files is the number of entries archived (FileCopy driver).
	// +optional
	Files int64 `json:"files,omitempty"`

	// SHA256 of the archive (FileCopy driver).
	// +optional
	SHA256 string `json:"sha256,omitempty"`

	// Ready is true once this volume's copy is durable.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// HookResult records one hook execution in one pod.
type HookResult struct {
	// Hook is the hook name.
	Hook string `json:"hook"`
	// Stage is "pre" or "post".
	Stage string `json:"stage"`
	// Pod the hook ran in; empty when no pod matched.
	// +optional
	Pod string `json:"pod,omitempty"`
	// Succeeded is the outcome.
	Succeeded bool `json:"succeeded"`
	// ExitCode of the command, when it ran to completion.
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// Message holds the tail of stderr or the error text.
	// +optional
	Message string `json:"message,omitempty"`
	// Time the execution finished.
	Time metav1.Time `json:"time"`
}

// BackupStatus is the observed state of a Backup.
type BackupStatus struct {
	// Phase is the lifecycle state.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// StartTime is when the operator first acted on the Backup.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// SnapshotTime is the point in time the copy represents.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`

	// CompletionTime is when the Backup reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Volumes holds per-claim progress.
	// +optional
	// +listType=map
	// +listMapKey=pvcName
	Volumes []VolumeBackup `json:"volumes,omitempty"`

	// Hooks records hook executions.
	// +optional
	// +listType=atomic
	Hooks []HookResult `json:"hooks,omitempty"`

	// ManifestKey is the object key of the backup manifest.
	// +optional
	ManifestKey string `json:"manifestKey,omitempty"`

	// SizeBytes sums Volumes[].SizeBytes.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// Message explains the current phase, especially Failed.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the generation last acted upon.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions follow the Kubernetes condition conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Backup is one point-in-time copy of a set of PersistentVolumeClaims.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=bk,categories=keeper
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=`.spec.driver.type`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.sizeBytes`
// +kubebuilder:printcolumn:name="Snapshot",type=date,JSONPath=`.status.snapshotTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec   BackupSpec   `json:"spec"`
	Status BackupStatus `json:"status,omitempty"`
}

// BackupList is a list of Backups.
//
// +kubebuilder:object:root=true
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}
