package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Well-known labels, annotations and finalizers.
const (
	// LabelPolicy is set on every Backup created by a BackupPolicy.
	LabelPolicy = "backup.keeper.dev/policy"
	// LabelBackup is set on driver-owned objects (VolumeSnapshots, mover Jobs).
	LabelBackup = "backup.keeper.dev/backup"
	// LabelRestore is set on mover Jobs and PVCs created for a Restore.
	LabelRestore = "backup.keeper.dev/restore"
	// LabelComponent identifies what an operator-created object is for.
	LabelComponent = "backup.keeper.dev/component"
	// LabelPin, when "true" on a Backup, exempts it from retention pruning.
	LabelPin = "backup.keeper.dev/pin"

	// AnnotationRetainedBy lists the retention rules that currently keep a Backup.
	AnnotationRetainedBy = "backup.keeper.dev/retained-by"
	// AnnotationScheduledTime records the schedule slot a Backup was created for.
	AnnotationScheduledTime = "backup.keeper.dev/scheduled-time"
	// AnnotationSkipStorageCleanup, when "true", lets a Backup be deleted without
	// touching object storage (an escape hatch for unreachable or deleted buckets).
	AnnotationSkipStorageCleanup = "backup.keeper.dev/skip-storage-cleanup"

	// FinalizerBackupCleanup guards the removal of a Backup's stored data.
	FinalizerBackupCleanup = "backup.keeper.dev/cleanup"
)

// Condition types shared by the API.
const (
	// ConditionReady is True when an object reached its successful terminal state.
	ConditionReady = "Ready"
	// ConditionScheduleValid reports whether a BackupPolicy schedule parses.
	ConditionScheduleValid = "ScheduleValid"
	// ConditionPreHooksSucceeded reports the result of the pre-snapshot hooks.
	ConditionPreHooksSucceeded = "PreHooksSucceeded"
	// ConditionSnapshotTaken is True once the point-in-time copy is cut.
	ConditionSnapshotTaken = "SnapshotTaken"
	// ConditionPostHooksSucceeded reports the result of the post-snapshot hooks.
	ConditionPostHooksSucceeded = "PostHooksSucceeded"
	// ConditionDataStored is True once data and manifest are durable in storage.
	ConditionDataStored = "DataStored"
)

// VolumeSource selects the PersistentVolumeClaims a Backup captures. Exactly
// one of pvcNames and pvcSelector must be set.
//
// +kubebuilder:validation:XValidation:rule="has(self.pvcNames) != has(self.pvcSelector)",message="exactly one of pvcNames or pvcSelector must be set"
type VolumeSource struct {
	// PVCNames lists claims, in the object's namespace, to back up.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=253
	PVCNames []string `json:"pvcNames,omitempty"`

	// PVCSelector selects claims, in the object's namespace, by label.
	// +optional
	PVCSelector *metav1.LabelSelector `json:"pvcSelector,omitempty"`
}

// DriverType names a snapshot driver.
// +kubebuilder:validation:Enum=CSISnapshot;FileCopy
type DriverType string

const (
	// DriverCSISnapshot cuts a CSI VolumeSnapshot per claim; the data stays in
	// the storage system and the backup manifest goes to object storage.
	DriverCSISnapshot DriverType = "CSISnapshot"
	// DriverFileCopy runs a mover Job per claim that streams a tar.gz of the
	// volume to object storage.
	DriverFileCopy DriverType = "FileCopy"
)

// DriverSpec chooses and configures the snapshot driver.
//
// +kubebuilder:validation:XValidation:rule="self.type == 'CSISnapshot' || !has(self.csi)",message="csi may only be set when type is CSISnapshot"
// +kubebuilder:validation:XValidation:rule="self.type == 'FileCopy' || !has(self.fileCopy)",message="fileCopy may only be set when type is FileCopy"
type DriverSpec struct {
	// Type selects the driver.
	Type DriverType `json:"type"`

	// CSI configures the CSISnapshot driver.
	// +optional
	CSI *CSIDriverSpec `json:"csi,omitempty"`

	// FileCopy configures the FileCopy driver.
	// +optional
	FileCopy *FileCopyDriverSpec `json:"fileCopy,omitempty"`
}

// CSIDriverSpec configures CSI VolumeSnapshot creation.
type CSIDriverSpec struct {
	// VolumeSnapshotClassName is passed to every VolumeSnapshot. Empty selects
	// the cluster default class.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	VolumeSnapshotClassName string `json:"volumeSnapshotClassName,omitempty"`

	// ReadyTimeoutSeconds bounds how long the operator waits for snapshots to
	// be cut and become ready to use.
	// +optional
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=86400
	ReadyTimeoutSeconds int32 `json:"readyTimeoutSeconds,omitempty"`
}

// FileCopyDriverSpec configures mover Jobs.
type FileCopyDriverSpec struct {
	// ActiveDeadlineSeconds bounds each mover Job.
	// +optional
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=172800
	ActiveDeadlineSeconds int64 `json:"activeDeadlineSeconds,omitempty"`

	// BackoffLimit is the mover Job's retry budget.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	BackoffLimit int32 `json:"backoffLimit,omitempty"`

	// Resources for the mover container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// StorageSpec points at an S3-compatible bucket.
//
// +kubebuilder:validation:XValidation:rule="isURL(self.endpoint) && url(self.endpoint).getScheme() in ['http', 'https']",message="endpoint must be an http(s) URL"
// +kubebuilder:validation:XValidation:rule="!has(self.prefix) || (!self.prefix.startsWith('/') && !self.prefix.contains('..'))",message="prefix must be relative and must not contain '..'"
// +kubebuilder:validation:XValidation:rule="has(self.credentialsSecretRef.name) && size(self.credentialsSecretRef.name) > 0",message="credentialsSecretRef.name is required"
type StorageSpec struct {
	// Endpoint is the S3 API URL, e.g. https://minio.backup.svc:9000.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint"`

	// Bucket must already exist.
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`
	Bucket string `json:"bucket"`

	// Prefix is prepended to every object key.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	Prefix string `json:"prefix,omitempty"`

	// Region is sent in request signatures.
	// +optional
	// +kubebuilder:default=us-east-1
	// +kubebuilder:validation:MaxLength=64
	Region string `json:"region,omitempty"`

	// CredentialsSecretRef names a Secret in the object's namespace holding
	// the keys accessKeyID and secretAccessKey.
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

// HookErrorPolicy says what a failing hook does to the Backup.
// +kubebuilder:validation:Enum=Fail;Continue
type HookErrorPolicy string

const (
	// HookFail fails the Backup when the hook fails.
	HookFail HookErrorPolicy = "Fail"
	// HookContinue records the failure and carries on.
	HookContinue HookErrorPolicy = "Continue"
)

// Hook runs a command inside every running pod matched by PodSelector.
type Hook struct {
	// Name identifies the hook in status and events.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// PodSelector selects the pods, in the object's namespace, to exec into.
	PodSelector metav1.LabelSelector `json:"podSelector"`

	// Container to exec into. Empty selects the pod's first container.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Container string `json:"container,omitempty"`

	// Command is executed directly (no shell is implied).
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	Command []string `json:"command"`

	// TimeoutSeconds bounds a single execution.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// OnError decides whether a failure fails the Backup.
	// +optional
	// +kubebuilder:default=Fail
	OnError HookErrorPolicy `json:"onError,omitempty"`
}

// Hooks are run around the snapshot to make it application-consistent: pre
// hooks quiesce the application, post hooks resume it. Post hooks run whenever
// pre hooks ran, even if the snapshot failed.
type Hooks struct {
	// Pre hooks run, in order, before the snapshot is cut.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	Pre []Hook `json:"pre,omitempty"`

	// Post hooks run, in order, once the snapshot is cut.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	Post []Hook `json:"post,omitempty"`
}

// DeletionPolicy says what happens to stored data when a Backup is deleted.
// +kubebuilder:validation:Enum=Delete;Retain
type DeletionPolicy string

const (
	// DeletionDelete removes snapshots and objects with the Backup.
	DeletionDelete DeletionPolicy = "Delete"
	// DeletionRetain leaves snapshots and objects in place.
	DeletionRetain DeletionPolicy = "Retain"
)

// BackupSpec describes one backup. It is immutable once created.
type BackupSpec struct {
	// Source selects the claims to capture.
	Source VolumeSource `json:"source"`

	// Driver cuts the point-in-time copy.
	Driver DriverSpec `json:"driver"`

	// Storage receives the backup manifest (and, for FileCopy, the data).
	Storage StorageSpec `json:"storage"`

	// Hooks make the copy application-consistent.
	// +optional
	Hooks *Hooks `json:"hooks,omitempty"`

	// DeletionPolicy applies when the Backup object is deleted.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}
