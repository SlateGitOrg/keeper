package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestorePhase is the coarse lifecycle state of a Restore.
// +kubebuilder:validation:Enum=Pending;Restoring;Completed;Failed
type RestorePhase string

// Restore phases.
const (
	RestorePending   RestorePhase = "Pending"
	RestoreRestoring RestorePhase = "Restoring"
	RestoreCompleted RestorePhase = "Completed"
	RestoreFailed    RestorePhase = "Failed"
)

// Terminal reports whether the phase is final.
func (p RestorePhase) Terminal() bool { return p == RestoreCompleted || p == RestoreFailed }

// RestoreSpec materialises a Backup into new PersistentVolumeClaims in the
// Backup's namespace. Existing claims are never overwritten.
type RestoreSpec struct {
	// BackupName is the Completed Backup to restore.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BackupName string `json:"backupName"`

	// PVCNamePrefix is prepended to each source claim name to form the
	// restored claim name.
	// +optional
	// +kubebuilder:default=restored-
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*)?$`
	// +kubebuilder:validation:MaxLength=63
	PVCNamePrefix string `json:"pvcNamePrefix,omitempty"`

	// Volumes restricts the restore to these source claims. Empty restores all.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=32
	Volumes []string `json:"volumes,omitempty"`

	// StorageClassName overrides the source claims' storage class.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// VolumeRestore is the per-claim record of a Restore.
type VolumeRestore struct {
	// SourcePVC is the claim name recorded in the Backup.
	SourcePVC string `json:"sourcePVC"`
	// TargetPVC is the created claim.
	TargetPVC string `json:"targetPVC"`
	// JobName is the restore mover Job (FileCopy driver).
	// +optional
	JobName string `json:"jobName,omitempty"`
	// Ready is true once the data is in place.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// RestoreStatus is the observed state of a Restore.
type RestoreStatus struct {
	// Phase is the lifecycle state.
	// +optional
	Phase RestorePhase `json:"phase,omitempty"`
	// StartTime is when the operator first acted on the Restore.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// CompletionTime is when the Restore reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// Volumes holds per-claim progress.
	// +optional
	// +listType=map
	// +listMapKey=sourcePVC
	Volumes []VolumeRestore `json:"volumes,omitempty"`
	// Message explains the current phase.
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

// Restore materialises a Backup into new PersistentVolumeClaims.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rs,categories=keeper
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.backupName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Restore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec   RestoreSpec   `json:"spec"`
	Status RestoreStatus `json:"status,omitempty"`
}

// RestoreList is a list of Restores.
//
// +kubebuilder:object:root=true
type RestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Restore `json:"items"`
}
