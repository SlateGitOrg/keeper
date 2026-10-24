// Package v1alpha1 contains the API types of the backup.keeper.dev group:
// BackupPolicy (a schedule plus retention), Backup (one point-in-time copy of a
// set of PersistentVolumeClaims) and Restore (materialising a Backup into new
// claims).
//
// +kubebuilder:object:generate=true
// +groupName=backup.keeper.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the API group and version of every type in this package.
var GroupVersion = schema.GroupVersion{Group: "backup.keeper.dev", Version: "v1alpha1"}

var (
	// SchemeBuilder registers the package's types with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme adds the package's types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&BackupPolicy{}, &BackupPolicyList{},
		&Backup{}, &BackupList{},
		&Restore{}, &RestoreList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
