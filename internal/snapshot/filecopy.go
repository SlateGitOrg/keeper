package snapshot

import (
	"context"
	"errors"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/mover"
	"github.com/archit2501/keeper/internal/storage"
)

// ComponentMover labels mover Jobs.
const ComponentMover = "mover"

// MoverCommand is the entrypoint of the operator image's mover mode.
var MoverCommand = []string{"/keeper", "mover"}

// FileCopy runs one mover Job per claim. The Job mounts the claim and streams
// a tar.gz archive plus a manifest to object storage. The copy is
// application-consistent only while pre hooks keep the application quiesced,
// because the Job reads the live filesystem.
type FileCopy struct {
	Deps
	// Image is the operator image, which contains the mover.
	Image string
}

// BackupJobName is the mover Job for a volume of a Backup.
func BackupJobName(backup, pvc string) string { return ChildName(backup, pvc, "backup") }

// RestoreJobName is the mover Job for a volume of a Restore.
func RestoreJobName(restore, pvc string) string { return ChildName(restore, pvc, "restore") }

func layout(spec keeperv1.StorageSpec) storage.Layout { return storage.Layout{Prefix: spec.Prefix} }

// Start implements Driver.
func (d *FileCopy) Start(ctx context.Context, b *keeperv1.Backup) error {
	l := layout(b.Spec.Storage)
	for _, v := range b.Status.Volumes {
		args := mover.Args{
			Mode:        mover.ModeBackup,
			Path:        mover.MountPath,
			PVCName:     v.PVCName,
			ArchiveKey:  l.Archive(b.Namespace, b.Name, v.PVCName),
			ManifestKey: l.VolumeManifest(b.Namespace, b.Name, v.PVCName),
		}
		job := d.moverJob(b.Namespace, BackupJobName(b.Name, v.PVCName), v.PVCName, true, args, b.Spec.Storage, b.Spec.Driver.FileCopy)
		job.Labels[keeperv1.LabelBackup] = b.Name
		job.Spec.Template.Labels[keeperv1.LabelBackup] = b.Name
		affinity, err := d.colocate(ctx, b.Namespace, v.PVCName)
		if err != nil {
			return err
		}
		job.Spec.Template.Spec.Affinity = affinity
		if err := d.createJob(ctx, b, job); err != nil {
			return err
		}
	}
	return nil
}

// colocate pins the mover to the node of a running pod that already mounts
// the claim, so ReadWriteOnce volumes can be read without a multi-attach.
func (d *FileCopy) colocate(ctx context.Context, namespace, pvc string) (*corev1.Affinity, error) {
	var pods corev1.PodList
	if err := d.Client.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	for _, p := range pods.Items {
		if p.Spec.NodeName == "" || p.Status.Phase != corev1.PodRunning || p.Labels[keeperv1.LabelComponent] == ComponentMover {
			continue
		}
		for _, vol := range p.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvc {
				return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{p.Spec.NodeName},
						}},
					}}},
				}}, nil
			}
		}
	}
	return nil, nil
}

func (d *FileCopy) createJob(ctx context.Context, owner client.Object, job *batchv1.Job) error {
	if err := controllerutil.SetControllerReference(owner, job, d.Scheme); err != nil {
		return fmt.Errorf("own job %s: %w", job.Name, err)
	}
	if err := d.Client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create job %s: %w", job.Name, err)
	}
	return nil
}

// moverJob builds a hardened Job running the mover against claim.
func (d *FileCopy) moverJob(namespace, name, claim string, readOnly bool, args mover.Args, st keeperv1.StorageSpec, cfg *keeperv1.FileCopyDriverSpec) *batchv1.Job {
	deadline, backoff := int64(3600), int32(1)
	var resources corev1.ResourceRequirements
	if cfg != nil {
		if cfg.ActiveDeadlineSeconds > 0 {
			deadline = cfg.ActiveDeadlineSeconds
		}
		backoff = cfg.BackoffLimit
		resources = cfg.Resources
	}
	// Backups only need to read every file; restores need to recreate
	// ownership and permissions. Nothing else is granted. DAC_OVERRIDE (not
	// DAC_READ_SEARCH) is used for reads because it is on the Pod Security
	// "baseline" allow-list; the read-only mount keeps it from writing.
	caps := []corev1.Capability{"DAC_OVERRIDE"}
	if !readOnly {
		caps = []corev1.Capability{"CHOWN", "DAC_OVERRIDE", "FOWNER"}
	}
	secretEnv := func(env, key string) corev1.EnvVar {
		return corev1.EnvVar{Name: env, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: st.CredentialsSecretRef, Key: key,
		}}}
	}
	lbls := map[string]string{keeperv1.LabelComponent: ComponentMover}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: copyLabels(lbls)},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To(backoff),
			ActiveDeadlineSeconds:   ptr.To(deadline),
			TTLSecondsAfterFinished: ptr.To(int32(86400)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: copyLabels(lbls)},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext:              &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name:    "mover",
						Image:   d.Image,
						Command: MoverCommand,
						Args:    args.Argv(),
						Env: []corev1.EnvVar{
							{Name: mover.EnvEndpoint, Value: st.Endpoint},
							{Name: mover.EnvBucket, Value: st.Bucket},
							{Name: mover.EnvRegion, Value: st.Region},
							secretEnv(mover.EnvAccessKey, storage.SecretAccessKeyID),
							secretEnv(mover.EnvSecretKey, storage.SecretSecretAccessKey),
						},
						Resources:    resources,
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: mover.MountPath, ReadOnly: readOnly}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: caps},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claim, ReadOnly: readOnly,
						}},
					}},
				},
			},
		},
	}
}

func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Observe implements Driver. For FileCopy the copy is cut and durable at the
// same moment: when the mover Job has completed and its manifest is stored.
func (d *FileCopy) Observe(ctx context.Context, b *keeperv1.Backup, store storage.ObjectStore) (Progress, error) {
	p := Progress{Cut: true, Done: true}
	for _, v := range b.Status.Volumes {
		v.JobName = BackupJobName(b.Name, v.PVCName)
		if v.Ready {
			// Recorded as durable by an earlier Observe; the Job may since
			// have been garbage collected (TTL), which is fine.
			p.Volumes = append(p.Volumes, v)
			continue
		}
		var job batchv1.Job
		err := d.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: v.JobName}, &job)
		if err != nil && !apierrors.IsNotFound(err) {
			return Progress{}, err
		}
		// NotFound right after Start is the informer cache lagging behind
		// the create; Start re-creates a Job that is really gone.
		done, failed, msg := false, false, ""
		if err == nil {
			done, failed, msg = jobState(&job)
		}
		switch {
		case failed:
			return Progress{Failed: true, Message: msg}, nil
		case !done:
			p.Cut, p.Done = false, false
		default:
			var m mover.VolumeManifest
			key := layout(b.Spec.Storage).VolumeManifest(b.Namespace, b.Name, v.PVCName)
			if err := storage.GetJSON(ctx, store, key, &m); err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return Progress{Failed: true, Message: fmt.Sprintf("mover job %s completed without writing %s", v.JobName, key)}, nil
				}
				return Progress{}, fmt.Errorf("read volume manifest: %w", err)
			}
			v.ObjectKey, v.SizeBytes, v.Files, v.SHA256, v.Ready = m.ArchiveKey, m.ArchiveBytes, m.Entries, m.SHA256, true
			if m.FinishedAt.After(p.CutTime) {
				p.CutTime = m.FinishedAt
			}
		}
		p.Volumes = append(p.Volumes, v)
	}
	return p, nil
}

// Cleanup implements Driver. Mover Jobs are owned by the Backup and garbage
// collected with it; stored objects are removed by the Backup controller.
func (d *FileCopy) Cleanup(context.Context, *keeperv1.Backup) error { return nil }

// StartRestore implements Driver: create an empty claim per volume and a
// mover Job that extracts the archive into it.
func (d *FileCopy) StartRestore(ctx context.Context, r *keeperv1.Restore, b *keeperv1.Backup, vols []RestoreVolume) error {
	for _, rv := range vols {
		if err := createClaim(ctx, d.Client, r, targetClaim(r, rv)); err != nil {
			return err
		}
		args := mover.Args{Mode: mover.ModeRestore, Path: mover.MountPath, ArchiveKey: rv.Source.ObjectKey, SHA256: rv.Source.SHA256}
		job := d.moverJob(r.Namespace, RestoreJobName(r.Name, rv.Source.PVCName), rv.Target, false, args, b.Spec.Storage, b.Spec.Driver.FileCopy)
		job.Labels[keeperv1.LabelRestore] = r.Name
		job.Spec.Template.Labels[keeperv1.LabelRestore] = r.Name
		if err := d.createJob(ctx, r, job); err != nil {
			return err
		}
	}
	return nil
}

// ObserveRestore implements Driver.
func (d *FileCopy) ObserveRestore(ctx context.Context, r *keeperv1.Restore, _ *keeperv1.Backup, vols []RestoreVolume) (RestoreProgress, error) {
	p := RestoreProgress{Done: true, Ready: map[string]bool{}, Jobs: map[string]string{}}
	for _, rv := range vols {
		name := RestoreJobName(r.Name, rv.Source.PVCName)
		p.Jobs[rv.Target] = name
		var job batchv1.Job
		if err := d.Client.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: name}, &job); err != nil {
			if apierrors.IsNotFound(err) {
				p.Done = false
				continue
			}
			return RestoreProgress{}, err
		}
		done, failed, msg := jobState(&job)
		switch {
		case failed:
			return RestoreProgress{Failed: true, Message: msg, Jobs: p.Jobs}, nil
		case done:
			p.Ready[rv.Target] = true
		default:
			p.Done = false
		}
	}
	return p, nil
}
