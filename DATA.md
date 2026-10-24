# Data and third-party files

keeper ships no datasets, and no test or demo uses external data.

| What | Where | Source | License |
|---|---|---|---|
| Volume contents in tests and the demo | generated at runtime in temp dirs (`writeFixture`, `workload`, `writeRows`) | synthetic | n/a |
| Fuzz regression seed | `internal/mover/testdata/fuzz/FuzzRestore/` | produced by `go test -fuzz` | MIT (this repo) |
| VolumeSnapshot / VolumeSnapshotContent / VolumeSnapshotClass CRDs | `config/test-crds/` | copied unmodified from `github.com/kubernetes-csi/external-snapshotter/client/v8@v8.7.0/config/crd` | Apache-2.0 |
| envtest binaries (kube-apiserver, etcd v1.37.0) | not committed; downloaded by `setup-envtest` to its per-user cache | kubernetes-sigs/controller-tools envtest releases | Apache-2.0 |
| MinIO server for `make integration` | not committed; pulled as `chainguard/minio@sha256:ae73…a113` | Chainguard build of MinIO | AGPL-3.0 (server only; not linked) |

The vendored CRDs are used only to install the VolumeSnapshot API into envtest.
The operator imports the matching Go types from the same module version.
