# Threat model

keeper copies data out of application volumes and runs commands inside
application pods. This document lists what it trusts, what it defends
against, and what it doesn't.

## Assets

- Volume data, both in transit (mover → S3) and at rest (bucket, VolumeSnapshots).
- S3 credentials, held in a Secret in each workload namespace.
- The workload itself: hooks freeze and thaw it, and a frozen filesystem is an outage.

## Trust boundaries

| Actor | Trusted for | Not trusted for |
|---|---|---|
| Cluster admin (installs the operator) | everything | – |
| Namespace user with create on Backup/BackupPolicy | choosing what to back up in *their* namespace | anything outside it (see Risk 1) |
| Object storage | availability | integrity of archives (verified by SHA-256 on restore) |
| Archive contents | – | paths, link targets, modes (validated on extract) |

## Mitigations in place

- **Archive integrity.** The SHA-256 of each archive is recorded in Backup
  status and the backup manifest. Restore hashes the whole object in a
  read-only pass before extracting anything, hashes it again while
  extracting into a staging directory, and moves entries into place only
  when both match. A mismatch leaves the target empty and, on the first
  pass, means the extractor never ran (tested).
- **Hostile archives.** Absolute paths, `..`, backslashes, NUL bytes, writes
  through planted symlinks or under a file "parent" are rejected. A
  directory entry whose name already exists as a symlink or file is
  rejected; directory modes are set via an `O_NOFOLLOW` descriptor and
  timestamps with `AT_SYMLINK_NOFOLLOW`, so no chmod or chtimes reaches a
  link target (fix round 1: before this, a symlink-then-dir pair could chmod
  a directory outside the restore root). setuid,
  setgid and sticky bits are stripped. Hard links and device nodes are
  skipped. Files are opened with `O_EXCL|O_NOFOLLOW`. Two fuzzers back this up.
- **No overwrite.** Restores create new claims only, and a pre-existing claim
  fails the Restore.
- **Least-privilege movers.** Mover pods run without a service account token,
  with a read-only root filesystem, no privilege escalation and RuntimeDefault
  seccomp. They drop every capability except DAC_OVERRIDE (backup, read-only
  mount) or CHOWN/DAC_OVERRIDE/FOWNER (restore).
- **Credentials.** S3 keys reach movers only through `secretKeyRef`. They never
  appear in Job specs, status, events or logs.
- **Hook blast radius.** An empty pod selector matches no pods. Hooks run only
  in the Backup's own namespace. Output kept in status is truncated to 512 bytes.
- **Never stuck frozen.** Post hooks run after every failure path that follows
  pre hooks, and when a Backup is deleted in any phase from `PreHooks` through
  `PostHooks`, including when the step's status write was lost to the
  deletion's resourceVersion bump (`TestDeletionMidLifecycleResumesApplication`).
  A thaw that fails is reported (`PostHookFailed` event) but cannot be retried
  forever; an operator crash between freeze and thaw is recovered on restart.

## Known risks (not mitigated)

1. **Exec as a privilege.** The operator's ServiceAccount holds `pods/exec`
   cluster-wide. Anyone who can create a Backup or BackupPolicy in a namespace
   can make the operator run an arbitrary command in any pod of that
   namespace. Grant those create rights only to people who could already exec
   into those pods. A future admission policy could restrict hook commands.
2. **Data in transit.** TLS to the bucket depends on the `https://` endpoint.
   An `http://` endpoint sends data in clear text. Custom CA bundles are not
   supported yet.
3. **No client-side encryption.** Archives are stored as gzip and are not
   encrypted. Rely on bucket-side encryption (SSE) for data at rest.
4. **Bucket write access is total.** The credentials can delete any object
   under the prefix. Object lock and immutability are not used.
5. **Pod Security.** Mover pods run as root with the capabilities above and are
   rejected in namespaces that enforce the `restricted` profile. Running them
   under `baseline` is the supported setup.
