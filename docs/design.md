# Slinky

## Goal

Build a lightweight continuous folder sync engine.

Easy folder sync between devices and the cloud.

Intuitive conflict resolution. Never lose work.

Totally self hosted. Easy setup. No third party managed service.

## Model

- Model the repo as a coordinator-sequenced log of deltas with periodic snapshots.
- The coordinator assigns the canonical order of published deltas.
- Deltas are the hot-path sync unit. They describe filesystem changes and do not require a
  full merkle root on every change.
- Snapshots are periodic checkpoints. They capture a full tree state at a specific log
  position and support recovery, compaction, and catch-up.
- Steady-state sync replays deltas after the local applied log position.
- If a device is too far behind, sync falls back to snapshot plus remaining delta tail.
- The coordinator orders concurrent changes, but devices detect semantic conflicts while
  applying later deltas against the ordered state produced by earlier ones.

## Design

- Coordinator-backed sync by default. Devices sync through a coordinator service that is
  easy to set up.
- Direct peer-to-peer transfer is automatic when possible. Users should not need to think
  about it beyond optional network config.
- The coordinator is bundled into the same Rust binary and is intended to be run by the
  user, with setup focused on making self-hosting easy.
- The sync protocol and conflict semantics are the same whether bytes flow through the
  coordinator or directly between peers.
- Backup: support cloud backup to S3 etc. Configurable version history?
- Content-addressable storage: Build a merkle tree of the folder from hashed blocks.
- Merkle DAG: Git-style causal graph for object versioning based on tree hashes.
  - Break files into blocks.
  - Merkle tree of block, file, folder, commit hashes.
  - Track hashes and parent hashes of commits for history.
  - Common ancestors to identify conflicts.
  - Frontier vectors: Devices can keep a vector clock of current HEAD hashes of all
    devices, similar to git branches.
  - Use rolling hashes to support small file modifications without rebuilding all blocks.
  - Devicees share snapshot hash/diffs with coordinator. Hybrid Merkle/journal.
- Conflict resolution
  - Conflict occurs when two devices edit the same file from the same parent hash.
  - Fork-and-rename. We do not delete or overwrite. We create copies of files that
    conflict.
  - CRDT style approach. Essentially treat the folder namespace as a CDRT.
- Garbage collection - delete unreferenced blocks/history.

## Architecture

- Coordinator-first design. Coordinator routes notifications, metadata.
- Devices register with coordinator, announce changes, and receive peer changes.
- Blob data flows through coordinator, cloud backup, or peer-to-peer.
- Device-centric sync engine. Core protocol is on devices.
- Devices store enough metadata to stop/start/sync without rebuilding state from scratch.
- One sync root per repo for MVP.

## Implementation

- single rust server runs on each device to sync
- actor/message-passing style concurrency for easy testing (mpsc channels)
- keep config/metadata in a home folder (~/.slinky), likely backed by SQLite on device
- cleanup stale blocks unless needed for sync. no metadata explosion.

## Data Model

- SeqNo
  - coordinator-assigned log sequence number

- Config
  - sync root
  - repo ID
  - device ID
  - public key
  - private key path
  - local snapshot
  - applied seqno
  - remote frontier
  - pending transfers
  - transfer checkpoints
  - conflict records
  - tombstones

- Snapshot
  - hash
  - root tree hash
  - parent snapshot hashes
  - device ID
  - timestamp

- Checkpoint
  - snapshot hash
  - seqno

- Delta
  - hash
  - seqno
  - base seqno
  - device ID
  - device seqno
  - timestamp
  - changes

- Change
  - create dir
  - delete path
  - move path
  - update file

- FileChange
  - path
  - base file hash
  - file

- TreeDiff
  - entries
  - name -> change
  - `delete | tree | file`

- Tree
  - hash
  - entries
  - child names
  - child types
  - child hashes

- File
  - path
  - hash
  - blob hashes

- Blob
  - hash
  - size

- Object
  - delta | snapshot | tree | file | blob

## Auth

- Devices identify themselves with public/private key pairs.
- Coordinators authenticate devices with signed challenges.
- Peers authenticate each other the same way for direct transfer.
- Repo membership determines which device keys are allowed to sync.
- Setup should make it easy to add and trust a new device.

## Retention

- Retention should be frontier-based.
- Devices report the latest frontier they have fully incorporated.
- Coordinators retain snapshots and referenced objects until no device frontier needs them.
- Garbage collection should start from retained snapshots and delete only unreachable objects.
- MVP should keep retention conservative.

## Frontiers

- A frontier tracks the latest known tip snapshot for each device.
- Devices use frontiers to understand which remote tips are furthest ahead.
- Sync should target frontier tips, not "a peer". Peers are only announcement sources.
- A tip that is an ancestor of another known tip is not interesting to sync toward.
- Over time, devices should reduce the known tips to the maximal frontier set.
- To sync, devices gather the known remote tips and drop any tip already incorporated locally.
- Devices then drop any tip that is behind another known tip.
- If one maximal remote tip remains, devices reconcile toward that snapshot.
- If several maximal remote tips remain, devices treat them as concurrent heads and merge later.
- If one frontier tip remains, devices can reconcile toward it directly.
- If several unrelated frontier tips remain, devices have concurrent heads to merge later.

## Conflict Resolution

- Conflict resolution stays file-level in MVP even though storage is chunk-based.

- General
  - no silent overwrite
  - no silent delete of concurrent edits
  - if content is identical, no conflict
  - if one side changed and the other did not, apply the change
  - materialize conflicts as renamed sibling files

- edit / edit
  - detect when both sides changed the same file hash from the same parent snapshot
  - keep both versions

- edit / delete
  - detect when one side changed a file and the other deleted it from the same parent
    snapshot
  - keep the edited version and record the delete

- rename / edit
  - detect when one side changed path and the other changed file hash for the same file
  - preserve both changes and fork if needed

- rename / rename
  - detect when both sides changed the path of the same file to different destinations
  - keep both paths

- delete / rename
  - detect when one side deleted a file and the other renamed it from the same parent
    snapshot
  - keep the renamed version and record the delete

- file / directory
  - detect when one side creates or updates a file at a path and the other creates or
    keeps a directory at that path
  - preserve both and fork the file path

## Sync Flow

- startup scan
  - scan the sync root on startup to build or verify local state

- watch
  - watch the sync root continuously for local changes

- coalesce
  - batch rapid local edits before publishing a snapshot

- stage
  - chunk changed files
  - write blobs
  - build path-level journal
  - rebuild file and tree objects
  - create a staged snapshot

- publish
  - upload staged metadata and blobs
  - publish the snapshot and change journal when required objects are available

- notify
  - coordinator pushes update notifications to devices when new snapshots are published
  - peers may also announce available snapshots directly when connected

- fetch
  - fetch newly announced snapshots
  - fetch the change journal for the base/target snapshots the device wants to reconcile
  - fetch missing blobs from the blob store

- reconcile
  - plan local updates before modifying the filesystem
  - detect conflicts and choose resolutions

- apply
  - apply the reconciliation plan
  - update local metadata and frontier atomically

- maintain
  - retry failed transfers
  - verify local objects
  - garbage collect conservatively

## Components

- Device
  - watcher
  - startup scanner
  - coalescer
  - journal builder
  - chunker
  - object store
  - metadata store
  - snapshot builder
  - sync client
  - reconciler
  - applier

- Coordinator
  - auth and device registry
  - obj store
  - repo membership
  - snapshot index
  - change journal index
  - object availability index
  - peer discovery
  - push notifications
  - transfer coordination

- Storage backends
  - coordinator-managed storage
  - cloud object storage
  - peer-to-peer transfer

## UI

Simple.

```shell
slinky setup  # configure folder etc.
slinky add-device
slinky start  # run sync server
slinky status # check sync status
slinky stop   # stop sync/server
```
