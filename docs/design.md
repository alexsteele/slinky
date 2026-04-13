# Slinky

## Goal

Build a lightweight continuous folder sync engine.

Easy folder sync between devices and the cloud.

Intuitive conflict resolution. Never lose work.

Totally self hosted. Easy setup. No third party managed service.

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
  - Frontier vectors: Devices can keep a vector clock of current HEAD hashes of all devices, similar to git branches.
  - Use rolling hashes to support small file modifications without rebuilding all blocks.
- Conflict resolution
  - Conflict occurs when two devices edit the same file from the same parent hash.
  - Fork-and-rename. We do not delete or overwrite. We create copies of files that conflict.
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
- keep config/metadata in a home folder (~/.slinky)
- cleanup stale blocks unless needed for sync. no metadata explosion.

## Data Model

- Config
  - sync root
  - repo ID
  - device ID
  - public key
  - private key path
  - local snapshot
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

## Auth

- Devices identify themselves with public/private key pairs.
- Coordinators authenticate devices with signed challenges.
- Peers authenticate each other the same way for direct transfer.
- Repo membership determines which device keys are allowed to sync.
- Setup should make it easy to add and trust a new device.

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
  - rebuild file and tree objects
  - create a staged snapshot

- publish
  - upload staged objects
  - publish the snapshot when required objects are available

- notify
  - coordinator pushes update notifications to devices when new snapshots are published
  - peers may also announce available snapshots directly when connected

- fetch
  - fetch newly announced snapshots
  - fetch missing objects from storage backends or peers

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
  - chunker
  - object store
  - metadata store
  - snapshot builder
  - sync client
  - reconciler
  - applier

- Coordinator
  - auth and device registry
  - repo membership
  - snapshot index
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
