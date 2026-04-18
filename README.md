# slinky

slinky is a self-hosted continuous file sync engine.

status: experimental project to test out GPT codex

## Overview

slinky syncs a folder between devices using an ordered changelog with periodic snapshots.

Devices watch the folder, publish deltas to a relay, and replay accepted changes in order. The relay
assigns seqnos, announces updates, and serves checkpoints for catch-up. Devices handle conflicts
with a lossless resolution protocol.

Device metadata is stored in sqlite under `~/.slinky` and your file blocks can be stored in the
cloud.

And **yes**, sorry, you need to run a cloud relay server. It's not fully peer-to-peer.

```text
                    +----------------------+
                    |        Relay         |
                    |----------------------|
                    | assign seqnos        |
                    | publish updates      |
                    | serve checkpoints    |
                    +----------+-----------+
                              ^
                deltas / acks | notifications / catch-up
                              |
          +-------------------+-------------------+
          |                                       |
          |                                       |
+---------+---------+                   +---------+---------+
|      Device A     |                   |      Device B     |
|-------------------|                   |-------------------|
| watch local fs    |                   | watch local fs    |
| chunk file data   |                   | chunk file data   |
| queue deltas      |                   | queue deltas      |
| apply ordered log |                   | apply ordered log |
| make snapshots    |                   | make snapshots    |
+---------+---------+                   +---------+---------+
          |                                       |
          v                                       v
      +----+-----+                            +----+----+
      | Folder A |                            | Folder B |
      +----------+                            +---------+
```

## Data Model

```
Snapshot   full folder state
Tree       directory structure
File       file metadata + blob refs
Blob       chunk data

Delta      ordered change record
FileOp     create / remove / move / modify
SeqNo      relay-assigned sequence number
Revision   device-assigned revision
Checkpoint snapshot + accepted SeqNo
```

## Auth

Devices are identified by a keypair. Communications go over encrypted TCP/http.

## Commands

```sh
slinky setup  # setup your device
slinky start  # start sync server
slinky start relay
```
