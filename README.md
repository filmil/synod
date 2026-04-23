# Synod Paxos Agent

[![Build and Test](https://github.com/filmil/synod/actions/workflows/build-test.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/build-test.yml)
[![Release Agent](https://github.com/filmil/synod/actions/workflows/release.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/release.yml)
[![Publish to my Bazel registry](https://github.com/filmil/synod/actions/workflows/publish.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/publish.yml)
[![Publish on Bazel Central Registry](https://github.com/filmil/synod/actions/workflows/publish-bcr.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/publish-bcr.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/filmil/synod.svg)](https://pkg.go.dev/github.com/filmil/synod)

## What it does

Synod is a distributed Paxos coordination agent implemented in Go. It manages a
highly available, synchronized Key-Value store across a network of peers using
the Paxos consensus algorithm. It allows multiple dynamically joining network
nodes to agree on a shared state, ensuring fault tolerance and consistency
across the cell.

## Quickstart

This quickstart shows how to download the repository and quickly start 3 synod
agents which talk to each other and are already set up to work properly. It
establishes state directories for each agent following the XDG base directory
specification.

You can also run the extracted script locally by executing `./tools/try_it.bash`
from the root of the repository.

```bash
#!/bin/bash
set -e

# Determine the base directory for state following XDG conventions
STATE_BASE="${XDG_STATE_HOME:-$HOME/.local/state}/synod"

bazel build //cmd/agent

readonly _agent_bin="./bazel-bin/cmd/agent/agent_/agent"

# Create state directories for the 3 agents
mkdir -p "$STATE_BASE/agent1"
mkdir -p "$STATE_BASE/agent2"
mkdir -p "$STATE_BASE/agent3"

echo "Starting Agent 1 (Bootstrap)..."
timeout 5m "${_agent_bin}" \
  --logtostderr \
  --state_dir="$STATE_BASE/agent1" \
  --grpc_addr=":50101" \
  --http_addr=":8081" &
PID1=$!

# Give the first agent a moment to start
sleep 3

echo "Starting Agent 2..."
timeout 5m "${_agent_bin}" \
  --logtostderr \
  --state_dir="$STATE_BASE/agent2" \
  --peer=":50101" &
PID2=$!

echo "Starting Agent 3..."
timeout 5m "${_agent_bin}" \
  --logtostderr \
  --state_dir="$STATE_BASE/agent3" \
  --grpc_addr=":50103" \
  --http_addr=":8083" \
  --peer=":50101" &
PID3=$!

echo ""
echo "Cell is running!"
echo "View Agent 1 at: http://localhost:8081"
echo "Navigate to other agents from there"

# Wait for all background processes
wait $PID1 $PID2 $PID3
```

## Features & Current State

- **Key-Value Store:** Implements standard Paxos consensus bound to unix-path
  keys (e.g. `/system/config`).
- **Persistence:** SQLite backed via `mattn/go-sqlite3`.
- **Dynamic Membership:** Nodes can dynamically join the cell via gRPC.
  Network membership itself is managed within the KV Store under the
  `/_internal/peers` key using optimistic concurrency/versioning.
- **Identities & Short Names:** Agents are assigned a unique UUID alongside a
  randomly selected human-readable short name for easier identification in
  dashboards.
- **Dynamic Port Selection:** If a requested gRPC or HTTP port is unavailable,
  the agent automatically attempts to find and bind to a free port.
- **Sync & Peer Health Monitoring:** Nodes continuously synchronize their KV
  store versions in the background, recovering any missing keys or resolving
  out-of-date versions. Periodically pings all known peers to monitor
  availability. If a peer fails to respond, a Paxos proposal is automatically
  initiated to remove the unresponsive node from the cell membership.
- **User API:** Provides a dedicated gRPC and HTTP interface for users to
  interact with the cell. Supports reading values with specific quorum
  requirements (Local, Majority, All) and writing values using Compare-and-Swap
  (CAS) semantics via `CompareAndWrite`.
- **Hierarchical Locking:** Allows users to acquire distributed locks on key
  paths. If a path contains `_lockable` segments (e.g.,
  `/foo/_lockable/bar/_lockable/baz`), the system automatically acquires and
  verifies locks sequentially from top to bottom. Operations on locked paths
  are strictly rejected if the lock is held by another agent or expired.
- **Robust Retries & Quorums:** Network and consensus operations utilize
  exponential backoff for transient failures. Lock acquisitions and writes
  require a `QuorumMajority`, while releasing locks requires a `QuorumAll`
  consensus to guarantee cell-wide consistency.
- **Web UI & Introspection:** Includes an embedded HTTP dashboard to inspect
  participants, read the Key-Value Store, and examine the RPC message log. The
  UI features a dedicated User API panel with a real-time table of ongoing
  requests, JSON pretty-printing, and runtime introspection (CPU/memory/stack).
- **Graceful Error Handling:** Top-level application errors safely abort the
  process with a clean exit code rather than crashing with panic traces.

## Running the Agent

You can start the agent using Bazel. Ensure you provide it a local existing
directory for the SQLite state.

**Start the first agent (bootstrap node):**

```bash
bazel run //cmd/agent -- \
  --state_dir="$(pwd)/local/agent1" \
  --grpc_addr=":50101" \
  --http_addr=":8081"
```

**Start a second agent and join the cell:**

```bash
bazel run //cmd/agent -- \
  --state_dir="$(pwd)/local/agent2" \
  --peer=":50101"
```

Once running, you can monitor the local state of the first node by navigating
your browser to `http://localhost:8081`, and you can follow from there to other
nodes. You can also specify the grpc and http hostports for the other agent if
you require it to appear at a pre-determined hostport.
