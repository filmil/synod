# Synod Paxos Agent

[![Build and Test](https://github.com/filmil/synod/actions/workflows/build-test.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/build-test.yml)
[![Release Agent](https://github.com/filmil/synod/actions/workflows/release.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/release.yml)

## What it does

Synod is a distributed Paxos coordination agent implemented in Go. It manages a highly available, synchronized Key-Value store across a network of peers using the Paxos consensus algorithm. It allows multiple dynamically joining network nodes to agree on a shared state, ensuring fault tolerance and consistency across the cluster.

## Quickstart

This quickstart shows how to download the repository and quickly start 3 synod agents which talk to each other and are already set up to work properly. It establishes state directories for each agent following the XDG base directory specification.

```bash
#!/bin/bash
set -e

# Clone the repository and enter it
git clone https://github.com/filmil/synod.git
cd synod

# Determine the base directory for state following XDG conventions
STATE_BASE="${XDG_STATE_HOME:-$HOME/.local/state}/synod"

# Create state directories for the 3 agents
mkdir -p "$STATE_BASE/agent1"
mkdir -p "$STATE_BASE/agent2"
mkdir -p "$STATE_BASE/agent3"

echo "Starting Agent 1 (Bootstrap)..."
bazel run //cmd/agent -- \
  --state_dir="$STATE_BASE/agent1" \
  --grpc_addr=":50101" \
  --http_addr=":8081" &
PID1=$!

# Give the first agent a moment to start
sleep 3

echo "Starting Agent 2..."
bazel run //cmd/agent -- \
  --state_dir="$STATE_BASE/agent2" \
  --grpc_addr=":50102" \
  --http_addr=":8082" \
  --peer="127.0.0.1:50101" &
PID2=$!

echo "Starting Agent 3..."
bazel run //cmd/agent -- \
  --state_dir="$STATE_BASE/agent3" \
  --grpc_addr=":50103" \
  --http_addr=":8083" \
  --peer="127.0.0.1:50101" &
PID3=$!

echo ""
echo "Cluster is running! Press Ctrl+C to stop."
echo "View Agent 1: http://localhost:8081"
echo "View Agent 2: http://localhost:8082"
echo "View Agent 3: http://localhost:8083"
echo ""

# Wait for all background processes
wait $PID1 $PID2 $PID3
```

## Features & Current State
- **Key-Value Store:** Implements standard Paxos consensus bound to unix-path keys (e.g. `/system/config`).
- **Persistence:** SQLite backed via `mattn/go-sqlite3`.
- **Dynamic Membership:** Nodes can dynamically join the cluster via gRPC. Network membership itself is managed within the KV Store under the `/_internal/peers` key using optimistic concurrency/versioning.
- **Identities & Short Names:** Agents are assigned a unique UUID alongside a randomly selected human-readable short name for easier identification in dashboards.
- **Dynamic Port Selection:** If a requested gRPC or HTTP port is unavailable, the agent automatically attempts to find and bind to a free port.
- **Sync:** Nodes continuously synchronize their KV store versions in the background, recovering any missing keys or resolving out-of-date versions.
- **Peer Health Monitoring:** Periodically pings all known peers to monitor availability. If a peer fails to respond, a Paxos proposal is automatically initiated to remove the unresponsive node from the cluster membership.
- **Web UI:** Includes a simple embedded HTTP dashboard (styled with Bootstrap) to inspect the node's local view of participants (always includes itself), read the raw Key-Value Store, examine the pretty-printed RPC message log, and issue custom data proposals.
- **Graceful Error Handling:** Top-level application errors safely abort the process with a clean exit code rather than crashing with panic traces.

## Running the Agent

You can start the agent using Bazel. Ensure you provide it a local directory for the SQLite state.

**Start the first agent (bootstrap node):**
```bash
bazel run //cmd/agent -- \
  --state_dir="$(pwd)/local/agent1" \
  --grpc_addr=":50101" \
  --http_addr=":8081"
```

**Start a second agent and join the cluster:**
```bash
bazel run //cmd/agent -- \
  --state_dir="$(pwd)/local/agent2" \
  --grpc_addr=":50102" \
  --http_addr=":8082" \
  --peer="127.0.0.1:50101"
```

Once running, you can monitor the local state of either node by navigating your browser to `http://localhost:8081` or `http://localhost:8082`.
