# Synod Paxos Agent

[![Build and Test](https://github.com/filmil/synod/actions/workflows/build-test.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/build-test.yml)
[![Release Agent](https://github.com/filmil/synod/actions/workflows/release.yml/badge.svg)](https://github.com/filmil/synod/actions/workflows/release.yml)

Synod is a distributed Paxos coordination agent implemented in Go, managing a synchronized Key-Value store across dynamically joining network peers.

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
