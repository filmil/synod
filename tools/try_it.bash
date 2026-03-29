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
