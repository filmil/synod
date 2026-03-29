#!/bin/bash
set -e

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
