#!/bin/bash
set -m
#WORKED WITH JASON ROTH
#HTTP endpoints for each node is its udp port + 5
# the API endpoints are {port}/logs/summary and {port}/logs/verbose
exec 1> >(tee "output.log")
exec 2>&1

# Constants
GATEWAY_HTTP_PORT=8888
GATEWAY_UDP_PORT=8000
CLUSTER_SIZE=8
NUM_OBSERVERS=1
HEARTBEAT_INTERVAL=3
LEADER_PORT=-1
CURRENT_LEADER_PORT=-1

# Step 1: Build the project and run tests
mvn clean compile

if [ $? -ne 0 ]; then
    echo "Maven build failed"
    exit 1
fi

declare -a SERVER_PIDS

# Step 2a: Start Gateway
java -cp target/classes edu.yu.cs.com3800.stage5.GatewayServer \
    $GATEWAY_HTTP_PORT $GATEWAY_UDP_PORT $CLUSTER_SIZE &
GATEWAY_PID=$!

sleep 2

# Step 2b: Start peer servers
for i in $(seq 1 $((CLUSTER_SIZE-1)))
do
    PORT=$((8000 + i * 10))
    java -cp target/classes edu.yu.cs.com3800.stage5.PeerServerImpl \
        $PORT $i $NUM_OBSERVERS $CLUSTER_SIZE &
    SERVER_PIDS[$i]=$!
done

check_and_print() {
    CLUSTER_STATUS=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info")
    if [[ $CLUSTER_STATUS == *"LEADER"* ]]; then
        echo "Current cluster status:"
        echo "$CLUSTER_STATUS"
        LEADER_PORT=$(echo "$CLUSTER_STATUS" | grep "LEADER" | awk '{print $3}')
        return 0
    fi
    return 1
}

check_leader() {
    CLUSTER_STATUS=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info")
    if [[ $CLUSTER_STATUS == *"LEADER"* ]]; then
        LEADER_PORT=$(echo "$CLUSTER_STATUS" | grep "LEADER" | awk '{print $3}')
        return 0
    fi
    return 1
}

# Wait for leader election
echo "Waiting for leader election..."
while ! check_and_print; do
    sleep 1
done

# Function to send request and get response
send_request() {
    local TEST_CLASS="package edu.yu.cs.fall2019.com3800.stage1;
    public class HelloWorld {
        public String run() {
            return \"Hello world! Request $1\";
        }
    }"

    echo "Sending request $1..."
    RESPONSE=$(curl -s -X POST -H "Content-Type: text/x-java-source" \
        -d "$TEST_CLASS" "http://localhost:$GATEWAY_HTTP_PORT/compileandrun")
    echo "Response for request $1: $RESPONSE"
}

# Send 9 initial requests
for i in {1..9}; do
    send_request $i
done

FIRST_LEADER_PORT=$LEADER_PORT

# Kill a follower
FOLLOWER_PID=$(ps aux | grep "PeerServerImpl" | grep -v "grep" | grep -v "$LEADER_PORT" | head -n 1 | awk '{print $2}')
kill -9 $FOLLOWER_PID 2>/dev/null

# Wait heartbeat interval * 10
sleep $((HEARTBEAT_INTERVAL * 15))

check_and_print

LEADER_PID=$(ps aux | grep "PeerServerImpl" | grep -v "grep" | grep " $LEADER_PORT" | awk '{print $2}')
kill -9 $LEADER_PID 2>/dev/null

sleep 1

REQUEST_PIDS=()

# Send 9 background requests
for i in {10..18}; do
  send_request "$i" &
  thePid="$!"
  REQUEST_PIDS+=("$!")
done

while true; do
    check_leader
    if [ "$LEADER_PORT" -ne "$FIRST_LEADER_PORT" ]; then
        echo "New leader elected on port $LEADER_PORT"
        break
    fi
    sleep 1
done

# Wait *only* on the request processes
for pid in "${REQUEST_PIDS[@]}"; do
  wait "$pid"
done

check_leader

# Send one final request
send_request "final"

sleep 2
# List gossip files
find . -name "*gossip*.txt"

# Function to cleanup processes
cleanup() {

    # Kill the gateway process
    if [ -n "$GATEWAY_PID" ]; then
        kill -9 $GATEWAY_PID 2>/dev/null
    fi

    # Kill each peer server process
    for pid in "${SERVER_PIDS[@]}"; do
        if [ -n "$pid" ]; then
            kill -9 $pid 2>/dev/null
        fi
    done

    # Ensure no remaining processes
    pkill -f "edu.yu.cs.com3800.stage5" 2>/dev/null

    find . -name "*.txt.lck" -exec rm -f {} +
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# Clean shutdown after all tasks complete
cleanup