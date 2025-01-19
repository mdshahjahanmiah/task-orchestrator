#!/bin/bash

# Check and handle running Redis Docker container
echo "Checking for existing Redis Docker container..."
container_name="task-orchestrator-redis"

if docker ps -a --format '{{.Names}}' | grep -q "^$container_name$"; then
    echo "Stopping and removing existing Redis container..."
    docker stop $container_name
    docker rm $container_name
fi

# Start Redis Docker container
echo "Starting Redis Docker container..."
docker run --name $container_name -p 6379:6379 -d redis:6

# Wait for Redis to be ready
echo "Waiting for Redis to be ready..."
sleep 3

# Fetch task states from Redis
echo "Fetching Task States..."
redis-cli HGETALL taskState | awk 'NR % 2 == 1 {key=$0} NR % 2 == 0 {print key ": " $0}'

echo ""

# Fetch task retries from Redis
echo "Fetching Task Retries..."
redis-cli HGETALL taskRetries | awk 'NR % 2 == 1 {key=$0} NR % 2 == 0 {print key ": Retries=" $0}'

echo ""

# Count task states
echo "Counting Task States..."
pending=$(redis-cli HGETALL taskState | grep -ci "Pending")
running=$(redis-cli HGETALL taskState | grep -ci "Running")
failed=$(redis-cli HGETALL taskState | grep -ci "Failed")
success=$(redis-cli HGETALL taskState | grep -ci "Success")
echo "Pending: $pending"
echo "Running: $running"
echo "Failed: $failed"
echo "Success: $success"

echo ""

# List tasks exceeding retry limits
echo "Tasks Exceeding Retry Limit..."
redis-cli HGETALL taskRetries | awk 'NR % 2 == 1 {key=$0} NR % 2 == 0 {if($0>=3) print "Task ID: " key ", Retries: " $0}'

echo ""
echo "Script execution completed."
