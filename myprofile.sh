#!/bin/bash

# Ensure script stops on errors
set -e

# Step 1: Start Redis Clone in the Background
echo "Starting Redis clone..."
./your_program.sh &
REDIS_PID=$!

# Wait briefly to ensure the server is up
sleep 2

# Step 2: Start High-Load Benchmark
echo "Running redis-benchmark with high concurrency..."
redis-benchmark -t get,set -n 100000 -q -c 100 &
BENCHMARK_PID=$!

# Step 3: Capture CPU Profile While Benchmark Runs
echo "Capturing Memory profile..."
go tool pprof -seconds 10 -output cpu_profile.pprof http://localhost:6060/debug/pprof/allocs

# Step 4: Analyze Results
# echo "Analyzing CPU profile..."
# go tool pprof -top cpu_profile.pprof

# Step 5: Stop Redis Clone
echo "Stopping Redis clone..."
kill $REDIS_PID
wait $REDIS_PID 2>/dev/null || true

echo "Profiling complete!"
