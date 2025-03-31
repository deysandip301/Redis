# High-Performance Redis-like Key-Value Store

A lightweight, high-performance key-value store written in C++ with HTTP API support.

## Table of Contents

- [Features](#features)
- [Building and Running](#building-and-running)
- [API Documentation](#api-documentation)
- [Optimizations](#optimizations)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

## Features

- In-memory key-value storage
- HTTP REST API for GET/PUT operations
- Multi-threaded architecture with thread pooling
- Connection handling optimizations
- LRU-based eviction strategy
- TTL support for automatic key expiration
- Sorted set data structure (zset)
- Automatic memory management
- High-throughput via non-blocking I/O

## Building and Running

### Prerequisites

- Docker
- Linux/macOS/Windows with Docker support

### Building the Docker Image

```bash
# Clone the repository (if not already done)
git clone <repository-url>
cd redis

# Build the Docker image
docker build -t redis-server .
```

### Running the Server

```bash
# Run the container
docker run -p 7171:7171 --name redis-server -d redis-server

# Check logs
docker logs redis-server

# Stop the server
docker stop redis-server
```

### Pushing to Docker Hub

```bash
# Login to Docker Hub
docker login

# Tag your image
docker tag redis-server deysandip301/redis_cpp:latest

# Push to Docker Hub
docker push deysandip301/redis_cpp:latest
```

If you encounter DNS issues with Docker Hub, check your network/DNS settings:

```bash
# Verify connectivity
ping registry-1.docker.io

# If DNS issues persist, try configuring DNS servers in Docker
# Edit /etc/docker/daemon.json to include:
{
  "dns": ["8.8.8.8", "8.8.4.4"]
}
```

## API Documentation

### GET Request

Retrieve a value by key:

```
GET /get?key=exampleKey HTTP/1.1
Host: localhost:7171
```

Response (success):
```json
{
  "status": "OK",
  "key": "exampleKey",
  "value": "exampleValue"
}
```

Response (key not found):
```json
{
  "status": "ERROR",
  "message": "Key not found."
}
```

### PUT Request

Insert or update a key-value pair:

```
POST /put HTTP/1.1
Host: localhost:7171
Content-Type: application/json
Content-Length: 44

{
  "key": "exampleKey",
  "value": "exampleValue"
}
```

Response:
```json
{
  "status": "OK",
  "message": "Key inserted/updated successfully."
}
```

## Optimizations

### 1. Memory Management

- **LRU Eviction**: Automatically evicts least recently used keys when memory pressure increases
- **Memory-Aware Eviction**: Monitors system memory usage and triggers proactive eviction when usage exceeds 70%
- **Adaptive Eviction Strategy**: Adjusts eviction percentage based on memory pressure (10-40%)
- **Size-Based Prioritization**: Under high memory pressure, prioritizes evicting larger entries first
- **Efficient Data Structures**: Custom AVL tree and hashtable implementations for O(log n) operations
- **Memory Pooling**: Reuses memory allocations to reduce fragmentation

### 2. Connection Handling

- **Non-blocking I/O**: Uses event-driven architecture with `poll()` for efficient connection handling
- **Connection Rate Limiting**: Prevents resource exhaustion from too many simultaneous connections
- **TCP Tuning**:
  - TCP_NODELAY: Disables Nagle's algorithm for lower latency
  - SO_KEEPALIVE: Detects and drops dead connections
  - SO_LINGER: Configures socket closure behavior
  - Custom keepalive parameters (idle time, interval, count)

### 3. Performance Enhancements

- **Thread Pool**: Utilizes a custom thread pool to process requests concurrently
- **Zero-copy Buffers**: Uses move semantics to avoid unnecessary data copying
- **Batched Writes**: Accumulates multiple writes before sending to reduce system call overhead
- **HTTP Pipelining Support**: Processes multiple HTTP requests per connection without waiting for responses

### 4. Resource Protection

- **Connection Limits**: Enforces maximum connection count (8000) to prevent resource exhaustion
- **Rate Limiting**: Limits new connections to 1000 per second
- **Timeout Management**: Idle connections are closed after 60 seconds of inactivity
- **Controlled Eviction**: Limits the number of connections closed per event loop to avoid CPU spikes

### 5. Data Structure Optimizations

- **Hash-based Lookups**: O(1) average time complexity for key lookups
- **Balanced Trees**: AVL tree for sorted sets with O(log n) operations
- **Lazy Deletion**: Defers cleanup operations to balance throughput and memory usage

## Performance Tuning

Adjust these parameters in the source code based on your workload:

- `MAX_KEYS` (default: 25000): Maximum number of keys before count-based eviction
- `MEM_EVICTION_THRESHOLD` (default: 0.70): Memory usage threshold for triggering eviction
- `MEM_HIGH_WATER` (default: 0.85): Threshold for aggressive eviction
- `MEM_CRITICAL` (default: 0.95): Threshold for emergency eviction
- `MAX_ACTIVE_CONNS` (default: 8000): Maximum simultaneous connections
- `MAX_CONN_PER_SEC` (default: 1000): Rate limit for new connections
- `k_idle_timeout_ms` (default: 60000): Idle connection timeout in milliseconds
- Thread pool size (default: 4): Number of worker threads

## Troubleshooting

### Connection Errors

If you see `ConnectionAbortedError(10053)` or `ConnectionResetError(10054)` errors:

1. **Check Connection Limits**: Your server might be hitting connection limits or be under heavy load
2. **Verify Client Timeouts**: Increase client-side timeouts
3. **Network Issues**: Check for network problems or firewalls
4. **Resource Exhaustion**: Monitor system resources (CPU, memory)

### Memory Issues

If you encounter out-of-memory errors:

1. **Adjust MAX_KEYS**: Lower the maximum key count
2. **Monitor Memory Usage**: Use tools like `docker stats`
3. **Increase Eviction Rate**: Modify eviction percentage (currently 10%)

### Docker Issues

For Docker-related problems:

1. **Port Binding**: Ensure the host port isn't already in use
2. **Resource Limits**: Check Docker resource allocation
3. **DNS Problems**: Configure Docker's DNS settings if push/pull fails

## License

This project is licensed under the terms of the MIT license.
