# Dask Cluster Integration Tests

This directory contains integration tests for async-cassandra-dataframe running on a distributed Dask cluster with a multi-node Cassandra 5 cluster.

## Overview

These tests validate that the library works correctly in a production-like distributed environment with:
- Multiple Dask workers across different nodes
- Multi-node Cassandra 5 cluster
- Real network communication
- Distributed task execution
- Memory limits and spilling

## Prerequisites

- Docker or Podman installed
- At least 8GB of free memory
- Ports 8786, 8787, 9042-9044 available
- Python environment with test dependencies

## Running the Tests

### 1. Start the Cluster Environment

Using Docker:
```bash
cd tests/cluster_dask_integration
docker-compose up -d
```

Using Podman:
```bash
cd tests/cluster_dask_integration
USE_PODMAN=true podman-compose up -d
```

Wait for all services to be healthy (approximately 60 seconds):
```bash
# Check Cassandra cluster status
docker exec cassandra-node1 nodetool status

# Check Dask cluster status
curl http://localhost:8787/api/v1/health
```

### 2. Run the Tests

These tests are skipped by default. To run them:

```bash
# Run all cluster integration tests
RUN_CLUSTER_TESTS=true pytest tests/cluster_dask_integration/ -v

# Run specific test file
RUN_CLUSTER_TESTS=true pytest tests/cluster_dask_integration/test_cluster_connectivity.py -v

# Run with podman
RUN_CLUSTER_TESTS=true USE_PODMAN=true pytest tests/cluster_dask_integration/ -v

# Run with more verbose output
RUN_CLUSTER_TESTS=true pytest tests/cluster_dask_integration/ -v -s --log-cli-level=INFO
```

### 3. Monitor the Cluster

- **Dask Dashboard**: http://localhost:8787
  - View task execution
  - Monitor worker memory
  - See task distribution

- **Cassandra Metrics**:
  ```bash
  # Node status
  docker exec cassandra-node1 nodetool status

  # Table statistics
  docker exec cassandra-node1 nodetool tablestats cluster_test
  ```

### 4. Cleanup

Stop and remove all containers:
```bash
docker-compose down -v
# or
podman-compose down -v
```

## Test Structure

### `test_cluster_connectivity.py`
Basic Dask cluster connectivity and operations:
- Worker registration
- Task distribution
- Error handling
- Dashboard availability

### `test_task_distribution.py`
Task distribution patterns:
- DataFrame partition distribution
- Shuffle operations
- Task stealing
- Memory distribution
- Fault tolerance

### `test_cassandra_dask_integration.py`
Full integration with Cassandra:
- Multi-node reads
- Token-aware distribution
- All partitioning strategies
- Large-scale processing
- Memory spilling
- Failure recovery

## Configuration

### Environment Variables

- `RUN_CLUSTER_TESTS`: Set to "true" to run tests (default: "false")
- `USE_PODMAN`: Set to "true" to use podman instead of docker (default: "false")

### Docker Compose Configuration

The `docker-compose.yml` defines:
- 3 Cassandra 5 nodes with 256 vnodes each
- 1 Dask scheduler
- 3 Dask workers (2 threads, 2GB memory each)
- Shared network for all services

### Modifying the Configuration

To adjust the cluster size, edit `docker-compose.yml`:

```yaml
# Add more Cassandra nodes
cassandra4:
  image: cassandra:5.0
  # ... configuration

# Add more Dask workers
dask-worker-4:
  image: ghcr.io/dask/dask:latest
  # ... configuration
```

## Troubleshooting

### Containers Won't Start

Check port availability:
```bash
netstat -an | grep -E '(8786|8787|9042|9043|9044)'
```

### Cassandra Cluster Not Forming

Check logs:
```bash
docker logs cassandra-node1
docker logs cassandra-node2
```

Verify seeds configuration and network connectivity.

### Dask Workers Not Registering

Check scheduler logs:
```bash
docker logs dask-scheduler
```

Check worker logs:
```bash
docker logs dask-worker-1
```

### Tests Timing Out

Increase timeouts in tests or ensure services are ready:
```bash
# Wait for Cassandra
while ! docker exec cassandra-node1 cqlsh -e "SELECT now() FROM system.local" > /dev/null 2>&1; do
  echo "Waiting for Cassandra..."
  sleep 5
done

# Check Dask workers
curl http://localhost:8787/api/v1/workers
```

### Memory Issues

If workers run out of memory:
1. Reduce test data size in `conftest.py`
2. Increase worker memory in `docker-compose.yml`
3. Add more workers to distribute load

## Performance Expectations

With the default 3-worker configuration:
- Small datasets (<10k rows): < 5 seconds
- Medium datasets (100k rows): 10-30 seconds
- Large datasets (1M rows): 1-3 minutes

Actual performance depends on:
- Host machine resources
- Network latency (if using remote Docker)
- Cassandra replication factor
- Data complexity

## Development Tips

1. **Test Individual Components**: Run connectivity tests first to ensure cluster is working
2. **Use Dask Dashboard**: Monitor task execution at http://localhost:8787
3. **Check Logs**: Use `docker logs` to debug issues
4. **Start Small**: Test with small datasets before scaling up
5. **Resource Monitoring**: Watch memory usage on workers during large tests

## CI/CD Integration

These tests are not part of the regular CI pipeline due to resource requirements. To run in CI:

1. Ensure CI environment has sufficient resources (8GB+ RAM)
2. Install Docker or Podman in CI environment
3. Add test stage with cluster setup:

```yaml
cluster-integration-tests:
  stage: integration
  script:
    - docker-compose up -d
    - sleep 60  # Wait for services
    - RUN_CLUSTER_TESTS=true pytest tests/cluster_dask_integration/
  after_script:
    - docker-compose down -v
```
