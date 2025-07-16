# Makefile for Dask cluster integration tests

.PHONY: help up down test test-connectivity test-distribution test-integration clean logs status

# Default target
help:
	@echo "Dask Cluster Integration Tests"
	@echo "============================="
	@echo ""
	@echo "Available targets:"
	@echo "  up                 - Start the Docker/Podman cluster"
	@echo "  down               - Stop the cluster"
	@echo "  test               - Run all cluster tests"
	@echo "  test-connectivity  - Test Dask cluster connectivity only"
	@echo "  test-distribution  - Test task distribution only"
	@echo "  test-integration   - Test Cassandra integration only"
	@echo "  clean              - Stop cluster and remove volumes"
	@echo "  logs               - Show logs from all containers"
	@echo "  status             - Check cluster status"
	@echo ""
	@echo "Environment variables:"
	@echo "  USE_PODMAN=true    - Use podman instead of docker"
	@echo ""
	@echo "Examples:"
	@echo "  make up"
	@echo "  make test"
	@echo "  USE_PODMAN=true make up"

# Detect container runtime
ifdef USE_PODMAN
	COMPOSE_CMD = podman-compose
	EXEC_CMD = podman exec
else
	COMPOSE_CMD = docker-compose
	EXEC_CMD = docker exec
endif

# Start the cluster
up:
	@echo "Starting cluster with $(COMPOSE_CMD)..."
	$(COMPOSE_CMD) up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@echo "Checking Cassandra cluster status..."
	@$(EXEC_CMD) cassandra-node1 nodetool status || true
	@echo ""
	@echo "Cluster should be ready. Dask dashboard: http://localhost:8787"

# Stop the cluster
down:
	@echo "Stopping cluster..."
	$(COMPOSE_CMD) down

# Run all tests
test:
	@echo "Running all cluster integration tests..."
	RUN_CLUSTER_TESTS=true pytest . -v -s --log-cli-level=INFO

# Test Dask connectivity only
test-connectivity:
	@echo "Testing Dask cluster connectivity..."
	RUN_CLUSTER_TESTS=true pytest test_cluster_connectivity.py -v -s

# Test task distribution
test-distribution:
	@echo "Testing task distribution..."
	RUN_CLUSTER_TESTS=true pytest test_task_distribution.py -v -s

# Test Cassandra integration
test-integration:
	@echo "Testing Cassandra + Dask integration..."
	RUN_CLUSTER_TESTS=true pytest test_cassandra_dask_integration.py -v -s

# Clean everything
clean:
	@echo "Stopping cluster and removing volumes..."
	$(COMPOSE_CMD) down -v
	@echo "Cleanup complete"

# Show logs
logs:
	$(COMPOSE_CMD) logs -f

# Check status
status:
	@echo "=== Container Status ==="
	@$(COMPOSE_CMD) ps
	@echo ""
	@echo "=== Cassandra Cluster Status ==="
	@$(EXEC_CMD) cassandra-node1 nodetool status || echo "Cassandra not ready"
	@echo ""
	@echo "=== Dask Workers ==="
	@curl -s http://localhost:8787/api/v1/workers | python -m json.tool | grep -E '"id"|"memory_limit"|"nthreads"' || echo "Dask API not ready"

# Quick test - just connectivity
quick-test: up test-connectivity

# Full test suite
full-test: up test clean

# Development mode - start cluster and tail logs
dev:
	$(COMPOSE_CMD) up

# Wait for cluster to be ready
wait-ready:
	@echo "Waiting for Cassandra to be ready..."
	@while ! $(EXEC_CMD) cassandra-node1 cqlsh -e "SELECT now() FROM system.local" > /dev/null 2>&1; do \
		echo "Waiting for Cassandra..."; \
		sleep 5; \
	done
	@echo "Cassandra is ready!"
	@echo "Checking Dask cluster..."
	@while ! curl -s http://localhost:8787/api/v1/health > /dev/null 2>&1; do \
		echo "Waiting for Dask..."; \
		sleep 2; \
	done
	@echo "Dask is ready!"

# Run tests with proper setup
test-with-setup: up wait-ready test
