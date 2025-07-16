"""
Configuration for Dask cluster integration tests.

What this provides:
------------------
1. Fixtures for connecting to distributed Dask cluster
2. Fixtures for connecting to Cassandra cluster
3. Health checks for both systems
4. Test data setup and teardown

Why this matters:
----------------
- Tests real distributed behavior
- Validates task distribution across workers
- Ensures proper integration with multi-node Cassandra
- Tests production-like scenarios
"""

import logging
import os

# We'll use subprocess instead of testcontainers for better podman support
import subprocess
import time
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from async_cassandra import AsyncCassandraSession
from cassandra.cluster import Cluster
from dask.distributed import Client

logger = logging.getLogger(__name__)


# Skip these tests by default - they require Docker/Podman
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_CLUSTER_TESTS", "false").lower() != "true",
    reason="Cluster integration tests require RUN_CLUSTER_TESTS=true",
)


@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to docker-compose file."""
    return os.path.join(os.path.dirname(__file__), "docker-compose.yml")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    """Project name for docker-compose."""
    return "dask-cassandra-integration"


@pytest.fixture(scope="session")
def dask_scheduler_address():
    """Dask scheduler address."""
    return "tcp://localhost:8786"


@pytest.fixture(scope="session")
def cassandra_contact_points():
    """Cassandra contact points."""
    return ["localhost"]


@pytest.fixture(scope="session")
def cassandra_port():
    """Cassandra port."""
    return 9042


@pytest.fixture(scope="session")
def compose_environment(docker_compose_file, docker_compose_project_name):
    """Start docker-compose environment."""
    # Check if we should use podman
    use_podman = os.getenv("USE_PODMAN", "false").lower() == "true"
    compose_cmd = "podman-compose" if use_podman else "docker-compose"

    logger.info(f"Starting docker-compose environment with {compose_cmd}...")

    # Change to the directory containing docker-compose.yml
    original_dir = os.getcwd()
    compose_dir = os.path.dirname(docker_compose_file)
    os.chdir(compose_dir)

    try:
        # Start services
        subprocess.run([compose_cmd, "-p", docker_compose_project_name, "up", "-d"], check=True)

        # Wait for services to be ready
        logger.info("Waiting for services to be ready...")
        time.sleep(30)  # Initial wait for containers to start

        yield True

    finally:
        logger.info("Stopping docker-compose environment...")
        subprocess.run([compose_cmd, "-p", docker_compose_project_name, "down"], check=True)
        os.chdir(original_dir)


@pytest.fixture(scope="session")
def dask_client(compose_environment, dask_scheduler_address):
    """Create Dask client connected to scheduler."""
    logger.info(f"Connecting to Dask scheduler at {dask_scheduler_address}")

    # Wait for scheduler to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            client = Client(dask_scheduler_address)
            logger.info(f"Connected to Dask cluster with {len(client.workers())} workers")

            # Wait for workers to register
            for _ in range(10):
                if len(client.workers()) >= 3:
                    break
                time.sleep(2)

            logger.info(f"Dask cluster ready with {len(client.workers())} workers")
            yield client
            client.close()
            return

        except Exception as e:
            if i < max_retries - 1:
                logger.warning(
                    f"Failed to connect to Dask scheduler (attempt {i+1}/{max_retries}): {e}"
                )
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Failed to connect to Dask scheduler after {max_retries} attempts"
                ) from e


@pytest.fixture(scope="session")
def cassandra_cluster(compose_environment, cassandra_contact_points, cassandra_port):
    """Create Cassandra cluster connection."""
    logger.info(f"Connecting to Cassandra at {cassandra_contact_points}:{cassandra_port}")

    # Wait for Cassandra to be ready
    max_retries = 60
    for i in range(max_retries):
        try:
            cluster = Cluster(
                contact_points=cassandra_contact_points,
                port=cassandra_port,
                protocol_version=5,  # Cassandra 5 supports protocol v5
            )
            session = cluster.connect()

            # Verify cluster is ready
            result = session.execute("SELECT cluster_name, release_version FROM system.local")
            row = result.one()
            logger.info(
                f"Connected to Cassandra cluster '{row.cluster_name}' version {row.release_version}"
            )

            # Check all nodes are up
            nodes_result = session.execute("SELECT peer, data_center, rack FROM system.peers")
            nodes = list(nodes_result)
            logger.info(f"Cassandra cluster has {len(nodes) + 1} nodes")

            session.shutdown()
            yield cluster
            cluster.shutdown()
            return

        except Exception as e:
            if i < max_retries - 1:
                logger.warning(f"Failed to connect to Cassandra (attempt {i+1}/{max_retries}): {e}")
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Failed to connect to Cassandra after {max_retries} attempts"
                ) from e


@pytest_asyncio.fixture
async def session(cassandra_cluster) -> AsyncGenerator[AsyncCassandraSession, None]:
    """Create async Cassandra session."""
    sync_session = cassandra_cluster.connect()

    # Create test keyspace with RF=3 for multi-node testing
    sync_session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS cluster_test
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 3
        }
        """
    )
    sync_session.set_keyspace("cluster_test")

    # Wrap in async session
    async_session = AsyncCassandraSession(sync_session)

    yield async_session

    # Cleanup
    await async_session.execute("DROP KEYSPACE IF EXISTS cluster_test")
    await async_session.close()


@pytest.fixture
def large_test_data_size():
    """Number of rows for large data tests."""
    return 100_000  # 100k rows


@pytest.fixture
def partition_count():
    """Expected number of partitions for tests."""
    return 20  # Reasonable for 3 workers with 2 threads each
