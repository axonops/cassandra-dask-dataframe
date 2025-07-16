"""
Configuration for Dask cluster integration tests using existing services.

This version connects to already running Dask and Cassandra services
instead of starting new ones.
"""

import logging
import os
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
def dask_client(dask_scheduler_address):
    """Create Dask client connected to existing scheduler."""
    logger.info(f"Connecting to Dask scheduler at {dask_scheduler_address}")
    
    client = Client(dask_scheduler_address)
    logger.info(f"Connected to Dask cluster")
    
    yield client
    client.close()


@pytest.fixture(scope="session")
def cassandra_cluster(cassandra_contact_points, cassandra_port):
    """Create Cassandra cluster connection to existing instance."""
    logger.info(f"Connecting to Cassandra at {cassandra_contact_points}:{cassandra_port}")
    
    cluster = Cluster(
        contact_points=cassandra_contact_points,
        port=cassandra_port,
        protocol_version=5  # Cassandra 5 supports protocol v5
    )
    
    yield cluster
    cluster.shutdown()


@pytest_asyncio.fixture
async def session(cassandra_cluster) -> AsyncGenerator[AsyncCassandraSession, None]:
    """Create async Cassandra session."""
    sync_session = cassandra_cluster.connect()
    
    # Create test keyspace
    sync_session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS cluster_test
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
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
    return 10_000  # Reduced for demo


@pytest.fixture
def partition_count():
    """Expected number of partitions for tests."""
    return 20  # Reasonable for 3 workers with 2 threads each