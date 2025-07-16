"""
Example of using cassandra-dask-dataframe with a Dask cluster.

This example demonstrates:
1. Creating a connection config for distributed execution
2. Reading data across multiple Dask workers
3. Each worker creating its own Cassandra connection

Prerequisites:
- Cassandra running on localhost:9042
- Dask cluster running (scheduler + workers)

To start a local Dask cluster:
  dask scheduler &
  dask worker localhost:8786 --nworkers 2 --nthreads 2 &

Then run this example:
  python examples/distributed_usage.py
"""

import asyncio
import logging

from cassandra.cluster import Cluster
from dask.distributed import Client

from cassandra_dask_dataframe import CassandraDataFrameReader
from cassandra_dask_dataframe.connection_config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def setup_test_data(cluster):
    """Create test keyspace and table with sample data."""
    session = cluster.connect()

    # Create keyspace
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test_distributed
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    session.set_keyspace("test_distributed")

    # Create table
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_data (
            sensor_id int,
            timestamp timestamp,
            temperature double,
            humidity double,
            location text,
            PRIMARY KEY (sensor_id, timestamp)
        )
    """
    )

    # Insert sample data
    from datetime import datetime, timedelta

    base_time = datetime.now()

    insert_stmt = session.prepare(
        """
        INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity, location)
        VALUES (?, ?, ?, ?, ?)
    """
    )

    # Generate data for 10 sensors
    for sensor_id in range(10):
        location = f"Building-{sensor_id // 5}"
        for hour in range(24):
            timestamp = base_time - timedelta(hours=hour)
            temperature = 20 + (sensor_id % 3) + (hour % 12) * 0.5
            humidity = 40 + (sensor_id % 5) * 5 + (hour % 6) * 2

            session.execute(insert_stmt, (sensor_id, timestamp, temperature, humidity, location))

    logger.info("Test data created successfully")
    session.shutdown()


async def distributed_example():
    """Demonstrate distributed reading with Dask cluster."""

    # Connect to Dask cluster
    client = Client("localhost:8786")
    logger.info(f"Connected to Dask cluster with {len(client.workers())} workers")

    # Create Cassandra cluster for initial connection
    cluster = Cluster(["localhost"])

    # Create connection config that workers will use
    connection_config = ConnectionConfig(
        contact_points=["localhost"],
        port=9042,
        protocol_version=5,
    )

    # Setup test data
    await setup_test_data(cluster)

    # Wrap in async session for the reader
    from async_cassandra import AsyncCassandraSession

    session = cluster.connect("test_distributed")
    async_session = AsyncCassandraSession(session)

    # Create reader with connection config
    reader = CassandraDataFrameReader(
        session=async_session,
        table="sensor_data",
        keyspace="test_distributed",
        connection_config=connection_config,  # Pass config for distributed execution
    )

    # Read data - this will be distributed across workers
    logger.info("Reading sensor data across Dask cluster...")
    df = await reader.read(
        partition_count=8,  # Create 8 partitions for parallel reading
    )

    logger.info(f"Created Dask DataFrame with {df.npartitions} partitions")

    # Perform distributed computations
    # 1. Calculate average temperature per location
    avg_temp_by_location = df.groupby("location")["temperature"].mean()
    result1 = avg_temp_by_location.compute()
    logger.info(f"Average temperature by location:\n{result1}")

    # 2. Find sensors with high humidity
    high_humidity = df[df["humidity"] > 60]
    high_humidity_count = high_humidity["sensor_id"].nunique().compute()
    logger.info(f"Sensors with humidity > 60: {high_humidity_count}")

    # 3. Demonstrate that work was distributed
    # Add a column showing which worker processed each partition
    def add_worker_info(partition):
        """Add worker hostname to partition."""
        import socket

        partition["worker"] = socket.gethostname()
        return partition

    df_with_worker = df.map_partitions(add_worker_info)
    worker_distribution = df_with_worker.groupby("worker").size().compute()
    logger.info(f"Rows processed per worker:\n{worker_distribution}")

    # Cleanup
    cluster.shutdown()
    client.close()

    logger.info("Distributed example completed successfully!")


async def compare_local_vs_distributed():
    """Compare local vs distributed execution."""

    # Setup
    cluster = Cluster(["localhost"])
    await setup_test_data(cluster)

    from async_cassandra import AsyncCassandraSession

    session = cluster.connect("test_distributed")
    async_session = AsyncCassandraSession(session)

    connection_config = ConnectionConfig.from_cluster(cluster)

    # Test 1: Local execution (no Dask cluster)
    logger.info("\n=== LOCAL EXECUTION ===")
    reader_local = CassandraDataFrameReader(
        session=async_session,
        table="sensor_data",
        connection_config=connection_config,
    )

    import time

    start = time.time()
    df_local = await reader_local.read(partition_count=4)
    count_local = df_local["sensor_id"].count().compute()
    local_time = time.time() - start
    logger.info(f"Local execution: {count_local} rows in {local_time:.2f}s")

    # Test 2: Distributed execution (with Dask cluster)
    try:
        client = Client("localhost:8786")
        logger.info("\n=== DISTRIBUTED EXECUTION ===")
        logger.info(f"Workers: {len(client.workers())}")

        reader_dist = CassandraDataFrameReader(
            session=async_session,
            table="sensor_data",
            connection_config=connection_config,
        )

        start = time.time()
        df_dist = await reader_dist.read(partition_count=8)
        count_dist = df_dist["sensor_id"].count().compute()
        dist_time = time.time() - start
        logger.info(f"Distributed execution: {count_dist} rows in {dist_time:.2f}s")

        speedup = local_time / dist_time
        logger.info(f"Speedup: {speedup:.2f}x")

        client.close()
    except Exception as e:
        logger.warning(f"Distributed execution failed (is Dask cluster running?): {e}")

    cluster.shutdown()


if __name__ == "__main__":
    # Run the examples
    asyncio.run(distributed_example())

    # Uncomment to compare local vs distributed
    # asyncio.run(compare_local_vs_distributed())
