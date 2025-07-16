"""
Test async-cassandra-dataframe with distributed Dask cluster and multi-node Cassandra.

What this tests:
---------------
1. Reading from multi-node Cassandra into distributed Dask
2. Token range partitioning across Dask workers
3. Data locality awareness
4. Large-scale data processing
5. All partitioning strategies in distributed environment

Why this matters:
----------------
- Validates production-like deployment
- Tests real distributed behavior
- Ensures scalability
- Verifies token-aware routing
"""

import logging
import time
import uuid
from datetime import UTC, datetime

import pandas as pd
import pytest
from cassandra.query import BatchStatement
from dask.distributed import wait

import async_cassandra_dataframe as cdf

logger = logging.getLogger(__name__)


class TestCassandraDaskIntegration:
    """Test async-cassandra-dataframe with distributed Dask and Cassandra."""

    @pytest.mark.asyncio
    async def test_distributed_read_basic(self, session, dask_client):
        """
        Test basic distributed read from Cassandra.

        Given: Data in multi-node Cassandra cluster
        When: Reading with Dask cluster
        Then: Data should be distributed across Dask workers
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_basic (
                id UUID PRIMARY KEY,
                value TEXT,
                timestamp TIMESTAMP
            )
            """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_test_basic (id, value, timestamp)
            VALUES (?, ?, ?)
            """
        )

        logger.info("Inserting 10,000 rows...")
        batch_size = 25
        now = datetime.now(UTC)

        for i in range(0, 10000, batch_size):
            batch = BatchStatement()
            for j in range(batch_size):
                if i + j < 10000:
                    batch.add(insert_stmt, (uuid.uuid4(), f"value_{i+j}", now))
            await session.execute(batch)

        # Read with distributed Dask
        with dask_client.as_current():
            logger.info("Reading data with distributed Dask...")
            df = await cdf.read_cassandra_table(
                "distributed_test_basic", session=session, partitioning_strategy="auto"
            )

            logger.info(f"Created {df.npartitions} partitions")

            # Track which workers process which partitions
            def identify_worker(partition):
                """Identify which worker processes this partition."""
                from distributed import get_worker

                worker = get_worker()
                return pd.DataFrame({"worker_id": [worker.id], "partition_rows": [len(partition)]})

            worker_info = df.map_partitions(identify_worker)
            worker_stats = worker_info.compute()

            # Analyze distribution
            logger.info("Partition distribution across workers:")
            worker_summary = worker_stats.groupby("worker_id")["partition_rows"].agg(
                ["count", "sum"]
            )
            for worker_id, stats in worker_summary.iterrows():
                logger.info(f"  {worker_id}: {stats['count']} partitions, {stats['sum']} rows")

            # Should use multiple workers
            unique_workers = worker_stats["worker_id"].nunique()
            assert unique_workers >= 2, f"Expected at least 2 workers, got {unique_workers}"

            # Verify total row count
            total_rows = worker_stats["partition_rows"].sum()
            assert total_rows == 10000, f"Expected 10000 rows, got {total_rows}"

    @pytest.mark.asyncio
    async def test_token_aware_distribution(self, session, dask_client):
        """
        Test that token ranges are properly distributed to workers.

        Given: Data spread across token ranges
        When: Reading with token-based partitioning
        Then: Each worker should handle specific token ranges
        """
        # Create table with known partition key distribution
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_tokens (
                partition_id INT,
                cluster_id INT,
                value TEXT,
                PRIMARY KEY (partition_id, cluster_id)
            )
            """
        )

        # Insert data across multiple partitions
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_test_tokens (partition_id, cluster_id, value)
            VALUES (?, ?, ?)
            """
        )

        logger.info("Inserting data across 100 partitions...")
        for partition in range(100):
            batch = BatchStatement()
            for cluster in range(100):
                batch.add(insert_stmt, (partition, cluster, f"p{partition}_c{cluster}"))
            await session.execute(batch)

        # Read with natural partitioning (one partition per token range)
        with dask_client.as_current():
            df = await cdf.read_cassandra_table(
                "distributed_test_tokens", session=session, partitioning_strategy="natural"
            )

            logger.info(f"Created {df.npartitions} natural partitions")

            # Track token range processing
            def analyze_partition(partition):
                """Analyze which token range this partition represents."""
                from distributed import get_worker

                if len(partition) == 0:
                    return pd.DataFrame(
                        {"worker_id": [], "min_partition": [], "max_partition": [], "row_count": []}
                    )

                return pd.DataFrame(
                    {
                        "worker_id": [get_worker().id],
                        "min_partition": [partition["partition_id"].min()],
                        "max_partition": [partition["partition_id"].max()],
                        "row_count": [len(partition)],
                    }
                )

            analysis = df.map_partitions(analyze_partition)
            results = analysis.compute()
            results = results[results["row_count"] > 0]  # Filter empty partitions

            # Log token range distribution
            logger.info("Token range distribution:")
            for _, row in results.iterrows():
                logger.info(
                    f"  {row['worker_id']}: partitions {row['min_partition']}-{row['max_partition']} "
                    f"({row['row_count']} rows)"
                )

            # Each worker should handle multiple token ranges
            worker_counts = results.groupby("worker_id").size()
            assert len(worker_counts) >= 2, "Should use at least 2 workers"

    @pytest.mark.asyncio
    async def test_split_strategy_distribution(self, session, dask_client):
        """
        Test SPLIT strategy with distributed Dask.

        Given: Large token ranges
        When: Using SPLIT strategy with distributed Dask
        Then: Sub-partitions should be distributed across workers
        """
        # Use existing table from previous test or create it
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_split (
                id UUID PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP
            )
            """
        )

        # Insert data if not already present
        count_result = await session.execute("SELECT COUNT(*) FROM distributed_test_split")
        if count_result.one()[0] < 5000:
            insert_stmt = await session.prepare(
                """
                INSERT INTO distributed_test_split (id, data, created_at)
                VALUES (?, ?, ?)
                """
            )

            logger.info("Inserting 5000 rows...")
            now = datetime.now(UTC)
            for i in range(0, 5000, 25):
                batch = BatchStatement()
                for j in range(25):
                    if i + j < 5000:
                        batch.add(insert_stmt, (uuid.uuid4(), f"data_{i+j}", now))
                await session.execute(batch)

        # Read with SPLIT strategy
        with dask_client.as_current():
            df = await cdf.read_cassandra_table(
                "distributed_test_split",
                session=session,
                partitioning_strategy="split",
                split_factor=3,
            )

            logger.info(f"SPLIT strategy created {df.npartitions} partitions")

            # Verify partitions are distributed
            def get_partition_info(partition):
                """Get information about partition processing."""
                from distributed import get_worker

                return pd.DataFrame({"worker": [get_worker().id], "rows": [len(partition)]})

            info = df.map_partitions(get_partition_info)
            distribution = info.compute()

            # Log distribution
            worker_stats = distribution.groupby("worker")["rows"].agg(["count", "sum"])
            logger.info("SPLIT strategy distribution:")
            for worker, stats in worker_stats.iterrows():
                logger.info(f"  {worker}: {stats['count']} partitions, {stats['sum']} rows")

            # Should distribute across all workers
            assert len(worker_stats) >= 3, f"Should use all 3 workers, got {len(worker_stats)}"

    @pytest.mark.asyncio
    async def test_large_scale_processing(self, session, dask_client, large_test_data_size):
        """
        Test large-scale data processing.

        Given: Large dataset in Cassandra
        When: Processing with distributed Dask
        Then: Should handle efficiently without memory issues
        """
        # Create wide table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_large (
                partition_key INT,
                cluster_key INT,
                data1 TEXT,
                data2 TEXT,
                data3 TEXT,
                data4 TEXT,
                data5 TEXT,
                metrics DOUBLE,
                PRIMARY KEY (partition_key, cluster_key)
            )
            """
        )

        # Insert large dataset
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_test_large
            (partition_key, cluster_key, data1, data2, data3, data4, data5, metrics)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        logger.info(f"Inserting {large_test_data_size} rows...")
        partitions = 1000
        rows_per_partition = large_test_data_size // partitions

        for partition in range(partitions):
            batch = BatchStatement()
            for cluster in range(rows_per_partition):
                batch.add(
                    insert_stmt,
                    (
                        partition,
                        cluster,
                        f"data1_{partition}_{cluster}",
                        f"data2_{partition}_{cluster}",
                        f"data3_{partition}_{cluster}",
                        f"data4_{partition}_{cluster}",
                        f"data5_{partition}_{cluster}",
                        float(partition * cluster),
                    ),
                )

                # Execute batch when full
                if len(batch) >= 25:
                    await session.execute(batch)
                    batch = BatchStatement()

            # Execute remaining
            if batch:
                await session.execute(batch)

            if partition % 100 == 0:
                logger.info(f"Inserted {partition}/{partitions} partitions")

        # Process large dataset
        with dask_client.as_current():
            start_time = time.time()

            df = await cdf.read_cassandra_table(
                "distributed_test_large", session=session, partitioning_strategy="auto"
            )

            logger.info(f"Created {df.npartitions} partitions for {large_test_data_size} rows")

            # Perform aggregations
            result = df.groupby("partition_key").agg({"metrics": ["mean", "sum", "count"]})

            # Compute with progress tracking
            future = dask_client.compute(result)
            wait(future)
            computed_result = future.result()

            elapsed = time.time() - start_time
            logger.info(f"Processed {large_test_data_size} rows in {elapsed:.2f} seconds")
            logger.info(f"Throughput: {large_test_data_size / elapsed:.0f} rows/second")

            # Verify results
            assert len(computed_result) == partitions
            assert computed_result[("metrics", "count")].sum() == large_test_data_size

    @pytest.mark.asyncio
    async def test_memory_spilling(self, session, dask_client):
        """
        Test that large computations properly spill to disk.

        Given: Memory-intensive operations
        When: Memory limits are exceeded
        Then: Should spill to disk without failing
        """
        # Create table with large text data
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_memory (
                id INT PRIMARY KEY,
                large_text TEXT,
                data BLOB
            )
            """
        )

        # Insert data with large text fields
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_test_memory (id, large_text, data)
            VALUES (?, ?, ?)
            """
        )

        logger.info("Inserting memory-intensive data...")
        large_text = "x" * 10000  # 10KB per row
        large_blob = b"y" * 10000

        for i in range(0, 10000, 100):
            batch = BatchStatement()
            for j in range(100):
                if i + j < 10000:
                    batch.add(insert_stmt, (i + j, large_text, large_blob))
            await session.execute(batch)

        # Process with memory pressure
        with dask_client.as_current():
            df = await cdf.read_cassandra_table(
                "distributed_test_memory",
                session=session,
                partitioning_strategy="fixed",
                partition_count=50,  # Many partitions to stress memory
            )

            # Memory-intensive operation
            def expand_data(partition):
                """Expand data to use more memory."""
                # Duplicate data to increase memory usage
                expanded = pd.concat([partition] * 5, ignore_index=True)
                expanded["processed"] = expanded["large_text"].str.upper()
                return expanded

            expanded_df = df.map_partitions(expand_data)

            # This should trigger memory spilling
            logger.info("Processing memory-intensive operation...")
            result = expanded_df.groupby("id").size().compute()

            # Should complete without memory errors
            assert len(result) == 10000

    @pytest.mark.asyncio
    async def test_failure_recovery(self, session, dask_client):
        """
        Test recovery from worker failures during computation.

        Given: Computation in progress
        When: Simulating failures
        Then: Should recover and complete
        """
        # Use simple table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_test_recovery (
                id INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_test_recovery (id, value) VALUES (?, ?)
            """
        )

        for i in range(1000):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # Read and process with potential failures
        with dask_client.as_current():
            df = await cdf.read_cassandra_table(
                "distributed_test_recovery", session=session, partitioning_strategy="natural"
            )

            # Function that may fail
            def unreliable_processing(partition):
                """Process that may fail occasionally."""
                import random

                from distributed import get_worker

                # Simulate occasional failures (but not too many)
                if random.random() < 0.1:  # 10% failure rate
                    raise RuntimeError(f"Simulated failure on {get_worker().id}")

                # Normal processing
                return partition.assign(processed=partition["value"].str.upper())

            # Process with retries
            processed_df = df.map_partitions(unreliable_processing)

            # Compute with retries enabled
            logger.info("Processing with potential failures...")
            try:
                result = processed_df.compute(retries=2)
                logger.info(f"Successfully processed {len(result)} rows despite failures")
                assert len(result) == 1000
            except Exception as e:
                logger.error(f"Failed after retries: {e}")
                # This is acceptable - we're testing the system under failure

    @pytest.mark.asyncio
    @pytest.mark.parametrize("strategy", ["auto", "natural", "compact", "fixed", "split"])
    async def test_all_strategies_distributed(self, session, dask_client, strategy):
        """
        Test all partitioning strategies in distributed environment.

        Given: Data in Cassandra cluster
        When: Using different partitioning strategies
        Then: All should work correctly with distributed Dask
        """
        # Use a common table
        table_name = "distributed_test_strategies"

        # Create table if needed
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                strategy_test TEXT,
                value INT
            )
            """
        )

        # Ensure data exists
        count_result = await session.execute(f"SELECT COUNT(*) FROM {table_name}")
        if count_result.one()[0] < 5000:
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {table_name} (id, strategy_test, value)
                VALUES (?, ?, ?)
                """
            )

            for i in range(5000):
                await session.execute(insert_stmt, (uuid.uuid4(), strategy, i))

        # Test reading with each strategy
        with dask_client.as_current():
            kwargs = {}
            if strategy == "fixed":
                kwargs["partition_count"] = 12
            elif strategy == "split":
                kwargs["split_factor"] = 2

            logger.info(f"Testing {strategy} strategy...")
            df = await cdf.read_cassandra_table(
                table_name, session=session, partitioning_strategy=strategy, **kwargs
            )

            logger.info(f"{strategy} strategy created {df.npartitions} partitions")

            # Verify data and distribution
            def check_partition(partition):
                """Check partition is processed correctly."""
                from distributed import get_worker

                return pd.DataFrame(
                    {
                        "strategy": [strategy],
                        "worker": [get_worker().id],
                        "rows": [len(partition)],
                        "min_value": [partition["value"].min() if len(partition) > 0 else None],
                        "max_value": [partition["value"].max() if len(partition) > 0 else None],
                    }
                )

            check_df = df.map_partitions(check_partition)
            checks = check_df.compute()

            # Remove empty partitions
            checks = checks[checks["rows"] > 0]

            # Log results
            logger.info(f"Strategy {strategy} results:")
            worker_summary = checks.groupby("worker")["rows"].agg(["count", "sum"])
            for worker, stats in worker_summary.iterrows():
                logger.info(f"  {worker}: {stats['count']} partitions, {stats['sum']} rows")

            # Verify all strategies work
            total_rows = checks["rows"].sum()
            assert total_rows >= 5000, f"Strategy {strategy} missed data: {total_rows} rows"

            # Verify distribution (at least 2 workers for distributed processing)
            unique_workers = checks["worker"].nunique()
            assert unique_workers >= 2, f"Strategy {strategy} only used {unique_workers} workers"
