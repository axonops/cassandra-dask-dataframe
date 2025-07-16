"""
Simple test to demonstrate distributed Cassandra + Dask execution.

This test shows how each Dask worker creates its own connection
to Cassandra and executes queries for its assigned token ranges.
"""

import logging
import time

import pandas as pd
import pytest
from cassandra.query import BatchStatement

import cassandra_dask_dataframe as cdf

logger = logging.getLogger(__name__)


class TestSimpleDistributed:
    """Simple tests to demonstrate distributed execution."""

    @pytest.mark.asyncio
    async def test_distributed_execution_demo(self, session, dask_client):
        """
        Demonstrate how distributed execution works.

        This test shows:
        1. How partitions are created based on token ranges
        2. How each worker gets partition metadata
        3. How workers execute queries independently
        """
        # Create a simple table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_demo (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO distributed_demo (pk, value) VALUES (?, ?)
            """
        )

        logger.info("Inserting 1000 rows...")
        batch = BatchStatement()
        for i in range(1000):
            batch.add(insert_stmt, (i, f"value_{i}"))
            if len(batch) >= 25:
                await session.execute(batch)
                batch = BatchStatement()
        if batch:
            await session.execute(batch)

        # Read with distributed Dask
        with dask_client.as_current():
            logger.info("Reading data with distributed Dask...")

            # Read with natural partitioning to see token ranges clearly
            df = await cdf.read_cassandra_table(
                "distributed_demo",
                session=session,
                partitioning_strategy="natural",  # One partition per token range
            )

            logger.info(f"Created {df.npartitions} partitions (one per token range)")

            # Create a function that shows how execution works
            def show_execution_details(partition):
                """
                This function runs on each worker.
                It shows that each worker:
                1. Receives partition metadata
                2. Creates its own Cassandra connection
                3. Executes the query for its token range
                """
                import os
                import socket

                try:
                    from distributed import get_worker

                    worker = get_worker()
                    worker_id = worker.id
                    worker_address = worker.address
                except Exception:
                    # Running locally
                    worker_id = "local"
                    worker_address = "local"

                # Show what's happening
                return pd.DataFrame(
                    {
                        "worker_id": [worker_id],
                        "worker_address": [worker_address],
                        "hostname": [socket.gethostname()],
                        "pid": [os.getpid()],
                        "partition_rows": [len(partition)],
                        "first_pk": [partition["pk"].min() if len(partition) > 0 else None],
                        "last_pk": [partition["pk"].max() if len(partition) > 0 else None],
                    }
                )

            # Apply to each partition
            execution_info = df.map_partitions(show_execution_details)

            # Compute and show results
            logger.info("Computing on distributed workers...")
            start_time = time.time()
            results = execution_info.compute()
            elapsed = time.time() - start_time

            logger.info(f"Computation completed in {elapsed:.2f} seconds")
            logger.info("\nExecution details:")
            logger.info("-" * 80)

            for _, row in results.iterrows():
                if row["partition_rows"] > 0:
                    logger.info(
                        f"Worker: {row['worker_id']}\n"
                        f"  Address: {row['worker_address']}\n"
                        f"  Hostname: {row['hostname']}\n"
                        f"  Process ID: {row['pid']}\n"
                        f"  Rows: {row['partition_rows']}\n"
                        f"  PK range: {row['first_pk']} to {row['last_pk']}\n"
                    )

            # Summary statistics
            unique_workers = results["worker_id"].nunique()
            total_rows = results["partition_rows"].sum()

            logger.info("\nSummary:")
            logger.info(f"  Total workers used: {unique_workers}")
            logger.info(f"  Total rows processed: {total_rows}")
            logger.info(f"  Average rows per worker: {total_rows / unique_workers:.1f}")

            # Verify correctness
            assert unique_workers >= 2, "Should use multiple workers"
            assert total_rows == 1000, f"Expected 1000 rows, got {total_rows}"

    @pytest.mark.asyncio
    async def test_token_range_distribution(self, session, dask_client):
        """
        Show how token ranges map to workers.

        This demonstrates the token-aware routing where each
        worker queries specific token ranges.
        """
        # Use existing table
        table_name = "distributed_demo"

        # Ensure table exists
        try:
            await session.execute(f"SELECT COUNT(*) FROM {table_name}")
        except Exception:
            # Create and populate if needed
            await self.test_distributed_execution_demo(session, dask_client)

        with dask_client.as_current():
            # Read with different strategies to show distribution
            for strategy in ["natural", "compact", "split"]:
                logger.info(f"\n{'='*60}")
                logger.info(f"Testing {strategy.upper()} partitioning strategy")
                logger.info("=" * 60)

                kwargs = {"split_factor": 3} if strategy == "split" else {}

                df = await cdf.read_cassandra_table(
                    table_name, session=session, partitioning_strategy=strategy, **kwargs
                )

                logger.info(f"Created {df.npartitions} partitions")

                # Show token range info for each partition
                def get_token_info(partition):
                    """Get token range information for this partition."""
                    import struct

                    from cassandra.metadata import Murmur3Token
                    from distributed import get_worker

                    if len(partition) == 0:
                        return pd.DataFrame()

                    # Calculate tokens for the data in this partition
                    tokens = []
                    for pk in partition["pk"]:
                        # Calculate Murmur3 token for integer PK
                        pk_bytes = struct.pack(">i", pk)
                        token = Murmur3Token.hash_fn(pk_bytes)
                        tokens.append(token)

                    return pd.DataFrame(
                        {
                            "worker": [get_worker().id],
                            "partition_rows": [len(partition)],
                            "min_token": [min(tokens)],
                            "max_token": [max(tokens)],
                            "token_span": [max(tokens) - min(tokens)],
                        }
                    )

                token_info = df.map_partitions(get_token_info)
                results = token_info.compute()
                results = results[results["partition_rows"] > 0]

                # Show distribution
                logger.info("\nToken range distribution:")
                for _, row in results.iterrows():
                    logger.info(
                        f"  Worker {row['worker']}: "
                        f"{row['partition_rows']} rows, "
                        f"tokens [{row['min_token']:,} to {row['max_token']:,}]"
                    )

                # Summary by worker
                worker_summary = results.groupby("worker")["partition_rows"].agg(["count", "sum"])
                logger.info("\nPer-worker summary:")
                for worker, stats in worker_summary.iterrows():
                    logger.info(
                        f"  {worker}: {stats['count']} partitions, " f"{stats['sum']} total rows"
                    )

    @pytest.mark.asyncio
    async def test_connection_per_worker(self, session, dask_client):
        """
        Demonstrate that each worker creates its own Cassandra connection.

        This is the key to understanding distributed execution:
        - Client sends partition metadata (not data) to workers
        - Each worker creates a Cassandra connection
        - Worker executes query for its token range
        - Results flow back through Dask
        """
        with dask_client.as_current():
            # Read table
            df = await cdf.read_cassandra_table(
                "distributed_demo",
                session=session,
                partitioning_strategy="fixed",
                partition_count=6,  # Ensure multiple partitions
            )

            def show_connection_info(partition):
                """
                This function demonstrates that each worker
                creates its own connection to Cassandra.
                """
                import socket

                from distributed import get_worker

                # In a real scenario, this is where the worker would:
                # 1. Get connection parameters from partition metadata
                # 2. Create cassandra.cluster.Cluster()
                # 3. Execute the query for its token range
                # 4. Return the results

                worker = get_worker()

                # Simulate connection info
                return pd.DataFrame(
                    {
                        "worker_id": [worker.id],
                        "worker_thread": [worker.thread_id],
                        "connection_info": [
                            f"Would connect to Cassandra from {socket.gethostname()}"
                        ],
                        "query_info": [
                            "Would execute: SELECT * FROM table WHERE token(pk) > X AND token(pk) <= Y"
                        ],
                        "partition_size": [len(partition)],
                    }
                )

            connection_info = df.map_partitions(show_connection_info)
            results = connection_info.compute()

            logger.info("\nConnection simulation:")
            logger.info("-" * 80)
            for _, row in results.iterrows():
                if row["partition_size"] > 0:
                    logger.info(
                        f"Worker {row['worker_id']}:\n"
                        f"  {row['connection_info']}\n"
                        f"  {row['query_info']}\n"
                        f"  Processing {row['partition_size']} rows\n"
                    )
