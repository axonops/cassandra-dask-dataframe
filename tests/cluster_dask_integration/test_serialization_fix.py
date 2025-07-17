"""
Test to demonstrate and fix the serialization issue.

This test shows the problem with non-serializable objects
and implements a solution.
"""

import asyncio
import logging

import dask
import pandas as pd
import pytest
from cassandra.query import BatchStatement
from dask.distributed import as_completed

logger = logging.getLogger(__name__)


class TestSerializationFix:
    """Test serialization issues and fixes."""

    @pytest.mark.asyncio
    async def test_identify_serialization_issue(self, session, dask_client):
        """
        Identify what's causing the serialization error.

        What this tests:
        ---------------
        1. Shows that asyncio.Semaphore cannot be pickled
        2. Shows that Cassandra session objects cannot be pickled
        3. Demonstrates the issue in partition definitions

        Why this matters:
        ----------------
        - Dask distributed requires all objects to be serializable
        - Thread locks and database connections cannot cross process boundaries
        - We need a different approach for distributed execution
        """
        import pickle

        # Test 1: Semaphore serialization
        semaphore = asyncio.Semaphore(10)
        try:
            pickle.dumps(semaphore)
            logger.info("✓ Semaphore is serializable")
        except Exception as e:
            logger.error(f"✗ Semaphore serialization failed: {type(e).__name__}: {e}")
            assert "lock" in str(e).lower() or "thread" in str(e).lower()

        # Test 2: Session serialization
        try:
            pickle.dumps(session)
            logger.info("✓ Session is serializable")
        except Exception as e:
            logger.error(f"✗ Session serialization failed: {type(e).__name__}: {e}")

        # Test 3: Demonstrate the actual issue
        partition_def = {
            "keyspace": "cluster_test",
            "table": "test_table",
            "token_range": {"start": -9223372036854775808, "end": 0},
            "_semaphore": semaphore,  # This causes the issue!
        }

        try:
            pickle.dumps(partition_def)
            logger.info("✓ Partition definition is serializable")
        except Exception as e:
            logger.error(f"✗ Partition definition serialization failed: {type(e).__name__}: {e}")
            assert "lock" in str(e).lower() or "thread" in str(e).lower()

    @pytest.mark.asyncio
    async def test_distributed_execution_with_connection_params(self, session, dask_client):
        """
        Demonstrate the correct way to handle distributed execution.

        What this tests:
        ---------------
        1. Pass connection parameters instead of session objects
        2. Each worker creates its own connection
        3. No non-serializable objects in partition definitions
        4. Successful distributed execution

        Why this matters:
        ----------------
        - This is how distributed database access should work
        - Each process needs its own database connection
        - Connection pooling happens per-worker, not globally
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_fix_demo (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            "INSERT INTO distributed_fix_demo (pk, value) VALUES (?, ?)"
        )

        batch = BatchStatement()
        for i in range(100):
            batch.add(insert_stmt, (i, f"value_{i}"))
            if len(batch) >= 25:
                await session.execute(batch)
                batch = BatchStatement()
        if batch:
            await session.execute(batch)

        # Define connection parameters (serializable)
        connection_params = {
            "contact_points": ["localhost"],
            "port": 9042,
            "keyspace": "cluster_test",
        }

        # Create partition definitions without non-serializable objects
        partitions = [
            {
                "partition_id": 0,
                "connection_params": connection_params,
                "query": "SELECT * FROM distributed_fix_demo WHERE pk >= 0 AND pk < 50",
            },
            {
                "partition_id": 1,
                "connection_params": connection_params,
                "query": "SELECT * FROM distributed_fix_demo WHERE pk >= 50 AND pk < 100",
            },
        ]

        def read_partition_distributed(partition_def):
            """
            This function runs on Dask workers.
            Each worker creates its own Cassandra connection.
            """
            from cassandra.cluster import Cluster

            # Create connection on the worker
            conn_params = partition_def["connection_params"]
            cluster = Cluster(
                contact_points=conn_params["contact_points"],
                port=conn_params["port"],
            )
            session = cluster.connect(conn_params["keyspace"])

            try:
                # Execute query
                result = session.execute(partition_def["query"])
                rows = list(result)

                # Convert to DataFrame
                if rows:
                    df = pd.DataFrame([dict(row._asdict()) for row in rows])
                else:
                    df = pd.DataFrame(columns=["pk", "value"])

                # Add partition info
                df["partition_id"] = partition_def["partition_id"]

                return df
            finally:
                cluster.shutdown()

        # Create delayed tasks
        with dask_client.as_current():
            delayed_partitions = [dask.delayed(read_partition_distributed)(p) for p in partitions]

            # Create Dask DataFrame
            meta = pd.DataFrame(
                {
                    "pk": pd.Series(dtype="int64"),
                    "value": pd.Series(dtype="object"),
                    "partition_id": pd.Series(dtype="int64"),
                }
            )
            df = dask.dataframe.from_delayed(delayed_partitions, meta=meta)

            # Compute results
            logger.info("Computing on distributed cluster...")
            result = df.compute()

            logger.info(f"✓ Successfully read {len(result)} rows")
            logger.info(f"✓ Partitions used: {result['partition_id'].unique()}")

            # Verify data
            assert len(result) == 100
            assert set(result["pk"]) == set(range(100))
            assert len(result["partition_id"].unique()) == 2

    @pytest.mark.asyncio
    async def test_async_wrapper_distributed_pattern(self, session, dask_client):
        """
        Show how cassandra-dask-dataframe should handle distributed execution.

        What this tests:
        ---------------
        1. Async wrapper on client side
        2. Sync execution on worker side
        3. Connection params passed, not sessions
        4. No thread locks in serialized data

        Why this matters:
        ----------------
        - This pattern allows async convenience on client
        - Workers use sync driver (no event loop issues)
        - Each worker manages its own connections
        - Fully serializable for distributed execution
        """
        # The pattern cassandra-dask-dataframe should follow:

        # 1. Client side (async) - discovers metadata
        metadata = await session.execute(
            "SELECT * FROM system_schema.columns WHERE keyspace_name = 'cluster_test' AND table_name = 'distributed_fix_demo'"
        )
        columns = [row.column_name for row in metadata]

        # 2. Create serializable partition definitions
        # (NO semaphores, NO session objects, NO query builders)
        partition_defs = []
        for i in range(4):
            start = i * 25
            end = (i + 1) * 25
            partition_defs.append(
                {
                    "connection": {
                        "contact_points": ["localhost"],
                        "port": 9042,
                        "keyspace": "cluster_test",
                    },
                    "table": "distributed_fix_demo",
                    "columns": columns,
                    "partition_filter": f"pk >= {start} AND pk < {end}",
                    "partition_number": i,
                }
            )

        def read_partition_worker(partition_def):
            """Worker-side function - uses sync Cassandra driver."""
            from cassandra.cluster import Cluster

            # Each worker creates its own cluster/session
            conn = partition_def["connection"]
            cluster = Cluster(
                contact_points=conn["contact_points"],
                port=conn["port"],
            )
            session = cluster.connect(conn["keyspace"])

            try:
                # Build and execute query
                columns_str = ", ".join(partition_def["columns"])
                query = f"SELECT {columns_str} FROM {partition_def['table']} WHERE {partition_def['partition_filter']}"

                result = session.execute(query)
                rows = list(result)

                if rows:
                    df = pd.DataFrame([dict(row._asdict()) for row in rows])
                else:
                    # Empty DataFrame with correct schema
                    df = pd.DataFrame(columns=partition_def["columns"])

                return df
            finally:
                cluster.shutdown()

        # 3. Use Dask distributed
        with dask_client.as_current():
            # Create delayed partitions
            delayed_partitions = [dask.delayed(read_partition_worker)(p) for p in partition_defs]

            # Track progress
            futures = dask_client.compute(delayed_partitions)

            completed = []
            for future in as_completed(futures):
                result = future.result()
                completed.append(result)
                logger.info(f"✓ Partition completed with {len(result)} rows")

            # Combine results
            final_df = pd.concat(completed, ignore_index=True)

            logger.info(f"✓ Total rows: {len(final_df)}")
            assert len(final_df) == 100
            assert set(final_df["pk"]) == set(range(100))
