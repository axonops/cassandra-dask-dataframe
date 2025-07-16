"""
Test basic distributed functionality without custom dtypes.
"""

import logging

import pandas as pd
import pytest
from cassandra.query import BatchStatement

import cassandra_dask_dataframe as cdf

logger = logging.getLogger(__name__)


class TestBasicDistributed:
    """Basic distributed tests without complex types."""

    @pytest.mark.asyncio
    async def test_basic_types_distributed(self, session, dask_client):
        """
        Test distributed execution with basic types only.

        This avoids custom dtype issues to verify core functionality.
        """
        # Create table with only basic types
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS basic_types (
                id INT PRIMARY KEY,
                name TEXT,
                active BOOLEAN,
                score FLOAT
            )
            """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO basic_types (id, name, active, score)
            VALUES (?, ?, ?, ?)
            """
        )

        logger.info("Inserting 1000 rows...")
        batch = BatchStatement()
        for i in range(1000):
            batch.add(insert_stmt, (i, f"name_{i}", i % 2 == 0, float(i) * 0.1))
            if len(batch) >= 25:
                await session.execute(batch)
                batch = BatchStatement()
        if batch:
            await session.execute(batch)

        # Read with distributed Dask
        with dask_client.as_current():
            logger.info("Reading data with distributed Dask...")

            df = await cdf.read_cassandra_table(
                "basic_types", session=session, partition_count=4  # Force multiple partitions
            )

            logger.info(f"Created {df.npartitions} partitions")

            # Compute simple aggregation
            result = df["score"].sum().compute()

            logger.info(f"Sum of scores: {result}")

            # Expected sum: 0 + 0.1 + 0.2 + ... + 99.9 = 49950.0
            expected = sum(i * 0.1 for i in range(1000))
            assert abs(result - expected) < 0.01, f"Expected {expected}, got {result}"

            # Check row count
            count = len(df)
            logger.info(f"Total rows: {count}")
            assert count == 1000, f"Expected 1000 rows, got {count}"

            # Test filtering
            active_df = df[df["active"]]
            active_count = len(active_df)
            logger.info(f"Active rows: {active_count}")
            assert active_count == 500, f"Expected 500 active rows, got {active_count}"

    @pytest.mark.asyncio
    async def test_worker_distribution(self, session, dask_client):
        """
        Verify that work is actually distributed across workers.
        """
        # Use existing basic_types table
        try:
            await session.execute("SELECT COUNT(*) FROM basic_types")
        except Exception:
            # Create and populate if needed
            await self.test_basic_types_distributed(session, dask_client)

        with dask_client.as_current():
            df = await cdf.read_cassandra_table(
                "basic_types", session=session, partition_count=6  # More partitions than workers
            )

            # Track which workers process partitions
            def get_worker_info(partition):
                """Get worker information."""
                try:
                    from distributed import get_worker

                    worker = get_worker()
                    worker_id = worker.id
                except Exception:
                    worker_id = "local"

                return pd.DataFrame(
                    {
                        "worker_id": [worker_id],
                        "rows": [len(partition)],
                        "min_id": [partition["id"].min() if len(partition) > 0 else None],
                        "max_id": [partition["id"].max() if len(partition) > 0 else None],
                    }
                )

            # Apply to all partitions
            worker_info = df.map_partitions(get_worker_info)
            results = worker_info.compute()

            # Show distribution
            logger.info("\nPartition distribution:")
            for _, row in results.iterrows():
                if row["rows"] > 0:
                    logger.info(
                        f"Worker {row['worker_id']}: {row['rows']} rows (IDs {row['min_id']}-{row['max_id']})"
                    )

            # Verify multiple workers were used
            unique_workers = results["worker_id"].nunique()
            logger.info(f"\nTotal unique workers: {unique_workers}")
            assert unique_workers >= 2, f"Expected at least 2 workers, got {unique_workers}"

            # Verify all rows were processed
            total_rows = results["rows"].sum()
            assert total_rows == 1000, f"Expected 1000 rows, got {total_rows}"
