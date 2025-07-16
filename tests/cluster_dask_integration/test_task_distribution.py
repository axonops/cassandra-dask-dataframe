"""
Test task distribution for async-cassandra-dataframe on Dask cluster.

What this tests:
---------------
1. DataFrame partitions are distributed across workers
2. Each worker processes its assigned partitions
3. Load balancing works correctly
4. Memory usage is distributed

Why this matters:
----------------
- Validates that our library properly distributes work
- Ensures no single worker bottleneck
- Tests real distributed DataFrame operations
"""

import logging
import time
from collections import defaultdict

import dask.dataframe as dd
import pandas as pd
import pytest

logger = logging.getLogger(__name__)


class TestTaskDistribution:
    """Test how tasks are distributed across Dask workers."""

    def test_dataframe_partition_distribution(self, dask_client):
        """
        Test that DataFrame partitions are distributed across workers.

        Given: A Dask DataFrame with multiple partitions
        When: Computing operations on it
        Then: Partitions should be processed by different workers
        """
        # Create a test DataFrame with known partitions
        npartitions = 12  # 4 per worker
        df = dd.from_pandas(
            pd.DataFrame(
                {
                    "id": range(10000),
                    "value": range(10000),
                    "group": [f"group_{i % 10}" for i in range(10000)],
                }
            ),
            npartitions=npartitions,
        )

        # Add a function that identifies which worker processes each partition
        def identify_worker(partition):
            """Add worker ID to each partition."""
            from distributed import get_worker

            worker = get_worker()
            partition["worker_id"] = worker.id
            partition["worker_host"] = worker.name
            return partition

        # Apply worker identification
        df_with_worker = df.map_partitions(identify_worker)

        # Compute and collect worker distribution
        logger.info(f"Processing {npartitions} partitions across cluster...")
        start_time = time.time()

        result = df_with_worker.compute()

        elapsed = time.time() - start_time
        logger.info(f"Computation completed in {elapsed:.2f} seconds")

        # Analyze worker distribution
        worker_distribution = result.groupby("worker_id").size()
        logger.info("Partition distribution across workers:")
        for worker_id, count in worker_distribution.items():
            logger.info(f"  {worker_id}: {count} rows")

        # Should use all 3 workers
        unique_workers = result["worker_id"].nunique()
        assert unique_workers >= 3, f"Expected at least 3 workers, got {unique_workers}"

        # Check relatively even distribution (within 50% of average)
        avg_rows = len(result) / unique_workers
        for worker_id, count in worker_distribution.items():
            assert (
                0.5 * avg_rows <= count <= 1.5 * avg_rows
            ), f"Worker {worker_id} has uneven load: {count} rows (avg: {avg_rows})"

    def test_groupby_shuffle_distribution(self, dask_client):
        """
        Test that shuffle operations distribute across workers.

        Given: A groupby operation requiring shuffle
        When: Computing aggregations
        Then: Shuffle should involve all workers
        """
        # Create DataFrame that will require shuffling
        df = dd.from_pandas(
            pd.DataFrame({"key": [f"key_{i % 100}" for i in range(50000)], "value": range(50000)}),
            npartitions=20,
        )

        # Track which workers handle which keys during shuffle
        def track_shuffle(partition):
            """Track worker handling each partition during shuffle."""
            from distributed import get_worker

            worker = get_worker()

            # Add tracking info
            partition = partition.copy()
            partition["shuffle_worker"] = worker.id
            return partition

        # Perform groupby with tracking
        logger.info("Performing groupby operation with shuffle...")
        df_tracked = df.map_partitions(track_shuffle)

        # Groupby and aggregate
        result = df_tracked.groupby("key").agg(
            {"value": "sum", "shuffle_worker": lambda x: list(set(x))[:3]}  # Sample workers
        )

        computed_result = result.compute()

        # Analyze shuffle distribution
        all_workers = set()
        for workers_list in computed_result["shuffle_worker"]:
            all_workers.update(workers_list)

        logger.info(f"Shuffle operation used {len(all_workers)} workers: {all_workers}")

        # Shuffle should use all workers
        assert len(all_workers) >= 3, f"Shuffle should use all workers, got {len(all_workers)}"

    def test_task_stealing(self, dask_client):
        """
        Test that task stealing works for load balancing.

        Given: Uneven task distribution
        When: Some workers finish early
        Then: They should steal tasks from busy workers
        """

        def variable_duration_task(x):
            """Task with variable duration based on input."""
            import time

            from distributed import get_worker

            # Some tasks take longer
            if x % 10 == 0:
                duration = 2.0  # Slow tasks
            else:
                duration = 0.1  # Fast tasks

            time.sleep(duration)

            return {"input": x, "worker": get_worker().id, "duration": duration}

        # Submit tasks with variable duration
        logger.info("Submitting tasks with variable duration...")
        futures = [dask_client.submit(variable_duration_task, i) for i in range(30)]

        # Wait for completion and gather results
        start_time = time.time()
        results = dask_client.gather(futures)
        elapsed = time.time() - start_time

        logger.info(f"All tasks completed in {elapsed:.2f} seconds")

        # Analyze task distribution
        worker_tasks = defaultdict(list)
        slow_tasks = 0
        fast_tasks = 0

        for result in results:
            worker_tasks[result["worker"]].append(result["input"])
            if result["duration"] > 1:
                slow_tasks += 1
            else:
                fast_tasks += 1

        logger.info(f"Task distribution: {slow_tasks} slow, {fast_tasks} fast")
        for worker, tasks in worker_tasks.items():
            logger.info(f"  {worker}: {len(tasks)} tasks")

        # With task stealing, no worker should have significantly more tasks
        task_counts = [len(tasks) for tasks in worker_tasks.values()]
        max_diff = max(task_counts) - min(task_counts)
        assert max_diff <= 10, f"Task distribution too uneven: {task_counts}"

    def test_memory_distribution(self, dask_client):
        """
        Test that memory usage is distributed across workers.

        Given: Large DataFrame operations
        When: Processing data
        Then: Memory should be distributed, not concentrated
        """
        # Create a memory-intensive DataFrame
        df = dd.from_pandas(
            pd.DataFrame(
                {"id": range(100000), "data": ["x" * 1000 for _ in range(100000)]}  # ~100MB
            ),
            npartitions=24,  # 8 per worker ideally
        )

        # Function to check memory on each worker
        def get_memory_usage():
            """Get current memory usage of worker."""
            import psutil
            from distributed import get_worker

            worker = get_worker()
            process = psutil.Process()

            return {
                "worker_id": worker.id,
                "memory_mb": process.memory_info().rss / 1024 / 1024,
                "memory_percent": process.memory_percent(),
            }

        # Get baseline memory
        baseline_futures = [
            dask_client.submit(get_memory_usage, pure=False)
            for _ in range(len(dask_client.workers()))
        ]
        baseline_memory = dask_client.gather(baseline_futures)

        logger.info("Baseline memory usage:")
        for mem in baseline_memory:
            logger.info(
                f"  {mem['worker_id']}: {mem['memory_mb']:.1f} MB ({mem['memory_percent']:.1f}%)"
            )

        # Perform memory-intensive operation
        logger.info("Processing large DataFrame...")
        df.groupby("id").agg({"data": "first"}).compute()

        # Check memory after operation
        after_futures = [
            dask_client.submit(get_memory_usage, pure=False)
            for _ in range(len(dask_client.workers()))
        ]
        after_memory = dask_client.gather(after_futures)

        logger.info("Memory usage after processing:")
        memory_increases = []
        for _, mem in enumerate(after_memory):
            baseline = next(b for b in baseline_memory if b["worker_id"] == mem["worker_id"])
            increase = mem["memory_mb"] - baseline["memory_mb"]
            memory_increases.append(increase)
            logger.info(
                f"  {mem['worker_id']}: {mem['memory_mb']:.1f} MB "
                f"(+{increase:.1f} MB, {mem['memory_percent']:.1f}%)"
            )

        # Memory increase should be relatively distributed
        avg_increase = sum(memory_increases) / len(memory_increases)
        for increase in memory_increases:
            # Allow 2x variance in memory increase
            assert (
                0.5 * avg_increase <= increase <= 2 * avg_increase
            ), f"Memory distribution too uneven: {memory_increases}"

    def test_fault_tolerance(self, dask_client):
        """
        Test that computation continues despite task failures.

        Given: Tasks that may fail
        When: Running distributed computation
        Then: Should complete successfully with retries
        """
        failure_count = 0

        def unreliable_task(x):
            """Task that fails occasionally but succeeds on retry."""
            import random

            from distributed import get_worker

            nonlocal failure_count

            # Fail 20% of first attempts
            if random.random() < 0.2 and failure_count < 5:
                failure_count += 1
                raise RuntimeError(f"Simulated failure for task {x}")

            return {"value": x * 2, "worker": get_worker().id}

        # Submit tasks
        logger.info("Submitting tasks that may fail...")
        futures = [dask_client.submit(unreliable_task, i, retries=2) for i in range(50)]

        # Gather results
        results = dask_client.gather(futures)

        # All tasks should eventually succeed
        assert len(results) == 50

        # Verify results
        for i, result in enumerate(results):
            assert result["value"] == i * 2

        # Check worker distribution (should still be distributed)
        workers_used = {r["worker"] for r in results}
        logger.info(f"Despite failures, used {len(workers_used)} workers")
        assert len(workers_used) >= 2  # At least 2 workers should be used

    @pytest.mark.parametrize("n_partitions", [6, 12, 24, 48])
    def test_partition_scaling(self, dask_client, n_partitions):
        """
        Test how different partition counts affect distribution.

        Given: DataFrames with different partition counts
        When: Processing them
        Then: Should distribute appropriately based on worker count
        """
        # Create DataFrame with specified partitions
        df = dd.from_pandas(
            pd.DataFrame({"id": range(10000), "value": range(10000)}), npartitions=n_partitions
        )

        # Track partition execution
        def track_partition(partition):
            """Track which worker processes partition."""
            from distributed import get_worker

            return pd.DataFrame(
                {"partition_size": [len(partition)], "worker_id": [get_worker().id]}
            )

        # Process and track
        tracking = df.map_partitions(track_partition)
        result = tracking.compute()

        # Analyze distribution
        worker_stats = result.groupby("worker_id")["partition_size"].agg(["count", "sum"])

        logger.info(f"Partition distribution for {n_partitions} partitions:")
        for worker_id, stats in worker_stats.iterrows():
            logger.info(f"  {worker_id}: {stats['count']} partitions, {stats['sum']} total rows")

        # With 3 workers, distribution should be reasonable
        worker_partition_counts = worker_stats["count"].values

        if n_partitions >= 6:  # Only check distribution if we have enough partitions
            max_partitions = max(worker_partition_counts)
            min_partitions = min(worker_partition_counts)

            # No worker should have more than 2x the minimum
            assert max_partitions <= max(
                2 * min_partitions, min_partitions + 2
            ), f"Uneven partition distribution: {worker_partition_counts}"
