"""
Test Dask cluster connectivity and basic operations.

What this tests:
---------------
1. Connection to distributed Dask scheduler
2. Worker registration and availability
3. Basic task distribution
4. Dask dashboard availability

Why this matters:
----------------
- Validates cluster setup before running integration tests
- Ensures workers are properly connected
- Verifies basic Dask functionality
"""

import logging
import time

import dask.array as da
from dask import delayed
from dask.distributed import as_completed

logger = logging.getLogger(__name__)


class TestDaskClusterConnectivity:
    """Test basic Dask cluster connectivity and operations."""

    def test_cluster_info(self, dask_client):
        """
        Test that Dask cluster is properly configured.

        Given: A running Dask cluster
        When: Querying cluster information
        Then: Should have expected number of workers and threads
        """
        info = dask_client.scheduler_info()
        workers = info["workers"]

        logger.info(f"Cluster has {len(workers)} workers")

        # We expect 3 workers from docker-compose
        assert len(workers) >= 3, f"Expected at least 3 workers, got {len(workers)}"

        # Check each worker configuration
        total_threads = 0
        total_memory = 0

        for worker_id, worker_info in workers.items():
            threads = worker_info["nthreads"]
            memory = worker_info["memory_limit"]

            logger.info(
                f"Worker {worker_id}: {threads} threads, {memory / (1024**3):.1f} GB memory"
            )

            total_threads += threads
            total_memory += memory

            # Each worker should have 2 threads as configured
            assert threads == 2, f"Expected 2 threads per worker, got {threads}"

        logger.info(
            f"Total cluster resources: {total_threads} threads, {total_memory / (1024**3):.1f} GB memory"
        )

    def test_simple_computation(self, dask_client):
        """
        Test simple distributed computation.

        Given: A Dask cluster
        When: Running a simple array computation
        Then: Should execute across workers and return correct result
        """
        # Create a large array computation
        x = da.random.random((10000, 10000), chunks=(1000, 1000))
        y = x + x.T
        result = y.mean()

        # Compute and track progress
        logger.info("Starting distributed computation...")
        start_time = time.time()

        computed_result = result.compute()

        elapsed = time.time() - start_time
        logger.info(f"Computation completed in {elapsed:.2f} seconds")

        # Result should be close to 1.0 (mean of random + random)
        assert 0.9 < computed_result < 1.1

        # Check that computation was distributed
        who_has = dask_client.who_has()
        workers_used = set()
        for key, worker_list in who_has.items():
            workers_used.update(worker_list)

        logger.info(f"Computation used {len(workers_used)} workers")
        assert len(workers_used) > 1, "Computation should use multiple workers"

    def test_delayed_tasks(self, dask_client):
        """
        Test delayed task execution across workers.

        Given: A Dask cluster
        When: Submitting multiple delayed tasks
        Then: Tasks should be distributed across workers
        """

        @delayed
        def slow_task(x):
            """Simulate a slow task."""
            import time

            time.sleep(0.5)
            return x**2

        # Create multiple tasks
        tasks = [slow_task(i) for i in range(12)]  # 12 tasks for 6 threads total

        logger.info("Submitting 12 delayed tasks...")
        start_time = time.time()

        # Compute all tasks
        results = dask_client.compute(tasks, sync=False)
        completed_results = dask_client.gather(results)

        elapsed = time.time() - start_time
        logger.info(f"All tasks completed in {elapsed:.2f} seconds")

        # With 6 threads total and 0.5s per task, should take ~1 second
        assert elapsed < 3.0, f"Tasks took too long ({elapsed:.2f}s), suggesting poor distribution"

        # Verify results
        expected = [i**2 for i in range(12)]
        assert completed_results == expected

    def test_future_submission(self, dask_client):
        """
        Test direct future submission to workers.

        Given: A Dask cluster
        When: Submitting futures directly
        Then: Should execute on different workers
        """

        def get_worker_info():
            """Get information about the executing worker."""
            import socket

            from distributed import get_worker

            worker = get_worker()
            return {
                "worker_id": worker.id,
                "hostname": socket.gethostname(),
                "threads": worker.nthreads,
            }

        # Submit tasks to get worker info
        futures = [dask_client.submit(get_worker_info) for _ in range(9)]

        # Gather results
        results = dask_client.gather(futures)

        # Count unique workers
        unique_workers = set(r["worker_id"] for r in results)
        logger.info(f"Tasks executed on {len(unique_workers)} unique workers")

        # Should use all 3 workers
        assert len(unique_workers) >= 3, f"Expected at least 3 workers, used {len(unique_workers)}"

        # Log distribution
        from collections import Counter

        worker_counts = Counter(r["worker_id"] for r in results)
        for worker_id, count in worker_counts.items():
            logger.info(f"Worker {worker_id}: executed {count} tasks")

    def test_scatter_gather(self, dask_client):
        """
        Test scatter and gather operations.

        Given: A Dask cluster
        When: Scattering data to workers and gathering results
        Then: Should properly distribute and collect data
        """
        # Create test data
        data = list(range(1000))

        # Scatter data to workers
        logger.info("Scattering data to workers...")
        scattered = dask_client.scatter(data, broadcast=True)

        # Define processing function
        def process_chunk(scattered_data, start, end):
            """Process a chunk of the scattered data."""
            return sum(scattered_data[start:end])

        # Submit tasks that use scattered data
        chunk_size = 100
        futures = []
        for i in range(0, len(data), chunk_size):
            future = dask_client.submit(process_chunk, scattered, i, i + chunk_size)
            futures.append(future)

        # Gather results
        results = dask_client.gather(futures)

        # Verify result
        total = sum(results)
        expected = sum(data)
        assert total == expected, f"Expected sum {expected}, got {total}"

        logger.info(f"Scatter-gather test successful: sum = {total}")

    def test_error_handling(self, dask_client):
        """
        Test error handling in distributed tasks.

        Given: A Dask cluster
        When: Tasks fail with exceptions
        Then: Should properly propagate errors
        """

        def failing_task(x):
            """Task that fails for certain inputs."""
            if x == 5:
                raise ValueError(f"Task failed for input {x}")
            return x * 2

        # Submit mix of succeeding and failing tasks
        futures = [dask_client.submit(failing_task, i) for i in range(10)]

        # Process results as they complete
        succeeded = []
        failed = []

        for future in as_completed(futures):
            try:
                result = future.result()
                succeeded.append(result)
            except ValueError as e:
                failed.append(str(e))

        logger.info(f"Succeeded: {len(succeeded)}, Failed: {len(failed)}")

        # Should have 1 failure (for x=5) and 9 successes
        assert len(failed) == 1
        assert len(succeeded) == 9
        assert "Task failed for input 5" in failed[0]

    def test_dashboard_availability(self, dask_client):
        """
        Test that Dask dashboard is accessible.

        Given: A Dask cluster with dashboard
        When: Checking dashboard URL
        Then: Dashboard should be available
        """
        dashboard_link = dask_client.dashboard_link
        logger.info(f"Dask dashboard available at: {dashboard_link}")

        # Dashboard should be on port 8787
        assert ":8787" in dashboard_link

        # Could add HTTP check here if needed
        # import requests
        # response = requests.get(f"{dashboard_link}/api/v1/health")
        # assert response.status_code == 200
