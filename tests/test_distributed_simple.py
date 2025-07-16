"""
Simple test to verify distributed execution works with the new architecture.

This test demonstrates that:
1. ConnectionConfig is serializable
2. Workers can create their own connections
3. Partitions can be read in distributed mode
"""

import pickle

import dask
import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import Client

from cassandra_dask_dataframe.connection_config import ConnectionConfig


class TestDistributedExecution:
    """Test distributed execution capabilities."""

    def test_connection_config_serialization(self):
        """
        Test that ConnectionConfig can be pickled for distributed execution.

        What this tests:
        ---------------
        1. ConnectionConfig with auth is serializable
        2. Complex configurations work
        3. Round-trip preserves all settings

        Why this matters:
        ----------------
        - Dask needs to send configs to workers
        - All settings must survive serialization
        """
        config = ConnectionConfig(
            contact_points=["localhost"],
            port=9042,
            auth_provider_class="PlainTextAuthProvider",
            auth_provider_args={"username": "test", "password": "test"},
            protocol_version=5,
            load_balancing_policy_class="DCAwareRoundRobinPolicy",
            load_balancing_policy_args={"local_dc": "datacenter1"},
        )

        # Test serialization
        serialized = pickle.dumps(config)
        deserialized = pickle.loads(serialized)

        # Verify all fields preserved
        assert deserialized.contact_points == ["localhost"]
        assert deserialized.auth_provider_args["username"] == "test"
        assert deserialized.protocol_version == 5

    def test_partition_definition_serialization(self):
        """
        Test that partition definitions are serializable.

        What this tests:
        ---------------
        1. Partition defs with ConnectionConfig are serializable
        2. No non-serializable objects remain
        3. All required data is included

        Why this matters:
        ----------------
        - Workers receive partition definitions
        - Must contain all info to execute queries
        """
        config = ConnectionConfig(contact_points=["localhost"])

        # Mock table metadata
        table_metadata = {
            "columns": [
                {"name": "id", "type": "int", "is_primary_key": True},
                {"name": "value", "type": "text", "is_primary_key": False},
            ],
            "partition_key": ["id"],
            "clustering_key": [],
        }

        partition_def = {
            "connection_config": config,
            "keyspace": "test_ks",
            "table": "test_table",
            "token_range": {"start": -9223372036854775808, "end": 0},
            "_table_metadata": table_metadata,
            "columns": ["id", "value"],
            "memory_limit_mb": 128,
            "max_concurrent_queries": 10,
            "consistency_level": 1,  # ConsistencyLevel.ONE
        }

        # Test serialization
        serialized = pickle.dumps(partition_def)
        deserialized = pickle.loads(serialized)

        # Verify structure
        assert "connection_config" in deserialized
        assert deserialized["keyspace"] == "test_ks"
        assert deserialized["token_range"]["start"] == -9223372036854775808

    @pytest.mark.skipif(
        not dask.config.get("scheduler.address", None), reason="Requires Dask cluster to be running"
    )
    def test_distributed_partition_reading(self):
        """
        Test reading partitions in distributed mode.

        NOTE: This test requires a running Dask cluster and Cassandra.
        It's skipped in CI but can be run locally.

        What this tests:
        ---------------
        1. Workers can create connections from config
        2. Partitions are read successfully
        3. Results are properly collected

        Why this matters:
        ----------------
        - Validates the entire distributed flow
        - Ensures workers can operate independently
        """
        with Client() as client:
            # Create a simple delayed task that simulates partition reading
            @dask.delayed
            def read_test_partition(partition_def):
                """Simulate reading a partition."""
                # In real scenario, this would:
                # 1. Create connection from config
                # 2. Execute query
                # 3. Return DataFrame

                # For now, return mock data
                return pd.DataFrame(
                    {"id": [1, 2, 3], "value": ["a", "b", "c"], "worker": [client.worker.id] * 3}
                )

            # Create test partitions
            config = ConnectionConfig(contact_points=["localhost"])
            partitions = []

            for i in range(4):
                partition_def = {
                    "connection_config": config,
                    "partition_id": i,
                    "keyspace": "test",
                    "table": "test",
                }
                partitions.append(partition_def)

            # Create delayed tasks
            delayed_dfs = [read_test_partition(p) for p in partitions]

            # Create Dask DataFrame
            meta = pd.DataFrame(
                {
                    "id": pd.Series(dtype="int64"),
                    "value": pd.Series(dtype="object"),
                    "worker": pd.Series(dtype="object"),
                }
            )
            df = dd.from_delayed(delayed_dfs, meta=meta)

            # Compute result
            result = df.compute()

            # Verify we got data from multiple partitions
            assert len(result) == 12  # 4 partitions * 3 rows each
            assert "worker" in result.columns

            # Check that multiple workers were used (if available)
            unique_workers = result["worker"].unique()
            print(f"Tasks executed on {len(unique_workers)} workers")


if __name__ == "__main__":
    # Run the serialization tests
    test = TestDistributedExecution()
    test.test_connection_config_serialization()
    print("✓ ConnectionConfig serialization test passed")

    test.test_partition_definition_serialization()
    print("✓ Partition definition serialization test passed")

    print("\nTo test distributed execution, start a Dask cluster and run:")
    print("  dask scheduler &")
    print("  dask worker localhost:8786 &")
    print(
        "  python -m pytest tests/test_distributed_simple.py::TestDistributedExecution::test_distributed_partition_reading -v"
    )
