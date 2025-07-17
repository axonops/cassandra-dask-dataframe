"""
Partition reading logic for Cassandra DataFrames.

Handles the actual reading of individual partitions with type conversion
and concurrency control.
"""

# mypy: ignore-errors

import asyncio
from typing import Any

import pandas as pd

from .cassandra_dtypes import (
    CassandraDateArray,
    CassandraDateDtype,
    CassandraDecimalArray,
    CassandraDecimalDtype,
    CassandraDurationArray,
    CassandraDurationDtype,
    CassandraInetArray,
    CassandraInetDtype,
    CassandraTimeUUIDArray,
    CassandraTimeUUIDDtype,
    CassandraUUIDArray,
    CassandraUUIDDtype,
    CassandraVarintArray,
    CassandraVarintDtype,
)
from .cassandra_udt_dtype import CassandraUDTArray, CassandraUDTDtype
from .partition import StreamingPartitionStrategy
from .query_builder import QueryBuilder
from .types import CassandraTypeMapper


class PartitionReader:
    """Reads individual partitions from Cassandra."""

    @staticmethod
    def read_partition_sync(
        partition_def: dict[str, Any],
        session,
    ) -> pd.DataFrame:
        """
        Synchronous wrapper for Dask delayed execution (local mode).

        Runs the async partition reader using a shared event loop.
        """
        # For local execution, use asyncio.run for simplicity
        return asyncio.run(PartitionReader.read_partition(partition_def, session))

    @staticmethod
    def read_partition_distributed(
        partition_def: dict[str, Any],
    ) -> pd.DataFrame:
        """
        Synchronous wrapper for Dask distributed execution.

        Creates a new connection on the worker and reads the partition.
        """
        # Create new event loop for this worker thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Run the distributed partition reader
            return loop.run_until_complete(
                PartitionReader.read_partition_distributed_async(partition_def)
            )
        finally:
            loop.close()

    @staticmethod
    async def read_partition(
        partition_def: dict[str, Any],
        session,
    ) -> pd.DataFrame:
        """
        Read a single partition with concurrency control (local mode).

        This is executed locally with an existing session.
        """
        # Extract metadata and create helpers
        table_metadata = partition_def["_table_metadata"]
        query_builder = QueryBuilder(table_metadata)
        type_mapper = CassandraTypeMapper()
        writetime_columns = partition_def.get("writetime_columns")
        ttl_columns = partition_def.get("ttl_columns")

        # Apply concurrency control if configured
        max_concurrent = partition_def.get("max_concurrent_queries")
        if max_concurrent:
            semaphore = asyncio.Semaphore(max_concurrent)
            async with semaphore:
                return await PartitionReader._read_partition_impl(
                    partition_def,
                    session,
                    query_builder,
                    type_mapper,
                    writetime_columns,
                    ttl_columns,
                )
        else:
            return await PartitionReader._read_partition_impl(
                partition_def, session, query_builder, type_mapper, writetime_columns, ttl_columns
            )

    @staticmethod
    async def read_partition_distributed_async(
        partition_def: dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read a single partition in distributed mode.

        Creates a new connection on the worker.
        """
        # Ensure custom dtypes are registered on worker
        from .dask_dtype_registration import _ensure_dtypes_registered

        _ensure_dtypes_registered()

        # Extract connection config
        connection_config = partition_def["connection_config"]
        keyspace = partition_def["keyspace"]

        # Create cluster and session on worker
        # Check for environment variable override for contact points (useful in containerized environments)
        import os

        if os.getenv("CASSANDRA_CONTACT_POINTS"):
            # Override contact points from environment
            contact_points = os.getenv("CASSANDRA_CONTACT_POINTS").split(",")
            connection_config.contact_points = contact_points
            print(f"DEBUG: Overriding contact points to: {contact_points}")
        else:
            print(f"DEBUG: Using original contact points: {connection_config.contact_points}")

        cluster = connection_config.create_cluster()
        session = cluster.connect(keyspace)

        try:
            # Wrap in async session
            from async_cassandra import AsyncCassandraSession

            async_session = AsyncCassandraSession(session)

            # Extract metadata and create helpers
            table_metadata = partition_def["_table_metadata"]
            query_builder = QueryBuilder(table_metadata)
            type_mapper = CassandraTypeMapper()
            writetime_columns = partition_def.get("writetime_columns")
            ttl_columns = partition_def.get("ttl_columns")

            # Apply concurrency control if configured
            max_concurrent = partition_def.get("max_concurrent_queries")
            if max_concurrent:
                semaphore = asyncio.Semaphore(max_concurrent)
                async with semaphore:
                    return await PartitionReader._read_partition_impl(
                        partition_def,
                        async_session,
                        query_builder,
                        type_mapper,
                        writetime_columns,
                        ttl_columns,
                    )
            else:
                return await PartitionReader._read_partition_impl(
                    partition_def,
                    async_session,
                    query_builder,
                    type_mapper,
                    writetime_columns,
                    ttl_columns,
                )
        finally:
            # Clean up connection
            cluster.shutdown()

    @staticmethod
    async def _read_partition_impl(
        partition_def: dict[str, Any],
        session,
        query_builder,
        type_mapper,
        writetime_columns,
        ttl_columns,
    ) -> pd.DataFrame:
        """Implementation of partition reading."""
        # Use streaming partition strategy to read data
        strategy = StreamingPartitionStrategy(
            session=session,
            memory_per_partition_mb=partition_def["memory_limit_mb"],
        )

        # Stream the partition
        df = await strategy.stream_partition(partition_def)

        # Apply type conversions based on table metadata
        if df.empty:
            # For empty DataFrames, ensure columns have correct dtypes
            df = PartitionReader._create_empty_dataframe(
                partition_def, type_mapper, writetime_columns, ttl_columns
            )
        else:
            # Apply conversions to non-empty DataFrames
            df = PartitionReader._apply_type_conversions(
                df, partition_def, type_mapper, writetime_columns, ttl_columns
            )

            # Apply NULL semantics
            df = type_mapper.handle_null_values(df, partition_def["_table_metadata"])

        return df

    @staticmethod
    def _create_empty_dataframe(
        partition_def: dict[str, Any],
        type_mapper,
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
    ) -> pd.DataFrame:
        """Create empty DataFrame with correct schema."""
        schema = {}
        columns = partition_def["columns"]

        for col in columns:
            col_info = next(
                (c for c in partition_def["_table_metadata"]["columns"] if c["name"] == col),
                None,
            )
            if col_info:
                col_type = str(col_info["type"])
                pandas_dtype = type_mapper.get_pandas_dtype(
                    col_type, partition_def["_table_metadata"]
                )
                schema[col] = pandas_dtype

        # Add writetime columns
        if writetime_columns:
            from .cassandra_writetime_dtype import CassandraWritetimeDtype

            for col in writetime_columns:
                schema[f"{col}_writetime"] = CassandraWritetimeDtype()

        # Add TTL columns
        if ttl_columns:
            for col in ttl_columns:
                schema[f"{col}_ttl"] = "Int64"  # Nullable int64

        # Create empty DataFrame with correct schema
        return type_mapper.create_empty_dataframe(schema)

    @staticmethod
    def _apply_type_conversions(
        df: pd.DataFrame,
        partition_def: dict[str, Any],
        type_mapper,
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
    ) -> pd.DataFrame:
        """Apply type conversions to DataFrame columns."""
        from .cassandra_writetime_dtype import CassandraWritetimeDtype

        for col in df.columns:
            if col.endswith("_writetime") and writetime_columns:
                # Keep writetime as raw microseconds for CassandraWritetimeDtype
                # The values are already in microseconds from Cassandra
                df[col] = df[col].astype(CassandraWritetimeDtype())
            elif col.endswith("_ttl") and ttl_columns:
                # Convert TTL to nullable Int64
                df[col] = pd.Series(df[col], dtype="Int64")
            else:
                # Apply type conversion based on column metadata
                col_info = next(
                    (c for c in partition_def["_table_metadata"]["columns"] if c["name"] == col),
                    None,
                )
                if col_info:
                    # Get the pandas dtype for this column
                    col_type = str(col_info["type"])
                    pandas_dtype = type_mapper.get_pandas_dtype(
                        col_type, partition_def["_table_metadata"]
                    )

                    # Convert the column to the expected dtype
                    if isinstance(
                        pandas_dtype,
                        CassandraDateDtype
                        | CassandraDecimalDtype
                        | CassandraVarintDtype
                        | CassandraInetDtype
                        | CassandraUUIDDtype
                        | CassandraTimeUUIDDtype
                        | CassandraDurationDtype
                        | CassandraUDTDtype,
                    ):
                        # Convert to appropriate Cassandra extension array
                        values = df[col].apply(
                            lambda x, ct=col_type: (
                                type_mapper.convert_value(x, ct) if pd.notna(x) else None
                            )
                        )

                        # Create the appropriate array type
                        if isinstance(pandas_dtype, CassandraDateDtype):
                            df[col] = pd.Series(CassandraDateArray(values, pandas_dtype), name=col)  # type: ignore[arg-type]
                        elif isinstance(pandas_dtype, CassandraDecimalDtype):
                            df[col] = pd.Series(
                                CassandraDecimalArray(values, pandas_dtype), name=col  # type: ignore[arg-type]
                            )
                        elif isinstance(pandas_dtype, CassandraVarintDtype):
                            df[col] = pd.Series(
                                CassandraVarintArray(values, pandas_dtype), name=col  # type: ignore[arg-type]
                            )
                        elif isinstance(pandas_dtype, CassandraInetDtype):
                            df[col] = pd.Series(CassandraInetArray(values, pandas_dtype), name=col)  # type: ignore[arg-type]
                        elif isinstance(pandas_dtype, CassandraUUIDDtype):
                            df[col] = pd.Series(CassandraUUIDArray(values, pandas_dtype), name=col)  # type: ignore[arg-type]
                        elif isinstance(pandas_dtype, CassandraTimeUUIDDtype):
                            df[col] = pd.Series(
                                CassandraTimeUUIDArray(values, pandas_dtype), name=col  # type: ignore[arg-type]
                            )
                        elif isinstance(pandas_dtype, CassandraDurationDtype):
                            df[col] = pd.Series(
                                CassandraDurationArray(values, pandas_dtype), name=col  # type: ignore[arg-type]
                            )
                        elif isinstance(pandas_dtype, CassandraUDTDtype):
                            df[col] = pd.Series(CassandraUDTArray(values, pandas_dtype), name=col)  # type: ignore[arg-type]

                    elif pandas_dtype == "object":
                        # No conversion needed for object types
                        pass
                    # Handle nullable integer types
                    elif pandas_dtype in ["Int8", "Int16", "Int32", "Int64"]:
                        # Check if all values are None
                        if df[col].isna().all():
                            # Create a Series with all pd.NA values and correct dtype
                            df[col] = pd.Series([pd.NA] * len(df), dtype=pandas_dtype)
                        else:
                            df[col] = df[col].astype(pandas_dtype)
                    # Handle nullable boolean
                    elif pandas_dtype == "boolean":
                        # Check if all values are None
                        if df[col].isna().all():
                            # Create a Series with all pd.NA values and correct dtype
                            df[col] = pd.Series([pd.NA] * len(df), dtype="boolean")
                        else:
                            # Convert to boolean, but first convert numpy booleans to Python booleans
                            df[col] = (
                                df[col]
                                .apply(
                                    lambda x: (
                                        bool(x) if pd.notna(x) and hasattr(x, "__bool__") else x
                                    )
                                )
                                .astype("boolean")
                            )
                    # Handle nullable float types
                    elif pandas_dtype in ["Float32", "Float64"]:
                        # Check if all values are None
                        if df[col].isna().all():
                            # Create a Series with all pd.NA values and correct dtype
                            df[col] = pd.Series([pd.NA] * len(df), dtype=pandas_dtype)
                        else:
                            df[col] = df[col].astype(pandas_dtype)
                    # Handle nullable string type
                    elif pandas_dtype == "string":
                        # Check if all values are None
                        if df[col].isna().all():
                            # Create a Series with all pd.NA values and correct dtype
                            df[col] = pd.Series([pd.NA] * len(df), dtype="string")
                        else:
                            df[col] = df[col].astype("string")
                    # Handle temporal types
                    elif pandas_dtype == "datetime64[ns]":
                        # This is for timestamp type, not date
                        # First check if the column is all None/object dtype
                        if df[col].dtype == "object" and df[col].isna().all():
                            # Force to datetime64[ns] with all NaT values
                            df[col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[ns]")
                        else:
                            # Apply normal conversion
                            df[col] = df[col].apply(
                                lambda x, ct=col_type: (
                                    type_mapper.convert_value(x, ct) if pd.notna(x) else pd.NaT
                                )
                            )
                            # Ensure the column has the correct dtype even after conversion
                            if df[col].dtype != "datetime64[ns]":
                                try:
                                    df[col] = pd.to_datetime(df[col])
                                except (pd.errors.OutOfBoundsDatetime, OverflowError):
                                    # Keep as object dtype for dates outside pandas range
                                    pass
                    elif pandas_dtype == "timedelta64[ns]":
                        # Convert time type
                        # First check if the column is all None/object dtype
                        if df[col].dtype == "object" and df[col].isna().all():
                            # Force to timedelta64[ns] with all NaT values
                            df[col] = pd.Series([pd.NaT] * len(df), dtype="timedelta64[ns]")
                        else:
                            # Apply normal conversion
                            converted_values = []
                            for x in df[col]:
                                if pd.notna(x):
                                    val = type_mapper.convert_value(x, col_type)
                                    # Ensure we have a timedelta
                                    if isinstance(val, pd.Timedelta):
                                        converted_values.append(val)
                                    elif (
                                        hasattr(val, "__class__")
                                        and val.__class__.__name__ == "datetime"
                                    ):
                                        # If somehow we got a datetime, convert to timedelta from midnight
                                        converted_values.append(
                                            pd.Timedelta(
                                                hours=val.hour,
                                                minutes=val.minute,
                                                seconds=val.second,
                                                microseconds=val.microsecond,
                                            )
                                        )
                                    else:
                                        converted_values.append(val)
                                else:
                                    converted_values.append(pd.NaT)  # type: ignore[arg-type]
                            df[col] = pd.Series(converted_values, dtype="timedelta64[ns]")
                    elif pandas_dtype == "datetime64[ns, UTC]":
                        # Ensure timestamp columns have UTC timezone
                        # First check if the column is all None/object dtype
                        if df[col].dtype == "object" and df[col].isna().all():
                            # Force to datetime64[ns, UTC] with all NaT values
                            df[col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[ns, UTC]")
                        else:
                            # Apply normal conversion
                            df[col] = pd.to_datetime(df[col], utc=True)
                    # For complex types (UDTs, collections), always apply custom conversion
                    elif (
                        pandas_dtype == "object" or col_type.startswith("frozen") or "<" in col_type
                    ):
                        df[col] = df[col].apply(
                            lambda x, ct=col_type: type_mapper.convert_value(x, ct)
                        )
                    # Check for UDTs by checking if it's not a known simple type
                    elif col_type not in [
                        "text",
                        "varchar",
                        "ascii",
                        "blob",
                        "boolean",
                        "tinyint",
                        "smallint",
                        "int",
                        "bigint",
                        "varint",
                        "decimal",
                        "float",
                        "double",
                        "counter",
                        "timestamp",
                        "date",
                        "time",
                        "timeuuid",
                        "uuid",
                        "inet",
                        "duration",
                    ]:
                        # This is likely a UDT
                        df[col] = df[col].apply(
                            lambda x, ct=col_type: type_mapper.convert_value(x, ct)
                        )

        return df
