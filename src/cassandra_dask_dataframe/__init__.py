"""
cassandra-dask-dataframe: Dask DataFrame integration for Apache Cassandra.

This library provides distributed processing capabilities for Cassandra data
using Dask DataFrames, built on top of async-cassandra.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cassandra-dask-dataframe")
except PackageNotFoundError:
    __version__ = "unknown"

from .cassandra_dtypes import (
    CassandraDateDtype,
    CassandraDecimalDtype,
    CassandraDurationDtype,
    CassandraInetDtype,
    CassandraTimeUUIDDtype,
    CassandraUUIDDtype,
    CassandraVarintDtype,
)
from .cassandra_udt_dtype import CassandraUDTDtype
from .cassandra_writetime_dtype import CassandraWritetimeDtype
from .dask_dtype_registration import _ensure_dtypes_registered
from .reader import read_cassandra_table, stream_cassandra_table

# Ensure dtypes are registered with Dask
_ensure_dtypes_registered()

__all__ = [
    "__version__",
    "read_cassandra_table",
    "stream_cassandra_table",
    # Export dtypes for users
    "CassandraDateDtype",
    "CassandraDecimalDtype",
    "CassandraDurationDtype",
    "CassandraInetDtype",
    "CassandraTimeUUIDDtype",
    "CassandraUUIDDtype",
    "CassandraVarintDtype",
    "CassandraUDTDtype",
    "CassandraWritetimeDtype",
]
