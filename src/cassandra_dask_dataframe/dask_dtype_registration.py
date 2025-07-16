"""
Dask registration for Cassandra custom dtypes.

This module registers our custom extension types with Dask so that it knows
how to create non-empty arrays for metadata operations.
"""

import numpy as np
from dask.dataframe.backends import make_array_nonempty

from cassandra_dask_dataframe.cassandra_dtypes import (
    CassandraDateDtype,
    CassandraDecimalDtype,
    CassandraDurationDtype,
    CassandraInetDtype,
    CassandraTimeUUIDDtype,
    CassandraUUIDDtype,
    CassandraVarintDtype,
)
from cassandra_dask_dataframe.cassandra_udt_dtype import CassandraUDTDtype
from cassandra_dask_dataframe.cassandra_writetime_dtype import CassandraWritetimeDtype


@make_array_nonempty.register(CassandraUUIDDtype)
def make_uuid_array_nonempty(dtype):
    """Create non-empty UUID array for Dask metadata."""
    # Create two different UUIDs for metadata
    import uuid

    return np.array([uuid.uuid4(), uuid.uuid4()], dtype=object)


@make_array_nonempty.register(CassandraTimeUUIDDtype)
def make_timeuuid_array_nonempty(dtype):
    """Create non-empty TimeUUID array for Dask metadata."""
    # Create two different TimeUUIDs for metadata
    import uuid

    return np.array([uuid.uuid1(), uuid.uuid1()], dtype=object)


@make_array_nonempty.register(CassandraDateDtype)
def make_date_array_nonempty(dtype):
    """Create non-empty date array for Dask metadata."""
    from datetime import date

    return np.array([date(2000, 1, 1), date(2000, 1, 2)], dtype=object)


@make_array_nonempty.register(CassandraDecimalDtype)
def make_decimal_array_nonempty(dtype):
    """Create non-empty decimal array for Dask metadata."""
    from decimal import Decimal

    return np.array([Decimal("1.23"), Decimal("4.56")], dtype=object)


@make_array_nonempty.register(CassandraVarintDtype)
def make_varint_array_nonempty(dtype):
    """Create non-empty varint array for Dask metadata."""
    return np.array([1, 2], dtype=object)


@make_array_nonempty.register(CassandraInetDtype)
def make_inet_array_nonempty(dtype):
    """Create non-empty inet array for Dask metadata."""
    from ipaddress import ip_address

    return np.array([ip_address("192.168.1.1"), ip_address("192.168.1.2")], dtype=object)


@make_array_nonempty.register(CassandraDurationDtype)
def make_duration_array_nonempty(dtype):
    """Create non-empty duration array for Dask metadata."""
    from cassandra.util import Duration

    return np.array([Duration(months=1), Duration(months=2)], dtype=object)


@make_array_nonempty.register(CassandraUDTDtype)
def make_udt_array_nonempty(dtype):
    """Create non-empty UDT array for Dask metadata."""
    # Create two None values for UDT metadata
    # This is safe because we don't actually use the values, just the shape
    return np.array([None, None], dtype=object)


@make_array_nonempty.register(CassandraWritetimeDtype)
def make_writetime_array_nonempty(dtype):
    """Create non-empty writetime array for Dask metadata."""
    # Writetimes are microseconds since Unix epoch
    return np.array([1000000000000, 2000000000000], dtype=object)


# Ensure registration happens on import
def _ensure_dtypes_registered():
    """Ensure all custom dtypes are registered with Dask."""
    # The decorators above handle registration when the module is imported
    pass
