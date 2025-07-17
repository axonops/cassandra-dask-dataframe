"""
Unit tests for Cassandra value serializers.

What this tests:
---------------
1. Writetime serialization/deserialization
2. TTL serialization/deserialization
3. Timezone handling
4. Edge cases and None values

Why this matters:
----------------
- Data integrity for special Cassandra values
- Correct timestamp conversions
- TTL accuracy
"""

import pandas as pd

from cassandra_dask_dataframe.serializers import (
    seconds_to_ttl,
    timedelta_to_ttl,
    timestamp_to_writetime,
    ttl_to_seconds,
    ttl_to_timedelta,
    writetime_to_timestamp,
)


class TestWritetime:
    """Test writetime serialization functionality."""

    def test_to_timestamp_valid(self):
        """Test converting writetime to timestamp."""
        # Cassandra writetime for 2024-01-15 10:30:00 UTC
        # Create a known timestamp first
        expected = pd.Timestamp("2024-01-15 10:30:00", tz="UTC")
        writetime = int(expected.timestamp() * 1_000_000)

        result = writetime_to_timestamp(writetime)

        assert isinstance(result, pd.Timestamp)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.tz is not None  # Should have timezone

    def test_to_timestamp_none(self):
        """Test converting None writetime."""
        assert writetime_to_timestamp(None) is None

    def test_from_timestamp_valid(self):
        """Test converting timestamp to writetime."""
        # Create timestamp
        ts = pd.Timestamp("2024-01-15 10:30:00", tz="UTC")

        result = timestamp_to_writetime(ts)

        assert isinstance(result, int)
        # Verify it converts back correctly
        assert writetime_to_timestamp(result) == ts

    def test_from_timestamp_with_timezone(self):
        """Test converting timestamp with different timezone."""
        # Create timestamp in different timezone
        ts = pd.Timestamp("2024-01-15 10:30:00", tz="America/New_York")

        result = timestamp_to_writetime(ts)

        # Should be converted to UTC
        assert isinstance(result, int)
        # Verify the UTC conversion is correct
        ts_utc = ts.tz_convert("UTC")
        assert writetime_to_timestamp(result) == ts_utc

    def test_from_timestamp_naive(self):
        """Test converting naive timestamp (no timezone)."""
        # Create naive timestamp
        ts = pd.Timestamp("2024-01-15 10:30:00")

        result = timestamp_to_writetime(ts)

        # Should assume UTC
        assert isinstance(result, int)
        # Verify it converts back to the same time when interpreted as UTC
        ts_back = writetime_to_timestamp(result)
        assert ts_back.year == 2024
        assert ts_back.month == 1
        assert ts_back.day == 15
        assert ts_back.hour == 10
        assert ts_back.minute == 30
        assert ts_back.tz is not None  # Should have UTC timezone

    def test_from_timestamp_none(self):
        """Test converting None timestamp."""
        assert timestamp_to_writetime(None) is None

    def test_round_trip_conversion(self):
        """Test converting writetime to timestamp and back."""
        # Create a known timestamp
        ts_original = pd.Timestamp("2024-01-15 10:30:00", tz="UTC")
        original = int(ts_original.timestamp() * 1_000_000)

        # Convert to timestamp and back
        ts = writetime_to_timestamp(original)
        result = timestamp_to_writetime(ts)

        assert result == original

    def test_epoch_writetime(self):
        """Test epoch timestamp (0)."""
        result = writetime_to_timestamp(0)
        assert result == pd.Timestamp("1970-01-01", tz="UTC")

    def test_negative_writetime(self):
        """Test negative writetime (before epoch)."""
        # -1 second before epoch
        writetime = -1000000
        result = writetime_to_timestamp(writetime)
        assert result < pd.Timestamp("1970-01-01", tz="UTC")


class TestTTL:
    """Test TTL serialization functionality."""

    def test_to_seconds_valid(self):
        """Test converting TTL to seconds."""
        ttl = 3600  # 1 hour

        result = ttl_to_seconds(ttl)

        assert result == 3600

    def test_to_seconds_none(self):
        """Test converting None TTL."""
        assert ttl_to_seconds(None) is None

    def test_to_timedelta_valid(self):
        """Test converting TTL to timedelta."""
        ttl = 3600  # 1 hour

        result = ttl_to_timedelta(ttl)

        assert isinstance(result, pd.Timedelta)
        assert result.total_seconds() == 3600

    def test_to_timedelta_none(self):
        """Test converting None TTL to timedelta."""
        assert ttl_to_timedelta(None) is None

    def test_from_seconds_valid(self):
        """Test converting seconds to TTL."""
        seconds = 7200  # 2 hours

        result = seconds_to_ttl(seconds)

        assert result == 7200

    def test_from_seconds_zero(self):
        """Test converting zero seconds."""
        assert seconds_to_ttl(0) is None

    def test_from_seconds_negative(self):
        """Test converting negative seconds."""
        assert seconds_to_ttl(-100) is None

    def test_from_seconds_none(self):
        """Test converting None seconds."""
        assert seconds_to_ttl(None) is None

    def test_from_timedelta_valid(self):
        """Test converting timedelta to TTL."""
        delta = pd.Timedelta(hours=2, minutes=30)

        result = timedelta_to_ttl(delta)

        assert result == 9000  # 2.5 hours in seconds

    def test_from_timedelta_none(self):
        """Test converting None timedelta."""
        assert timedelta_to_ttl(None) is None

    def test_from_timedelta_negative(self):
        """Test converting negative timedelta."""
        delta = pd.Timedelta(seconds=-100)
        assert timedelta_to_ttl(delta) is None

    def test_round_trip_timedelta(self):
        """Test converting TTL to timedelta and back."""
        original = 3600

        # Convert to timedelta and back
        delta = ttl_to_timedelta(original)
        result = timedelta_to_ttl(delta)

        assert result == original

    def test_large_ttl(self):
        """Test large TTL values."""
        # 30 days in seconds
        ttl = 30 * 24 * 60 * 60

        delta = ttl_to_timedelta(ttl)
        assert delta.days == 30

        result = timedelta_to_ttl(delta)
        assert result == ttl

    def test_fractional_seconds(self):
        """Test that fractional seconds are truncated."""
        # Timedelta with microseconds
        delta = pd.Timedelta(seconds=100.5)

        result = timedelta_to_ttl(delta)

        # Should truncate to integer seconds
        assert result == 100
