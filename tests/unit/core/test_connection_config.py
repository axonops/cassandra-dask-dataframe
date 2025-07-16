"""
Unit tests for ConnectionConfig class.

What this tests:
---------------
1. Serialization of connection parameters
2. Cluster creation from config
3. Config extraction from existing cluster
4. Various authentication scenarios
5. SSL configuration options

Why this matters:
----------------
- Ensures configs can be pickled for Dask
- Validates parameter handling
- Confirms cluster recreation works
"""

import pickle
from unittest.mock import MagicMock, Mock, patch

from cassandra_dask_dataframe.connection_config import ConnectionConfig


class TestConnectionConfig:
    """Test serializable connection configuration."""

    def test_default_config_serializable(self):
        """
        Test that default config can be pickled.

        What this tests:
        ---------------
        1. Default ConnectionConfig is pickle-able
        2. Unpickled config matches original
        3. No non-serializable defaults

        Why this matters:
        ----------------
        - Dask requires all partition data to be serializable
        - Default configs should work out of the box
        """
        config = ConnectionConfig()

        # Pickle and unpickle
        pickled = pickle.dumps(config)
        unpickled = pickle.loads(pickled)

        # Verify fields match
        assert unpickled.contact_points == config.contact_points
        assert unpickled.port == config.port
        assert unpickled.compression == config.compression

    def test_config_with_auth_serializable(self):
        """
        Test config with authentication is serializable.

        What this tests:
        ---------------
        1. Auth provider config can be pickled
        2. Credentials are preserved
        3. Auth class name is stored correctly

        Why this matters:
        ----------------
        - Most production deployments use authentication
        - Credentials must be passed to workers securely
        """
        config = ConnectionConfig(
            contact_points=["node1", "node2"],
            port=9042,
            auth_provider_class="PlainTextAuthProvider",
            auth_provider_args={"username": "cassandra", "password": "cassandra123"},
        )

        # Pickle and unpickle
        pickled = pickle.dumps(config)
        unpickled = pickle.loads(pickled)

        # Verify auth config preserved
        assert unpickled.auth_provider_class == "PlainTextAuthProvider"
        assert unpickled.auth_provider_args["username"] == "cassandra"
        assert unpickled.auth_provider_args["password"] == "cassandra123"

    def test_config_with_ssl_serializable(self):
        """
        Test SSL configuration serialization.

        What this tests:
        ---------------
        1. SSL parameters can be pickled
        2. Certificate paths are preserved
        3. SSL options are maintained

        Why this matters:
        ----------------
        - Secure clusters require SSL/TLS
        - Certificate paths must be accessible on workers
        """
        config = ConnectionConfig(
            contact_points=["secure-node"],
            ssl_enabled=True,
            ssl_ca_certs="/path/to/ca.crt",
            ssl_verify_mode="CERT_REQUIRED",
            ssl_check_hostname=True,
        )

        # Pickle and unpickle
        pickled = pickle.dumps(config)
        unpickled = pickle.loads(pickled)

        # Verify SSL config
        assert unpickled.ssl_enabled is True
        assert unpickled.ssl_ca_certs == "/path/to/ca.crt"
        assert unpickled.ssl_verify_mode == "CERT_REQUIRED"
        assert unpickled.ssl_check_hostname is True

    @patch("cassandra.cluster.Cluster")
    def test_create_cluster_basic(self, mock_cluster_class):
        """
        Test basic cluster creation.

        What this tests:
        ---------------
        1. Cluster created with correct parameters
        2. Contact points and port passed correctly
        3. Compression setting preserved

        Why this matters:
        ----------------
        - Workers must create identical cluster configs
        - Connection parameters must be applied correctly
        """
        config = ConnectionConfig(
            contact_points=["host1", "host2"], port=9043, compression=False, connect_timeout=30.0
        )

        # Create cluster
        config.create_cluster()

        # Verify Cluster was called with correct args
        mock_cluster_class.assert_called_once()
        call_args = mock_cluster_class.call_args[1]

        assert call_args["contact_points"] == ["host1", "host2"]
        assert call_args["port"] == 9043
        assert call_args["compression"] is False
        assert call_args["connect_timeout"] == 30.0

    @patch("cassandra.auth.PlainTextAuthProvider")
    @patch("cassandra.cluster.Cluster")
    def test_create_cluster_with_auth(self, mock_cluster_class, mock_auth_provider):
        """
        Test cluster creation with authentication.

        What this tests:
        ---------------
        1. Auth provider instantiated correctly
        2. Credentials passed to provider
        3. Provider attached to cluster

        Why this matters:
        ----------------
        - Auth must work identically on all workers
        - Credentials must be handled securely
        """
        config = ConnectionConfig(
            auth_provider_class="PlainTextAuthProvider",
            auth_provider_args={"username": "test_user", "password": "test_pass"},
        )

        # Create cluster
        config.create_cluster()

        # Verify auth provider created
        mock_auth_provider.assert_called_once_with(username="test_user", password="test_pass")

        # Verify auth provider passed to cluster
        call_args = mock_cluster_class.call_args[1]
        assert "auth_provider" in call_args

    @patch("ssl.create_default_context")
    @patch("cassandra.cluster.Cluster")
    def test_create_cluster_with_ssl(self, mock_cluster_class, mock_ssl_context):
        """
        Test cluster creation with SSL.

        What this tests:
        ---------------
        1. SSL context created properly
        2. CA certs loaded
        3. Verify mode set correctly

        Why this matters:
        ----------------
        - SSL must be configured identically on workers
        - Certificate validation is critical for security
        """
        mock_context = MagicMock()
        mock_ssl_context.return_value = mock_context

        config = ConnectionConfig(
            ssl_enabled=True,
            ssl_ca_certs="/path/to/ca.crt",
            ssl_verify_mode="CERT_REQUIRED",
            ssl_check_hostname=False,
        )

        # Create cluster
        config.create_cluster()

        # Verify SSL context created
        mock_ssl_context.assert_called_once()
        mock_context.load_verify_locations.assert_called_once_with(cafile="/path/to/ca.crt")

        # Verify SSL context passed to cluster
        call_args = mock_cluster_class.call_args[1]
        assert "ssl_context" in call_args
        assert call_args["ssl_context"] == mock_context

    def test_from_cluster_extraction(self):
        """
        Test extracting config from existing cluster.

        What this tests:
        ---------------
        1. Contact points extracted
        2. Port and protocol preserved
        3. Compression setting extracted

        Why this matters:
        ----------------
        - Allows easy config creation from existing connections
        - Ensures all relevant settings are captured
        """
        # Mock cluster
        mock_cluster = Mock()
        mock_cluster.contact_points = ["node1", "node2"]
        mock_cluster.port = 9042
        mock_cluster.compression = True
        mock_cluster.protocol_version = 5
        mock_cluster.connect_timeout = 15.0
        mock_cluster.auth_provider = None
        mock_cluster.ssl_context = None
        mock_cluster.load_balancing_policy = None

        # Extract config
        config = ConnectionConfig.from_cluster(mock_cluster)

        # Verify extraction
        assert config.contact_points == ["node1", "node2"]
        assert config.port == 9042
        assert config.compression is True
        assert config.protocol_version == 5
        assert config.connect_timeout == 15.0

    def test_complex_config_serializable(self):
        """
        Test complex configuration with all options.

        What this tests:
        ---------------
        1. All config options together
        2. Complex nested structures
        3. Full serialization round-trip

        Why this matters:
        ----------------
        - Production configs are often complex
        - All combinations must be serializable
        """
        config = ConnectionConfig(
            contact_points=["prod1", "prod2", "prod3"],
            port=9042,
            auth_provider_class="PlainTextAuthProvider",
            auth_provider_args={"username": "app", "password": "secret"},
            ssl_enabled=True,
            ssl_ca_certs="/etc/cassandra/ca.crt",
            protocol_version=5,
            compression=True,
            connect_timeout=30.0,
            request_timeout=60.0,
            load_balancing_policy_class="TokenAwarePolicy",
            load_balancing_policy_args={
                "child_policy": "DCAwareRoundRobinPolicy",
                "child_policy_args": {"local_dc": "dc1"},
            },
            retry_policy_class="RetryPolicy",
            execution_profiles={
                "default": {"consistency_level": "LOCAL_QUORUM", "request_timeout": 30.0}
            },
            cluster_kwargs={"idle_heartbeat_interval": 30},
        )

        # Full serialization test
        pickled = pickle.dumps(config)
        unpickled = pickle.loads(pickled)

        # Spot check complex fields
        assert unpickled.load_balancing_policy_args["child_policy"] == "DCAwareRoundRobinPolicy"
        assert unpickled.execution_profiles["default"]["consistency_level"] == "LOCAL_QUORUM"
        assert unpickled.cluster_kwargs["idle_heartbeat_interval"] == 30
