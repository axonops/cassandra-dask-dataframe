"""
Serializable connection configuration for distributed Cassandra access.

This module provides a configuration class that can be serialized and sent
to Dask workers, allowing them to create their own Cassandra connections.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConnectionConfig:
    """
    Serializable configuration for Cassandra connections.

    This class holds all the necessary parameters to create a Cassandra
    cluster connection in a distributed environment. All attributes must
    be serializable (pickle-able) for use with Dask.

    What this provides:
    -------------------
    1. Serializable connection parameters
    2. Authentication configuration
    3. SSL/TLS settings
    4. Load balancing and retry policies
    5. Protocol and consistency settings

    Why this matters:
    ----------------
    - Dask workers need to create their own connections
    - Non-serializable objects (sessions, locks) cannot cross process boundaries
    - Each worker maintains its own connection pool

    Example usage:
    -------------
    ```python
    # Create config from existing cluster
    config = ConnectionConfig(
        contact_points=['cassandra1', 'cassandra2'],
        port=9042,
        auth_provider_class='PlainTextAuthProvider',
        auth_provider_args={'username': 'user', 'password': 'pass'},
        protocol_version=5
    )

    # In worker, recreate cluster
    cluster = config.create_cluster()
    session = cluster.connect(keyspace)
    ```
    """

    # Connection endpoints
    contact_points: list[str] = field(default_factory=lambda: ["127.0.0.1"])
    port: int = 9042

    # Authentication
    auth_provider_class: str | None = (
        None  # 'PlainTextAuthProvider', 'DSEPlainTextAuthProvider', etc.
    )
    auth_provider_args: dict[str, Any] | None = None  # {'username': 'x', 'password': 'y'}

    # SSL/TLS configuration
    ssl_enabled: bool = False
    ssl_ca_certs: str | None = None  # Path to CA certs file
    ssl_verify_mode: str | None = None  # 'CERT_REQUIRED', 'CERT_OPTIONAL', 'CERT_NONE'
    ssl_check_hostname: bool = True

    # Protocol settings
    protocol_version: int | None = None
    compression: bool = True

    # Connection behavior
    connect_timeout: float = 10.0
    request_timeout: float = 10.0

    # Load balancing
    load_balancing_policy_class: str | None = (
        None  # 'DCAwareRoundRobinPolicy', 'TokenAwarePolicy', etc.
    )
    load_balancing_policy_args: dict[str, Any] | None = None

    # Retry policy
    retry_policy_class: str | None = None  # 'RetryPolicy', 'FallthroughRetryPolicy', etc.
    retry_policy_args: dict[str, Any] | None = None

    # Execution profiles (advanced)
    execution_profiles: dict[str, dict[str, Any]] | None = None

    # Additional cluster kwargs
    cluster_kwargs: dict[str, Any] = field(default_factory=dict)

    def create_cluster(self):
        """
        Create a Cassandra Cluster instance from this configuration.

        Returns:
            cassandra.cluster.Cluster: Configured cluster instance

        Note:
            This method imports cassandra modules dynamically to avoid
            serialization issues with module-level imports.
        """
        from cassandra.auth import PlainTextAuthProvider, SaslAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.policies import (
            DCAwareRoundRobinPolicy,
            FallthroughRetryPolicy,
            RetryPolicy,
            RoundRobinPolicy,
            TokenAwarePolicy,
        )

        # Build cluster kwargs
        kwargs = {
            "contact_points": self.contact_points,
            "port": self.port,
            "compression": self.compression,
            "connect_timeout": self.connect_timeout,
            "control_connection_timeout": self.connect_timeout,
        }

        # Add protocol version if specified
        if self.protocol_version is not None:
            kwargs["protocol_version"] = self.protocol_version

        # Configure authentication
        if self.auth_provider_class:
            auth_classes = {
                "PlainTextAuthProvider": PlainTextAuthProvider,
                "SaslAuthProvider": SaslAuthProvider,
            }

            auth_class = auth_classes.get(self.auth_provider_class)
            if auth_class and self.auth_provider_args:
                kwargs["auth_provider"] = auth_class(**self.auth_provider_args)

        # Configure SSL
        if self.ssl_enabled:
            import ssl

            # Create SSL context
            ssl_context = ssl.create_default_context()

            if self.ssl_ca_certs:
                ssl_context.load_verify_locations(cafile=self.ssl_ca_certs)

            if self.ssl_verify_mode:
                verify_modes = {
                    "CERT_REQUIRED": ssl.CERT_REQUIRED,
                    "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
                    "CERT_NONE": ssl.CERT_NONE,
                }
                ssl_context.verify_mode = verify_modes.get(self.ssl_verify_mode, ssl.CERT_REQUIRED)

            ssl_context.check_hostname = self.ssl_check_hostname
            kwargs["ssl_context"] = ssl_context

        # Configure load balancing policy
        if self.load_balancing_policy_class:
            policy_classes = {
                "DCAwareRoundRobinPolicy": DCAwareRoundRobinPolicy,
                "TokenAwarePolicy": TokenAwarePolicy,
                "RoundRobinPolicy": RoundRobinPolicy,
            }

            policy_class = policy_classes.get(self.load_balancing_policy_class)
            if policy_class:
                policy_args = self.load_balancing_policy_args or {}

                # Special handling for TokenAwarePolicy which wraps another policy
                if self.load_balancing_policy_class == "TokenAwarePolicy":
                    child_policy_name = policy_args.pop("child_policy", "DCAwareRoundRobinPolicy")
                    child_policy_args = policy_args.pop("child_policy_args", {})
                    child_policy_class = policy_classes.get(
                        child_policy_name, DCAwareRoundRobinPolicy
                    )
                    child_policy = child_policy_class(**child_policy_args)
                    kwargs["load_balancing_policy"] = TokenAwarePolicy(child_policy)
                else:
                    kwargs["load_balancing_policy"] = policy_class(**policy_args)

        # Configure retry policy
        if self.retry_policy_class:
            retry_classes = {
                "RetryPolicy": RetryPolicy,
                "FallthroughRetryPolicy": FallthroughRetryPolicy,
            }

            retry_class = retry_classes.get(self.retry_policy_class)
            if retry_class:
                retry_args = self.retry_policy_args or {}
                kwargs["default_retry_policy"] = retry_class(**retry_args)

        # Configure execution profiles if provided
        if self.execution_profiles:
            from cassandra import ConsistencyLevel
            from cassandra.cluster import ExecutionProfile

            profiles = {}
            for name, profile_config in self.execution_profiles.items():
                profile_kwargs = {}

                # Handle consistency level
                if "consistency_level" in profile_config:
                    cl_name = profile_config["consistency_level"]
                    profile_kwargs["consistency_level"] = getattr(ConsistencyLevel, cl_name)

                # Handle request timeout
                if "request_timeout" in profile_config:
                    profile_kwargs["request_timeout"] = profile_config["request_timeout"]

                # Handle retry policy
                if "retry_policy" in profile_config:
                    # Similar logic as above for retry policy
                    pass

                profiles[name] = ExecutionProfile(**profile_kwargs)

            kwargs["execution_profiles"] = profiles

        # Add any additional cluster kwargs
        kwargs.update(self.cluster_kwargs)

        # Check for environment variable override for contact points (useful in containerized environments)
        import os

        if os.getenv("CASSANDRA_CONTACT_POINTS"):
            # Override contact points from environment
            contact_points = os.getenv("CASSANDRA_CONTACT_POINTS").split(",")
            kwargs["contact_points"] = contact_points

        # Create and return cluster
        return Cluster(**kwargs)

    @classmethod
    def from_cluster(cls, cluster) -> "ConnectionConfig":
        """
        Create a ConnectionConfig from an existing Cluster instance.

        Note: This extracts only the serializable configuration.
        Some runtime state (like current connections) is not preserved.

        Args:
            cluster: cassandra.cluster.Cluster instance

        Returns:
            ConnectionConfig: Serializable configuration
        """
        config = cls(
            contact_points=list(cluster.contact_points),
            port=cluster.port,
            compression=cluster.compression,
            protocol_version=cluster.protocol_version,
            connect_timeout=cluster.connect_timeout,
        )

        # Extract auth provider info if possible
        if hasattr(cluster.auth_provider, "__class__"):
            auth_class_name = cluster.auth_provider.__class__.__name__
            config.auth_provider_class = auth_class_name

            # For PlainTextAuthProvider, extract credentials
            if auth_class_name == "PlainTextAuthProvider" and hasattr(
                cluster.auth_provider, "username"
            ):
                config.auth_provider_args = {
                    "username": cluster.auth_provider.username,
                    "password": cluster.auth_provider.password,
                }

        # Extract SSL info
        if cluster.ssl_context is not None:
            config.ssl_enabled = True
            # Note: We can't extract all SSL details from existing context
            # User should provide these explicitly

        # Extract load balancing policy info
        if cluster.load_balancing_policy:
            policy = cluster.load_balancing_policy
            config.load_balancing_policy_class = policy.__class__.__name__

            # Handle TokenAwarePolicy
            if hasattr(policy, "_child_policy"):
                config.load_balancing_policy_args = {
                    "child_policy": policy._child_policy.__class__.__name__,
                }

        return config
