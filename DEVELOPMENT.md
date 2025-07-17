# 🏗️ Development Guide

This guide will help you set up your development environment and contribute to cassandra-dask-dataframe.

## 📋 Prerequisites

Before you begin, ensure you have:

- 🐍 **Python 3.9+** (We recommend using pyenv or similar)
- 🐳 **Docker** or **Podman** (for running Cassandra and Dask clusters)
- 📦 **Git** (obviously!)
- ⚡ **Make** (for running development commands)

## 🚀 Quick Setup

### 1. Clone and Enter the Repository

```bash
git clone https://github.com/yourusername/cassandra-dask-dataframe.git
cd cassandra-dask-dataframe
```

### 2. Create Virtual Environment

```bash
# Using venv (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Or using pyenv-virtualenv
pyenv virtualenv 3.12 cassandra-dask-df
pyenv activate cassandra-dask-df
```

### 3. Install Development Dependencies

```bash
# Install in development mode with all extras
pip install -e ".[dev,test]"

# Install pre-commit hooks
pre-commit install
```

### 4. Verify Installation

```bash
# Run basic checks
make lint
make test-unit
```

## 🧪 Testing

### 📊 Test Organization

```
tests/
├── unit/                    # Fast, isolated unit tests
│   ├── core/               # Core functionality
│   ├── data_handling/      # Type conversion, serialization
│   ├── execution/          # Streaming, incremental building
│   └── partitioning/       # Token ranges, strategies
├── integration/            # Tests requiring Cassandra
│   ├── core/              # Metadata extraction
│   ├── data_types/        # All Cassandra types
│   ├── filtering/         # Predicate pushdown
│   ├── partitioning/      # Token range handling
│   ├── reading/           # End-to-end reads
│   └── resilience/        # Error scenarios
└── cluster_dask_integration/  # Distributed Dask tests
```

### 🏃 Running Tests

```bash
# Run all tests (requires Cassandra container)
make test

# Run specific test suites
make test-unit          # Unit tests only (fast, no Cassandra)
make test-integration   # Integration tests (requires Cassandra)
make test-distributed   # Distributed tests (requires Dask cluster)

# Run with coverage
make test-coverage

# Run specific test file
pytest tests/unit/core/test_metadata.py -v

# Run with specific marker
pytest -m "not distributed" -v  # Skip distributed tests
```

### 🐳 Container Management

The Makefile handles container lifecycle automatically, but you can control it manually:

```bash
# Start Cassandra container
make cassandra-start

# Stop Cassandra container
make cassandra-stop

# Start full test environment (Cassandra + Dask cluster)
docker-compose -f tests/cluster_dask_integration/docker-compose.yml up -d

# View container logs
docker logs async-cassandra-test -f
```

### ⚙️ Test Configuration

Key environment variables:

```bash
# Cassandra connection
export CASSANDRA_HOST=localhost
export CASSANDRA_PORT=9042

# Dask scheduler (for distributed tests)
export DASK_SCHEDULER=tcp://localhost:8786

# Container runtime (if using podman)
export CONTAINER_RUNTIME=podman
```

## 💻 Development Workflow

### 1. 📝 Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. 🧪 Write Tests First (TDD)

**CRITICAL**: Always write tests before implementation!

```python
# tests/unit/test_your_feature.py
def test_new_functionality():
    """
    Test description.

    What this tests:
    ---------------
    1. Specific behavior
    2. Edge cases
    3. Error conditions

    Why this matters:
    ----------------
    - Real-world use case
    - Prevents regression
    """
    # Test implementation
    assert False  # Should fail initially
```

### 3. 🔨 Implement Feature

```python
# src/cassandra_dask_dataframe/your_module.py
def your_new_function():
    """Implement to make test pass."""
    pass
```

### 4. 🎯 Run Tests Locally

```bash
# Run your new test
pytest tests/unit/test_your_feature.py -v

# Run related tests
pytest tests/unit/core/ -v

# Run all tests to ensure no regression
make test
```

### 5. 🎨 Format and Lint

```bash
# Auto-format code
make format

# Run all linters
make lint

# Or run individually
black src tests
isort src tests
ruff check src tests
mypy src
```

### 6. 📚 Update Documentation

- Update docstrings with examples
- Add to README.md if it's a user-facing feature
- Update ARCHITECTURE.md if it changes internals

### 7. 💾 Commit with Meaningful Message

```bash
git add .
git commit -m "feat: Add support for custom partitioning strategies

- Implement TokenRangeGrouper for intelligent range grouping
- Add tests for wraparound token ranges
- Update documentation with examples"
```

## 🔧 Debugging Tips

### 🐛 Debug Integration Tests

```python
# Add to your test to see Cassandra queries
import logging
logging.basicConfig(level=logging.DEBUG)

# Print DataFrame info
print(df.compute())
print(df.dtypes)
print(df.memory_usage())
```

### 📊 Inspect Token Ranges

```python
from cassandra_dask_dataframe.token_ranges import discover_token_ranges

# In an async test
ranges = await discover_token_ranges(session, 'keyspace')
for r in ranges:
    print(f"Range: {r.start} to {r.end}, replicas: {r.replicas}")
```

### 🔍 Debug Distributed Execution

```python
# Enable Dask diagnostics
from dask.distributed import Client

client = Client('localhost:8786')
print(client.dashboard_link)  # View in browser

# Check worker logs
docker logs dask-worker-1 -f
```

## 📏 Code Standards

### 🎯 General Guidelines

- **Type Hints**: Always use type hints
- **Docstrings**: Google style with examples
- **Error Messages**: Be specific and helpful
- **Logging**: Use appropriate log levels
- **Comments**: Explain "why", not "what"

### 🧪 Test Standards

```python
"""
Test module description.

Tests the specific functionality and edge cases.
"""

import pytest
from cassandra_dask_dataframe import SomeClass


class TestSomeClass:
    """Test SomeClass functionality."""

    @pytest.mark.asyncio
    async def test_critical_behavior(self, session):
        """
        Test critical behavior with detailed description.

        What this tests:
        ---------------
        1. Main functionality
        2. Edge case handling
        3. Error conditions

        Why this matters:
        ----------------
        - Prevents data loss
        - Ensures correctness
        - Common use case
        """
        # Arrange
        instance = SomeClass(session)

        # Act
        result = await instance.do_something()

        # Assert
        assert result is not None
        assert result.property == expected_value
```

### 🏗️ Code Structure

```python
"""Module docstring explaining purpose."""

from __future__ import annotations

import logging
from typing import Any, Optional

from .base import BaseClass

logger = logging.getLogger(__name__)


class YourClass(BaseClass):
    """
    Class description.

    This class handles X by doing Y, which is important because Z.

    Attributes:
        attribute1: Description of attribute1
        attribute2: Description of attribute2

    Example:
        >>> instance = YourClass(param1="value")
        >>> result = await instance.method()
        >>> print(result)
    """

    def __init__(self, param1: str, param2: Optional[int] = None) -> None:
        """
        Initialize YourClass.

        Args:
            param1: Description of param1
            param2: Description of param2 (default: None)
        """
        self.param1 = param1
        self.param2 = param2 or self.DEFAULT_VALUE
```

## 🚨 Common Pitfalls

### ❌ Don't Do This

```python
# Don't skip tests
@pytest.mark.skip("Broken test")  # NO! Fix it instead

# Don't use print for debugging in production code
print(f"Debug: {value}")  # Use logging instead

# Don't catch broad exceptions
try:
    risky_operation()
except Exception:  # Too broad!
    pass

# Don't modify global state in tests
some_global_var = "modified"  # Will affect other tests
```

### ✅ Do This Instead

```python
# Fix failing tests
def test_feature():
    """Test passes after fixing implementation."""
    assert feature_works()

# Use proper logging
logger.debug(f"Processing value: {value}")

# Catch specific exceptions
try:
    risky_operation()
except ValueError as e:
    logger.error(f"Invalid value: {e}")
    raise

# Use fixtures for test state
@pytest.fixture
def test_data():
    """Provide test data."""
    return {"key": "value"}
```

## 🔄 CI/CD Pipeline

Our CI pipeline runs on every PR:

1. **Linting**: All code must pass black, isort, ruff, mypy
2. **Unit Tests**: Must pass with adequate coverage
3. **Integration Tests**: Must pass (uses container)
4. **Distributed Tests**: Must pass (uses Dask cluster)

### 🎯 Pre-commit Hooks

Pre-commit runs automatically before commits:

```yaml
# .pre-commit-config.yaml includes:
- trailing-whitespace
- end-of-file-fixer
- check-yaml
- check-added-large-files
- black
- isort
- ruff
- mypy
```

To run manually:
```bash
pre-commit run --all-files
```

## 🐞 Known Issues

### 🔍 Debug Print Statements

There are currently debug print statements in:
- `partition_reader.py`
- `partition.py`
- `incremental_builder.py`

These will be removed before the 1.0 release.

### 🐳 Container Issues

If you see "address already in use":
```bash
# Find and stop existing container
docker ps | grep 9042
docker stop <container_id>

# Or use our helper
make cassandra-stop
```

## 🤝 Submitting Changes

1. **Ensure all tests pass**: `make test`
2. **Ensure linting passes**: `make lint`
3. **Update documentation** if needed
4. **Push your branch**: `git push origin feature/your-feature`
5. **Create a Pull Request** with:
   - Clear description of changes
   - Link to any related issues
   - Test results summary

## 📞 Getting Help

- 💬 Open an issue for bugs or questions
- 📖 Read the architecture docs for deep dives
- 🤔 Check existing issues/PRs for similar work

---

<div align="center">
Happy coding! 🚀
</div>
