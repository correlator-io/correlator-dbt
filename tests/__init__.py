"""Test suite for dbt-correlator.

This package contains unit tests, integration tests, and test fixtures for
the dbt-correlator plugin.

Test Organization:
    - test_parser.py: Tests for dbt artifact parsing
    - test_emitter.py: Tests for OpenLineage event construction and emission
    - test_cli.py: Tests for CLI commands
    - test_config.py: Tests for configuration management
    - test_integration.py: End-to-end integration tests with mock HTTP
    - fixtures/: Sample dbt artifacts and test data

Test Categories:
    - @pytest.mark.unit: Fast, isolated unit tests
    - @pytest.mark.integration: End-to-end tests with mock HTTP (no external deps)
    - @pytest.mark.slow: Long-running tests

Running Tests:
    $ make run test                       # All tests
    $ uv run pytest -m "not integration"  # Unit tests only
    $ uv run pytest -m integration        # Integration tests only
    $ make run coverage                   # With coverage report
"""
