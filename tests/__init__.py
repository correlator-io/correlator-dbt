"""Test suite for dbt-correlator.

This package contains unit tests, integration tests, and test fixtures for
the dbt-correlator plugin.

Test Organization:
    - test_parser.py: Tests for dbt artifact parsing
    - test_emitter.py: Tests for OpenLineage event construction and emission
    - test_cli.py: Tests for CLI commands (Task 1.4)
    - test_config.py: Tests for configuration management (Task 1.5)
    - fixtures/: Sample dbt artifacts and test data

Test Categories:
    - @pytest.mark.unit: Fast, isolated unit tests
    - @pytest.mark.integration: Integration tests (requires Correlator)
    - @pytest.mark.slow: Long-running tests

Running Tests:
    $ make run test              # All tests
    $ make run test unit         # Unit tests only
    $ make run test integration  # Integration tests only
    $ make run coverage          # With coverage report
"""
