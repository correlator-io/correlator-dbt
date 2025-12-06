"""Configuration management for dbt-correlator.

This module handles configuration from environment variables, config files,
and CLI arguments with proper priority order:
    1. CLI arguments (highest priority)
    2. Environment variables
    3. Config file (.dbt-correlator.yml)
    4. Default values (lowest priority)

Uses Pydantic Settings for type-safe configuration with automatic
environment variable loading.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CorrelatorConfig(BaseSettings):
    """Configuration for dbt-correlator.

    All settings can be provided via:
        - Environment variables (OPENLINEAGE_URL, OPENLINEAGE_NAMESPACE, etc.)
        - Config file (.dbt-correlator.yml) - Implementation in Task 1.5
        - CLI arguments (override all others)

    Attributes:
        openlineage_url: OpenLineage API endpoint URL (required).
            Example: http://localhost:8080/api/v1/lineage
        openlineage_namespace: Namespace for OpenLineage events (default: "dbt").
            Used to group related jobs/datasets.
        openlineage_api_key: Optional API key for authentication.
        dbt_project_dir: dbt project directory (default: ".").
        dbt_profiles_dir: dbt profiles directory (default: "~/.dbt").
        job_name: Job name for OpenLineage events (default: "dbt_test_run").
    """

    model_config = SettingsConfigDict(
        env_prefix="",  # No prefix - use exact names like OPENLINEAGE_URL
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields
    )

    # OpenLineage configuration
    openlineage_url: str = Field(
        ...,  # Required field
        description="OpenLineage API endpoint URL",
        examples=["http://localhost:8080/api/v1/lineage"],
    )

    openlineage_namespace: str = Field(
        default="dbt",
        description="Namespace for OpenLineage events",
        examples=["dbt", "production", "staging"],
    )

    openlineage_api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for authentication",
    )

    # dbt configuration
    dbt_project_dir: str = Field(
        default=".",
        description="Path to dbt project directory",
    )

    dbt_profiles_dir: str = Field(
        default="~/.dbt",
        description="Path to dbt profiles directory",
    )

    # Job configuration
    job_name: str = Field(
        default="dbt_test_run",
        description="Job name for OpenLineage events",
    )

    def get_run_results_path(self) -> Path:
        """Get path to dbt run_results.json file.

        Returns:
            Path to run_results.json in target directory.
        """
        return Path(self.dbt_project_dir) / "target" / "run_results.json"

    def get_manifest_path(self) -> Path:
        """Get path to dbt manifest.json file.

        Returns:
            Path to manifest.json in target directory.
        """
        return Path(self.dbt_project_dir) / "target" / "manifest.json"

    def validate_paths(self) -> None:
        """Validate that required dbt artifacts exist.

        Raises:
            FileNotFoundError: If run_results.json or manifest.json not found.

        Note:
            This is a helper for validation. Implementation in Task 1.5.
        """
        run_results = self.get_run_results_path()
        manifest = self.get_manifest_path()

        if not run_results.exists():
            raise FileNotFoundError(
                f"run_results.json not found at {run_results}. "
                f"Please run 'dbt test' first."
            )

        if not manifest.exists():
            raise FileNotFoundError(
                f"manifest.json not found at {manifest}. "
                f"Please run 'dbt compile' or 'dbt test' first."
            )
