"""Configuration management for dbt-correlator.

This module handles configuration from config files and provides path helpers.
Configuration priority order (handled by Click in cli.py):
    1. CLI arguments (highest priority)
    2. Environment variables
    3. Config file (.dbt-correlator.yml)
    4. Default values (lowest priority)

See docs/CONFIGURATION.md for detailed documentation.
"""

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

# Default config file names (searched in order)
DEFAULT_CONFIG_FILENAMES = (".dbt-correlator.yml", ".dbt-correlator.yaml")

# Mapping from YAML nested keys to CorrelatorConfig field names
CONFIG_FIELD_MAPPING: dict[tuple[str, str], str] = {
    ("correlator", "endpoint"): "correlator_endpoint",
    ("correlator", "namespace"): "openlineage_namespace",
    ("correlator", "api_key"): "correlator_api_key",
    ("dbt", "project_dir"): "dbt_project_dir",
    ("dbt", "profiles_dir"): "dbt_profiles_dir",
    ("job", "name"): "job_name",
}

# Mapping from CorrelatorConfig field names to CLI option names
# Used by cli.py to set Click's default_map from config file values
# Single source of truth - avoid duplication between config.py and cli.py
CONFIG_TO_CLI_MAPPING: dict[str, str] = {
    "correlator_endpoint": "correlator_endpoint",
    "openlineage_namespace": "openlineage_namespace",
    "correlator_api_key": "correlator_api_key",
    "dbt_project_dir": "project_dir",
    "dbt_profiles_dir": "profiles_dir",
    "job_name": "job_name",
}


def _interpolate_env_vars(value: str) -> str:
    """Expand ${VAR_NAME} patterns in string values.

    Replaces patterns like ${VAR_NAME} with the corresponding environment
    variable value. If the environment variable is not set, replaces with
    an empty string.

    Args:
        value: String potentially containing ${VAR} patterns.

    Returns:
        String with environment variables expanded.

    Example:
        >>> os.environ["API_KEY"] = "secret"
        >>> _interpolate_env_vars("key: ${API_KEY}")
        "key: secret"
    """
    # Pattern to match ${VAR_NAME}
    pattern = r"\$\{([^}]+)\}"

    def replace_env_var(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, "")

    return re.sub(pattern, replace_env_var, value)


def _interpolate_dict_values(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively interpolate environment variables in dict values.

    Args:
        data: Dictionary with potentially nested string values.

    Returns:
        Dictionary with all string values interpolated.
    """
    result: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = _interpolate_env_vars(value)
        elif isinstance(value, dict):
            result[key] = _interpolate_dict_values(value)
        else:
            result[key] = value
    return result


def load_yaml_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    """Load configuration from YAML file.

    Searches for configuration file in the following order:
    1. Explicit path if provided
    2. .dbt-correlator.yml or .dbt-correlator.yaml in current working directory
    3. .dbt-correlator.yml or .dbt-correlator.yaml in user home directory

    Environment variables in the format ${VAR_NAME} are expanded.

    Args:
        config_path: Optional explicit path to config file.
                    If None, searches default locations.

    Returns:
        Dictionary of configuration values (empty dict if no file found).

    Raises:
        ValueError: If file exists but contains invalid YAML.

    Example:
        >>> config = load_yaml_config(Path(".dbt-correlator.yml"))
        >>> config["correlator"]["endpoint"]
        "http://localhost:8080"
    """
    # Determine which path to use
    if config_path is not None:
        paths_to_try = [config_path]
    else:
        # Search for both .yml and .yaml extensions in cwd and home
        paths_to_try = []
        for filename in DEFAULT_CONFIG_FILENAMES:
            paths_to_try.append(Path.cwd() / filename)
        for filename in DEFAULT_CONFIG_FILENAMES:
            paths_to_try.append(Path.home() / filename)

    # Find first existing config file
    found_path: Optional[Path] = None
    for path in paths_to_try:
        if path.exists():
            found_path = path
            break

    # No config file found
    if found_path is None:
        return {}

    # Read and parse YAML
    try:
        content = found_path.read_text(encoding="utf-8")
        data = yaml.safe_load(content)

        # Handle empty file or file with only comments
        if data is None:
            return {}

        # Interpolate environment variables in all string values
        return _interpolate_dict_values(data)

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file {found_path}: {e}") from e


def flatten_config(nested: dict[str, Any]) -> dict[str, Any]:
    """Flatten nested YAML structure to flat dict with CorrelatorConfig field names.

    Converts nested YAML config structure to flat dictionary with keys
    matching CorrelatorConfig attribute names.

    Args:
        nested: Nested dict from YAML (e.g., {"correlator": {"endpoint": "..."}})

    Returns:
        Flat dict with field names matching CorrelatorConfig attributes.

    Example:
        >>> flatten_config({"correlator": {"endpoint": "http://..."}})
        {"correlator_endpoint": "http://..."}
    """
    result: dict[str, Any] = {}

    for (section, key), field_name in CONFIG_FIELD_MAPPING.items():
        if (
            section in nested
            and isinstance(nested[section], dict)
            and key in nested[section]
        ):
            result[field_name] = nested[section][key]

    return result


def get_run_results_path(project_dir: str) -> Path:
    """Get path to dbt run_results.json file.

    Args:
        project_dir: Path to dbt project directory.

    Returns:
        Path to run_results.json in target directory.

    Example:
        >>> get_run_results_path(".")
        PosixPath('target/run_results.json')
    """
    return Path(project_dir) / "target" / "run_results.json"


def get_manifest_path(project_dir: str) -> Path:
    """Get path to dbt manifest.json file.

    Args:
        project_dir: Path to dbt project directory.

    Returns:
        Path to manifest.json in target directory.

    Example:
        >>> get_manifest_path(".")
        PosixPath('target/manifest.json')
    """
    return Path(project_dir) / "target" / "manifest.json"
