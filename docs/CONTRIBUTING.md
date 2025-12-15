# Contributing Guidelines for dbt-correlator

Thank you for considering contributing to dbt-correlator! This document provides guidelines to make the process as smooth as possible for everyone involved.

## Code of Conduct

We expect all contributors to adhere to a high standard of conduct, treating all participants with respect and fostering an inclusive environment.

## Reporting Bugs and Issues

If you find a bug or issue with dbt-correlator, please open an issue in the project's [issue tracker](https://github.com/correlator-io/correlator-dbt/issues). Please provide as much detail as possible, including:

- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Python version and dbt version
- Stack traces or error messages
- Sample dbt artifacts (run_results.json, manifest.json) if applicable

## Contributing Code

If you would like to contribute code to dbt-correlator, please follow these guidelines:

1. Fork the Project
2. Create your Feature [Branch](#branch-naming-convention-and-commit-message-format) (`git checkout -b major/AmazingFeature`)
3. [Commit](#commit-message-guidelines) your Changes (`git commit -m 'major: Add some AmazingFeature'`)
4. Push to the Branch (`git push origin major/AmazingFeature`)
5. Open a Pull Request

Before submitting a pull request, please ensure that your code adheres to the following guidelines:

- Follow the existing code style and formatting (Black, Ruff, etc.)
- Write clear and concise [commit messages](#commit-message-guidelines)
- Include tests for any new functionality or bug fixes
- Ensure your changes pass all pre-commit hooks
- Run `make check` to verify all checks pass (formatting, linting, type checking, tests)

By contributing to dbt-correlator, you agree to license your contributions under the terms of the Apache License 2.0.

### Code Review

All code contributions will be reviewed by a maintainer of the project. The maintainer may provide feedback or request changes to the code. Please be patient during the review process.

## Development Environment Setup

To set up the development environment:

```bash
# Clone the repository
git clone https://github.com/correlator-io/correlator-dbt.git
cd correlator-dbt

# Setup development environment (installs uv and dependencies)
make start

# Activate virtual environment
source .venv/bin/activate
```

## Quality Assurance

The project uses several code quality tools that can be run via Make commands:

```bash
# Format code with Black
make fix

# Run linting with Ruff
make run linter

# Run type checking with MyPy
make run typecheck

# Run security scanning with Bandit
make run security

# Run unit tests with pytest
make run test

# Run tests with coverage report
make run coverage

# Run all quality checks
make check
```

## Branch Naming Convention and Commit Message Format

The branch naming convention and commit message format are as follows:

- Branch naming convention: `type/branch-name`
- Commit message format: `type: commit message`

The `type` can be one of the following:

- `minor`: Minor changes or a new feature
- `major`: Major changes or breaking change
- `patch`: A bug fix
- `chore`: Maintenance tasks such as adding tests, adding documentation, updating dependencies or configuration files or bootstrap code

### Commit Message Guidelines

To maintain consistency and clarity in our project history, all commit messages should follow the format: `type: commit message`

#### Accepted Types
- **minor**: For minor changes or new features.
- **major**: For major changes or breaking changes.
- **patch**: For bug fixes.
- **chore**: For maintenance tasks, such as adding or modifying tests, updating dependencies or configuration files or bootstrap code.

#### Examples
- `minor: Add support for dbt 1.5+ artifacts`
- `major: Refactor OpenLineage event construction`
- `patch: Fix dataset namespace parsing for Snowflake`
- `test: Add integration tests for parser module`
- `chore: Update OpenLineage client to v1.2.0`

#### Why This Matters
Using a consistent format for commit messages helps:
- Easily identify the purpose and impact of each commit
- Streamline the release process by automatically generating changelogs
- Improve collaboration and understanding among team members

Make sure to follow these guidelines for every commit to keep our project history clean and meaningful!

## Testing

All new features and bug fixes should be accompanied by appropriate tests. Tests are written using pytest and should be placed in the `tests` directory.

To run tests:

```bash
# Run all tests
make run test

# Run tests with coverage report
make run coverage
```

### Test Coverage

We aim for >90% test coverage. Please ensure your changes maintain or improve the current coverage level.

## Documentation

If your changes affect the API or add new features, please update the documentation accordingly. Documentation files are located in the `docs` directory.

## Python Code Standards

The project follows strict Python coding standards:

- **Type Hints**: All function signatures and class attributes must have type hints
- **PEP 8 Compliance**: Code must follow PEP 8 with Black formatting
- **Docstrings**: Comprehensive docstrings in Google style
- **Error Handling**: Proper exception handling with custom exceptions
- **Code Quality**: Ruff linting, MyPy type checking, Bandit security scanning

## Pre-commit Hooks

The project uses pre-commit hooks to enforce code quality standards. Install them with:

```bash
# Install pre-commit hooks
make start  # This installs hooks automatically

# Or manually
uv run pre-commit install
```

The hooks will automatically run on each commit to:
- Format code with Black
- Lint with Ruff
- Check types with MyPy
- Scan for security issues with Bandit

## License

By contributing to dbt-correlator, you agree to license your contributions under the terms of the Apache License 2.0.

---

## Questions or Issues?

If you have any questions or issues, please:
- Open an issue in this repository: https://github.com/correlator-io/correlator-dbt/issues
- Join the discussion: https://github.com/correlator-io/correlator/discussions

Thank you for contributing to dbt-correlator! 🎉
