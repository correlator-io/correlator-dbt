# Deployment Guide

## PyPI Release Process

The `dbt-correlator` project uses semantic versioning and automated PyPI releases through GitHub Actions.

### How It Works

1. The version is automatically updated based on the branch prefix when a PR is merged:
    - **MAJOR** version: Use `major/` branch prefix (e.g., `major/redesign-api`)
    - **MINOR** version: Use `minor/` branch prefix (e.g., `minor/add-dataset-filtering`)
    - **PATCH** version: Use `patch/` branch prefix (e.g., `patch/fix-parser-error`)
    - No version change: Use `chore/` prefix (e.g., `chore/update-dependencies`, `chore/add-tests`)

2. When a PR is merged to `main`, the automated workflow:
    - Determines the next version based on the branch prefix
    - Updates version in `pyproject.toml`
    - Commits the version change
    - Creates a new git tag (e.g., `v0.1.1`)
    - Runs full test suite to ensure quality
    - Builds the package distribution (wheel + sdist)
    - Publishes to PyPI
    - Creates a GitHub release with the new version and changelog

### Requirements

- A PyPI API token must be stored in GitHub repository secrets as `PYPI_API_TOKEN`
- The workflow runs on Python 3.9+ (tests against 3.9, 3.10, 3.11, 3.12, 3.13)
- All tests must pass before deployment
- Code coverage must meet the 90% threshold

### Branch Naming Convention

Follow these branch prefixes for proper versioning (as defined in [CONTRIBUTING.md](CONTRIBUTING.md)):

| Prefix   | Version Bump          | Example                         | Use Case                                              |
|----------|-----------------------|---------------------------------|-------------------------------------------------------|
| `major/` | Major (1.0.0 → 2.0.0) | `major/breaking-api-changes`    | Breaking changes, API redesign                        |
| `minor/` | Minor (0.1.0 → 0.2.0) | `minor/add-config-file-support` | New features, enhancements                            |
| `patch/` | Patch (0.1.0 → 0.1.1) | `patch/fix-manifest-parsing`    | Bug fixes                                             |
| `chore/` | No change             | `chore/update-dependencies`     | Maintenance tasks (tests, docs, config, dependencies) |

### Manual Release

Automated versioning is the primary method, but if needed, you can manually trigger a release:

1. **Create a PR** from a branch with the appropriate prefix:
   ```bash
   # For a new feature (minor version bump)
   git checkout -b minor/add-great-expectations-support
   
   # Make changes, commit with proper format
   git add .
   git commit -m "minor: Add Great Expectations integration"
   git push origin minor/add-great-expectations-support
   ```

2. **Get the PR approved and merged** to `main`

3. **Automated workflow handles the rest:**
    - Version bumped automatically
    - Tests run
    - Package published to PyPI
    - GitHub release created

### Test Releases to TestPyPI

For testing releases before publishing to production PyPI:

1. Use the `testpypi.yml` workflow via manual trigger in GitHub Actions
2. Or publish manually:
   ```bash
   # Build the package
   uv build
   
   # Publish to TestPyPI
   uv publish --repository testpypi
   ```

3. Test installation from TestPyPI:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ correlator-dbt
   ```

### Pre-release Checklist

Before merging a PR that will trigger a release:

- [ ] All tests pass locally (`make run test`)
- [ ] Code coverage meets 90% threshold (`make run coverage`)
- [ ] Type checking passes (`make run typecheck`)
- [ ] Linting passes (`make run linter`)
- [ ] Code formatting passes (`make fix`)
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Integration tests pass with Correlator running

### Post-release Verification

After a release is published:

1. **Verify PyPI publication:**
    - Check https://pypi.org/project/correlator-dbt/
    - Verify version number is correct
    - Check package metadata

2. **Test installation:**
   ```bash
   # Fresh virtual environment
   python -m venv test-env
   source test-env/bin/activate
   
   # Install from PyPI
   pip install correlator-dbt
   
   # Verify CLI works
   dbt-correlator --version
   dbt-correlator --help
   ```

3. **Update documentation:**
    - Update installation instructions if needed
    - Update version references in docs

### Rollback Procedure

If a release has critical issues:

1. **Yank the release from PyPI** (doesn't delete, but marks as broken):
   ```bash
   pip install twine
   twine upload --skip-existing --repository pypi dist/*
   ```

2. **Create a patch fix:**
   ```bash
   git checkout -b patch/fix-critical-issue
   # Fix the issue
   git commit -m "patch: Fix critical issue in version X.Y.Z"
   # Create PR and merge (triggers new patch release)
   ```

3. **Notify users:**
    - Update GitHub release notes with warning
    - Post in Discussions if applicable
    - Update documentation

### Release Notes

Release notes are automatically generated from PR titles and commit messages. For better release notes:

- Use clear, descriptive PR titles
- Follow conventional commit format when possible
- Include breaking changes in PR description
- Tag PRs with appropriate labels

### Versioning Strategy

**Current phase: Alpha (0.x.x)**

- Breaking changes allowed in minor versions
- Rapid iteration and feature development

**Future phase: Beta (0.9.x+)**

- Feature freeze for 1.0
- Focus on stability and bug fixes
- Breaking changes require major version bump

**Future phase: Stable (1.x.x+)**

- Semantic versioning strictly followed
- Breaking changes only in major versions
- Maintain backward compatibility in minor/patch versions

---

## Manual Deployment (Emergency)

If GitHub Actions is unavailable, you can deploy manually:

```bash
# Ensure you're on main branch and up to date
git checkout main
git pull origin main

# Update version in pyproject.toml manually
# Edit: version = "0.1.X"

# Build the package
uv build

# Verify the build
ls dist/

# Publish to PyPI
uv publish

# Tag the release
git tag v0.1.X
git push origin v0.1.X

# Create GitHub release manually
# Go to: https://github.com/correlator-io/correlator-dbt/releases/new
```

---

## Distribution Channels

**Primary:**

- PyPI: https://pypi.org/project/correlator-dbt/

**Future:**

- Conda-forge (community support)
- Docker Hub (containerized CLI)
- Homebrew tap (macOS users)

---

## Security Considerations

- PyPI API token stored as GitHub secret (never in code)
- Package signing with GPG key (future)
- Automated security scanning via `bandit` in CI
- Dependency vulnerability scanning via Dependabot
- Two-factor authentication required for PyPI maintainers

---

**For questions about deployment, see [CONTRIBUTING.md](CONTRIBUTING.md) or open a Discussion.**

