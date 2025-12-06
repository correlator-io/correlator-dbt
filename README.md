# 🔗 dbt-correlator

**Accelerate dbt incident resolution with automated correlation**

[![PyPI version](https://badge.fury.io/py/correlator-dbt.svg)](https://badge.fury.io/py/correlator-dbt)
[![Python Version](https://img.shields.io/pypi/pyversions/correlator-dbt.svg)](https://pypi.org/project/correlator-dbt/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

---

## What It Does

Eliminates manual detective work when dbt tests fail:

- Automatically links test failures to the job runs that caused them
- Provides direct navigation from incident to root cause
- Eliminates context switching across multiple tools
- Works with your existing OpenLineage infrastructure

---

## Why It Matters

**The Problem:**
When dbt tests fail, teams spend significant time manually hunting through logs, lineage graphs, and job histories to
find the root cause.

**What You Get:**
Stop the manual detective work. `dbt-correlator` automatically connects your dbt test failures to their root cause,
putting you in control of incidents instead of reacting to them.

**Key Benefits:**

- **Faster incident resolution**: Automated correlation reduces time spent investigating
- **Eliminate tool switching**: One correlation view instead of navigating multiple dashboards
- **Instant root cause**: Direct path from test failure to problematic job run
- **Zero-friction setup**: Add one command after `dbt test`

**Built on Standards:**
Uses OpenLineage, the industry standard for data lineage. No vendor lock-in, no proprietary formats.

---

## Versioning

This package follows [Semantic Versioning](https://semver.org/) with the following guidelines:

- **0.x.y versions** (e.g., 0.1.0, 0.2.0) indicate **initial development phase**:
    - The API is not yet stable and may change between minor versions
    - Features may be added, modified, or removed without major version changes
    - Not recommended for production-critical systems without pinned versions

- **1.0.0 and above** will indicate a **stable API** with semantic versioning guarantees:
    - MAJOR version for incompatible API changes
    - MINOR version for backwards-compatible functionality additions
    - PATCH version for backwards-compatible bug fixes

The current version is in early development stage, so expect possible API changes until the 1.0.0 release.

---

## Documentation

**For detailed usage, configuration, and development:**

- **Development**: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) - Development setup, testing, local environment, pro tips
- **Contributing**: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contribution guidelines, branch naming, commit format
- **Deployment**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) - Release process, versioning, PyPI publishing

---

## Requirements

- Python 3.9+
- dbt-core 1.0+
- [Correlator](https://github.com/correlator-io/correlator)

---

## Links

- **Correlator**: https://github.com/correlator-io/correlator
- **OpenLineage**: https://openlineage.io/
- **Issues**: https://github.com/correlator-io/correlator-dbt/issues
- **Discussions**: https://github.com/correlator-io/correlator/discussions

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
