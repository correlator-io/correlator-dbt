<!--
Please comply with the contributing guidelines and best practices of this repository.
Pull Request titles must follow the format: type: description
Allowed types: minor, major, patch, chore

Examples:
- minor: Add support for dbt 1.8+ artifacts
- patch: Fix dataset namespace parsing for Snowflake
- major: Redesign OpenLineage event structure (breaking change)
- chore: Update dependencies and pre-commit hooks
-->

## Description
<!-- Provide a detailed description of the changes in this PR -->

## Issue Link
<!-- Link to the related issue (if applicable) -->

## Type of Change
<!-- Mark the appropriate option with an 'x' -->
- [ ] Bug fix (non-breaking change which fixes an issue) - **patch**
- [ ] New feature (non-breaking change which adds functionality) - **minor**
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected) - **major**
- [ ] Documentation update - **chore**
- [ ] Code refactoring - **chore**
- [ ] CI/CD or workflow changes - **chore**
- [ ] Dependency updates - **chore**
- [ ] Other (please describe):

## Quality Assurance Checklist
<!-- Mark completed items with an 'x' -->
- [ ] I have followed the branch naming convention (`type/description`)
- [ ] I have followed commit message format (`type: message`)
- [ ] I have reviewed my own code before requesting review
- [ ] All quality checks pass locally (`make check`)
- [ ] I have added/updated tests for the changes
- [ ] All tests pass locally (`make run test`)
- [ ] Code coverage meets 90% threshold (`make run coverage`)
- [ ] OpenLineage event structure is valid (for emitter changes)
- [ ] Documentation updated where necessary (README.md, docstrings, etc.)
- [ ] I have tested the CLI with sample dbt artifacts (for parser/emitter changes)

## Additional Notes
<!-- Any additional information that reviewers should know -->
