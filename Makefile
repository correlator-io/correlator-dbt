.PHONY: start install run check fix build deploy reset help

# Variables
PACKAGE_NAME=dbt_correlator
PYTHON_VERSION=3.9
UV=uv

#===============================================================================
# INTENT-BASED COMMANDS
#===============================================================================

# Begin working (setup environment + install dependencies)
start:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🚀 Starting development environment..."; \
		$(MAKE) start-setup; \
	else \
		echo "❌ Unknown start command: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available: make start"; \
		exit 1; \
	fi

# Install/update dependencies
install:
	@if [ ! -d ".venv" ]; then \
		echo "❌ Virtual environment not found"; \
		echo "💡 Run 'make start' first to create environment"; \
		exit 1; \
	fi; \
	echo "📥 Installing/updating dependencies..."; \
	if $(UV) pip install -e ".[dev]"; then \
		echo "✅ Dependencies installed"; \
		echo ""; \
		echo "💡 Package installed in editable mode"; \
		echo "💡 Changes to source code are immediately available"; \
	else \
		echo "❌ Installation failed"; \
		echo "💡 Check the error message above for details"; \
		exit 1; \
	fi

# Execute something (run CLI by default, or run tests, linter, etc.)
run:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		$(MAKE) run-cli; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		$(MAKE) run-test; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "test" ] && [ "$(word 3,$(MAKECMDGOALS))" = "unit" ]; then \
		$(MAKE) run-test-unit; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "test" ] && [ "$(word 3,$(MAKECMDGOALS))" = "integration" ]; then \
		$(MAKE) run-test-integration; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "coverage" ]; then \
		$(MAKE) run-coverage; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "linter" ]; then \
		$(MAKE) run-linter; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "typecheck" ]; then \
		$(MAKE) run-typecheck; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "security" ]; then \
		$(MAKE) run-security; \
	else \
		echo "❌ Unknown run command: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo ""; \
		echo "📖 Available run commands:"; \
		echo "  make run                   # Run CLI (default)"; \
		echo "  make run test              # Run all tests"; \
		echo "  make run test unit         # Run unit tests only"; \
		echo "  make run test integration  # Run integration tests only"; \
		echo "  make run coverage          # Run tests with coverage report"; \
		echo "  make run linter            # Run ruff linter"; \
		echo "  make run typecheck         # Run mypy type checker"; \
		echo "  make run security          # Run bandit security scanner"; \
		exit 1; \
	fi

# Verify code quality (lint + test + type check)
check:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🔍 Running code quality checks..."; \
		$(MAKE) check-all; \
	else \
		echo "❌ Unknown check command: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available: make check"; \
		exit 1; \
	fi

# Repair issues (format + fix lints + clean artifacts)
fix:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🔧 Fixing code issues..."; \
		$(MAKE) fix-all; \
	else \
		echo "❌ Unknown fix command: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available: make fix"; \
		exit 1; \
	fi

# Create artifacts (build package)
build:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🔨 Building package..."; \
		$(MAKE) build-package; \
	else \
		echo "❌ Unknown build target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available: make build"; \
		exit 1; \
	fi

# Prepare for production (build + verify + package)
# Note: This is a LOCAL developer convenience command for manual releases.
# It runs quality checks and builds the package, then shows manual publish command.
# GitHub workflows handle automated releases - this is for manual verification.
deploy:
	@echo "🚀 Preparing deployment package..."; \
	if $(MAKE) check; then \
		echo "✅ Quality checks passed"; \
	else \
		echo "❌ Quality checks failed"; \
		echo "💡 Fix issues above before deploying"; \
		exit 1; \
	fi; \
	echo ""; \
	if $(MAKE) build; then \
		echo "✅ Deployment package ready!"; \
		echo ""; \
		echo "📦 Distribution files in dist/"; \
		ls -lh dist/; \
		echo ""; \
		echo "🚀 To publish to PyPI:"; \
		echo "  uv publish dist/*"; \
	else \
		echo "❌ Build failed"; \
		echo "💡 Cannot deploy - fix build errors first"; \
		exit 1; \
	fi

# Start fresh (clean everything + reset environment)
reset:
	@echo "🔄 Performing reset..."; \
	echo "🗑️ Cleaning build artifacts..."; \
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage htmlcov/ .mypy_cache/ .ruff_cache/; \
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true; \
	find . -type f -name "*.pyc" -delete 2>/dev/null || true; \
	echo "🧹 Removing virtual environment..."; \
	rm -rf .venv/; \
	echo ""; \
	echo "💥 Reset complete!"; \
	echo ""; \
	echo "🚀 To rebuild environment:"; \
	echo "   make start"

#===============================================================================
# INTERNAL TARGETS
#===============================================================================

# Start: Setup development environment
start-setup:
	@echo "🔍 Checking Python version..."; \
	if ! command -v python3 >/dev/null 2>&1; then \
		echo "❌ Python 3 not found. Please install Python $(PYTHON_VERSION)+"; \
		exit 1; \
	fi; \
	PYTHON_VER=$$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2); \
	echo "✅ Found Python $$PYTHON_VER"; \
	echo ""; \
	echo "🔍 Checking uv installation..."; \
	if ! command -v $(UV) >/dev/null 2>&1; then \
		echo "📦 uv not found, installing..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		echo "✅ uv installed"; \
	else \
		echo "✅ uv is available"; \
	fi; \
	echo ""; \
	echo "📦 Setting up virtual environment..."; \
	if [ -d ".venv" ]; then \
		echo "✅ Virtual environment already exists"; \
	else \
		$(UV) venv; \
		echo "✅ Virtual environment created"; \
	fi; \
	echo ""; \
	echo "📥 Installing dependencies..."; \
	if $(UV) pip install -e ".[dev]"; then \
		echo "✅ Dependencies installed"; \
	else \
		echo "❌ Dependency installation failed"; \
		echo "💡 Check the error message above for details"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🎉 Development environment ready!"; \
	echo ""; \
	echo "💡 Next steps:"; \
	echo "   source .venv/bin/activate    # Activate virtual environment"; \
	echo "   make run test                # Run tests"; \
	echo "   make check                   # Verify code quality"; \
	echo "   deactivate                   # deactivate virtual environment"

# Run: Execute tests
run-test:
	@echo "🧪 Running all tests..."; \
	if $(UV) run pytest -v; then \
		echo ""; \
		echo "✅ All tests passed"; \
	else \
		echo ""; \
		echo "❌ Tests failed"; \
		echo "💡 Review test failures above"; \
		exit 1; \
	fi

# Run: Execute unit tests only
run-test-unit:
	@echo "🧪 Running unit tests..."; \
	$(UV) run pytest -v -m unit; \
	EXIT_CODE=$$?; \
	if [ $$EXIT_CODE -eq 0 ]; then \
		echo ""; \
		echo "✅ Unit tests passed"; \
	elif [ $$EXIT_CODE -eq 5 ]; then \
		echo ""; \
		echo "⚠️ No unit tests collected (none marked with @pytest.mark.unit)"; \
		echo "💡 This is expected if tests are not yet marked"; \
	else \
		echo ""; \
		echo "❌ Unit tests failed"; \
		echo "💡 Review test failures above"; \
		exit 1; \
	fi

# Run: Execute integration tests only
run-test-integration:
	@echo "🧪 Running integration tests..."; \
	$(UV) run pytest -v -m integration; \
	EXIT_CODE=$$?; \
	if [ $$EXIT_CODE -eq 0 ]; then \
		echo ""; \
		echo "✅ Integration tests passed"; \
	elif [ $$EXIT_CODE -eq 5 ]; then \
		echo ""; \
		echo "⚠️ No integration tests collected (none marked with @pytest.mark.integration)"; \
		echo "💡 This is expected if tests are not yet marked"; \
	else \
		echo ""; \
		echo "❌ Integration tests failed"; \
		echo "💡 Review test failures above"; \
		echo "💡 Ensure Correlator is running if required"; \
		exit 1; \
	fi

# Run: Execute tests with coverage
run-coverage:
	@echo "🧪 Running tests with coverage..."; \
	if $(UV) run pytest --cov=$(PACKAGE_NAME) --cov-report=term-missing --cov-report=html -v; then \
		echo ""; \
		echo "✅ Tests passed"; \
		echo "📊 Coverage report generated in htmlcov/index.html"; \
	else \
		echo ""; \
		echo "❌ Tests failed"; \
		echo "💡 Fix failing tests before reviewing coverage"; \
		exit 1; \
	fi

# Run: Execute CLI in development mode
run-cli:
	@echo "🖥️  Running dbt-correlator CLI..."; \
	echo "💡 Usage: make run <args> (e.g., make run test --help)"; \
	echo ""; \
	$(UV) run dbt-correlator --help

# Run: Execute linter
run-linter:
	@echo "🔍 Running ruff linter..."; \
	$(UV) run ruff check .

# Run: Execute type checker
run-typecheck:
	@echo "🔍 Running mypy type checker..."; \
	$(UV) run mypy src/$(PACKAGE_NAME)

# Run: Execute security scanner
run-security:
	@echo "🔒 Running bandit security scanner..."; \
	$(UV) run bandit -c pyproject.toml -r src/$(PACKAGE_NAME)

# Check: Run all quality checks
check-all:
	@echo "🎨 Running formatter check..."; \
	if $(UV) run black --check .; then \
		echo "✅ Formatting verified"; \
	else \
		echo "❌ Formatting check failed"; \
		echo "💡 Run 'make fix' to auto-format your code"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🔍 Running linter..."; \
	if $(UV) run ruff check .; then \
		echo "✅ Linting passed"; \
	else \
		echo "❌ Linting failed"; \
		echo "💡 Run 'make fix' to auto-fix common linting issues"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🔤 Running type checker..."; \
	if $(UV) run mypy src/$(PACKAGE_NAME); then \
		echo "✅ Type checking passed"; \
	else \
		echo "❌ Type checking failed"; \
		echo "💡 Check the errors above and fix type hint issues"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🧪 Running tests..."; \
	if $(UV) run pytest -v; then \
		echo "✅ Tests passed"; \
	else \
		echo "❌ Tests failed"; \
		echo "💡 Run 'make run test' for more details"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🔒 Running security scan..."; \
	if $(UV) run bandit -c pyproject.toml -r src/$(PACKAGE_NAME) -q; then \
		echo "✅ Security scan passed"; \
	else \
		echo "❌ Security scan failed"; \
		echo "💡 Review security issues above and fix vulnerabilities"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🎉 All quality checks passed!"

# Fix: Format code and fix lints
fix-all:
	@echo "🎨 Formatting code with black..."; \
	if $(UV) run black .; then \
		echo "✅ Code formatted"; \
	else \
		echo "❌ Black formatting failed"; \
		echo "💡 Check the error message above for details"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🔧 Fixing lints with ruff..."; \
	if $(UV) run ruff check --fix .; then \
		echo "✅ Lints fixed"; \
	else \
		echo "❌ Ruff auto-fix failed"; \
		echo "💡 Some lints may require manual fixes"; \
		echo "💡 Check the errors above for details"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "📦 Sorting imports..."; \
	if $(UV) run ruff check --select I --fix .; then \
		echo "✅ Imports sorted"; \
	else \
		echo "❌ Import sorting failed"; \
		echo "💡 Check for import-related errors above"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "🎉 All code fixes applied successfully!"

# Build: Build package (always clean first for reproducibility)
build-package:
	@echo "🗑️ Cleaning old distribution files..."; \
	rm -rf dist/; \
	echo "🔨 Building package..."; \
	if $(UV) build; then \
		echo "✅ Package built successfully"; \
		echo "📦 Distribution files:"; \
		ls -lh dist/; \
	else \
		echo "❌ Package build failed"; \
		echo "💡 Check the error message above for details"; \
		echo "💡 Common issues:"; \
		echo "   - Invalid configuration in pyproject.toml"; \
		echo "   - Missing dependencies or build tools"; \
		echo "   - Syntax errors in source code"; \
		exit 1; \
	fi

#===============================================================================
# HELP
#===============================================================================

help:
	@echo "***************************************************************"
	@echo "*               🔗 dbt-correlator Development                 *"
	@echo "***************************************************************"
	@echo ""
	@echo "🚀 Getting Started:"
	@echo "    start   - Begin working (setup environment + install dependencies)"
	@echo "    install - Install/update dependencies (after changing pyproject.toml)"
	@echo "    run     - Execute CLI (run, run test, run linter)"
	@echo ""
	@echo "🛠️  Daily Development:"
	@echo "    check   - Verify code quality (lint + test + type check)"
	@echo "    fix     - Repair issues (format + fix lints)"
	@echo ""
	@echo "🏗️  Build & Deploy:"
	@echo "    build   - Build package (clean + wheel + sdist)"
	@echo "    deploy  - Verify package is ready for PyPI (local check before manual publish)"
	@echo ""
	@echo "🔧 Maintenance:"
	@echo "    reset   - Start fresh (clean everything + reset environment)"
	@echo ""
	@echo "📖 Examples:"
	@echo "    🚀 Development:"
	@echo "        make start                    # Setup development environment"
	@echo "        make install                  # Update dependencies after pyproject.toml changes"
	@echo "        make run                      # Run CLI (shows help)"
	@echo "        make run test                 # Run all tests"
	@echo "        make run test unit            # Run unit tests only"
	@echo "        make run test integration     # Run integration tests only"
	@echo "        make run coverage             # Run tests with coverage"
	@echo "        make check                    # Verify code quality"
	@echo ""
	@echo "    🔧 Code Quality:"
	@echo "        make run linter               # Run ruff linter"
	@echo "        make run typecheck            # Run mypy type checker"
	@echo "        make run security             # Run security scanner"
	@echo "        make fix                      # Auto-fix formatting and lints"
	@echo ""
	@echo "    🏗️  Build & Deploy:"
	@echo "        make build                    # Build package (clean + wheel + sdist)"
	@echo "        make deploy                   # Local verification before manual PyPI publish"
	@echo ""
	@echo "    🆘 Troubleshooting:"
	@echo "        make reset                    # Clean slate"
	@echo ""
	@echo "⚡ Quick Start:"
	@echo "    🆕 New to this project?          make start"
	@echo "    💻 Daily development?            make check"
	@echo "    🚀 Manual release?               make deploy (then: uv publish dist/*)"
	@echo ""
	@echo "💡 For detailed options: make <command> --help"

# Handle command line arguments for parameterized commands
%:
	@:
