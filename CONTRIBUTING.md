# Contributing to Agentics

Thank you for your interest in contributing to Agentics! This document provides guidelines and instructions for contributing to the project.

## Getting Started

### Prerequisites

- `uv` package manager ([install here](https://docs.astral.sh/uv/getting-started/installation/))
- Python 3.11 or higher (but less than 3.13)
  - If not available, uv will download it automatically.
- Git

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/IBM/agentics.git
   cd agentics
   ```

2. **Install dependencies using `uv`:**
   ```bash
   uv sync --all-groups --all-extras
   ```

   This installs all dependencies including development tools needed for testing and code quality checks.

3. **Verify your setup:**
   ```bash
   uv run pytest
   ```
   
   Also, to ensure the  [version is correctly computed from Git tags](#versioning-scheme)
   try running:
   
   ```bash
   uvx --with uv-dynamic-versioning hatchling version
   ```
   
   
## Pre-commit Hooks

We use pre-commit hooks to ensure code quality and consistency. These hooks automatically run checks before each commit.

### Installing Pre-commit Hooks

1. **Install pre-commit:**
   ```bash
   uv tool install pre-commit
   ```

2. **Set up the git hooks:**
   ```bash
   pre-commit install
   ```

### What Pre-commit Checks

Our pre-commit configuration includes the [following checks](.pre-commit-config.yaml):

- **codespell**: Checks for common spelling mistakes in Python, RST, and Markdown files
- **trailing-whitespace**: Removes trailing whitespace
- **check-ast**: Validates Python syntax
- **end-of-file-fixer**: Ensures files end with a newline
- **debug-statements**: Detects leftover debugging code
- **isort**: Sorts and organizes Python imports
- **black**: Formats Python code to consistent style
- **nbstripout**: Removes outputs from Jupyter notebooks

### Running Pre-commit Manually

If you want to run pre-commit checks manually without committing:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run a specific hook
pre-commit run codespell --all-files
pre-commit run black --all-files
```

### Skipping Pre-commit (Not Recommended)

If you absolutely need to skip pre-commit checks for a commit:

```bash
git commit --no-verify
```

However, this is not recommended as it may cause CI/CD pipeline failures.

## Running Tests

We use pytest for testing. All contributions should include tests for new functionality.

### Running All Tests

```bash
uv run pytest tests/
```

### Running Tests with Verbose Output

```bash
uv run pytest tests/ -v
```

### Running Specific Tests

```bash
# Run a specific test file
pytest tests/test_specific_module.py

# Run a specific test function
pytest tests/test_module.py::test_function_name

# Run tests matching a pattern (can be a file, function or package)
pytest tests/ -k "pattern"
```

### Running Tests in Parallel

```bash
pytest tests/ -n auto
```

This uses all available CPU cores to run tests faster.

### Pytest Fixtures

Pytest uses fixtures to initialize state of tests or provide some dependencies [`tests/conftest.py`](tests/conftest.py).

You can see the list of available fixtures with `uv run pytest --fixtures`.

### Generating an HTML Test Report

```bash
pytest tests/ --html=report.html --self-contained-html
```

The test report will be saved as `report.html` in the project root for later analysis or sharing.


### Running Tests with Coverage

**Code coverage** measures the percentage of your codebase that is exercised by tests. 
It's an important metric that helps you understand how thoroughly your how much code 
is not exercised by your tests.


```bash
pytest tests/ --cov=src/agentics --cov-report=html
```

This command generates a detailed coverage report in HTML format in the `htmlcov/` directory. You can open `htmlcov/index.html` in your browser to see which lines are covered and which are not.

## Code Style

This project follows specific code style guidelines:

- **Formatting**: Black (automatic via pre-commit)
- **Import ordering**: isort (automatic via pre-commit)
- **Spell checking**: codespell (automatic via pre-commit)

Code style checks are enforced automatically by pre-commit hooks, so ensure you've installed them properly.

## Making Changes

1. **Create a new branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and ensure they pass tests:
   ```bash
   pytest tests/ -v
   ```

3. **Run pre-commit checks:**
   ```bash
   pre-commit run --all-files
   ```

4. **Commit your changes** with a descriptive message:
   ```bash
   git commit -m "Add description of changes"
   ```

5. **Push to your fork and create a Pull Request**

6. **When your PR is merged to `main`**, one of the maintainers will include a release keyword in the merge commit:
   - Merge the PR and include `[release major]`, `[release minor]`, or `[release patch]` in the commit message
   - This ensures tests run and the package is published to PyPI with a new version
   - If the change doesn't include the change a new release can be created from Github
     to create a new version in PyPI.

## Publishing new releases to PyPI


1. **Create a new release in Github**
   Make sure the release has the format v{MAJOR}.{MINOR}.{PATCH} so the
   github actions picks this change and publishes to PyPI.

### Versioning Scheme

This project uses Semantic Versioning (SemVer):
- **MAJOR** (X.0.0): Breaking changes
- **MINOR** (0.X.0): New features (backward compatible)
- **PATCH** (0.0.X): Bug fixes (backward compatible)

**Technical Details:**
- The version is dynamically determined from git tags using `uv-dynamic-versioning`
- Tags follow the format `v{major}.{minor}.{patch}` (e.g., `v1.0.0`)
- All automated releases pass the test suite before being published
- Releases are built as Python wheels and published in PyPI automatically.

## Pull Request Guidelines

- Provide a clear description of what your PR does
- Include tests for new functionality
- Ensure all tests pass locally before submitting
- Keep commits focused and logical
- Update documentation if needed

## Questions?

If you have questions or need help, please open an issue on GitHub or contact the maintainers.

Thank you for contributing to Agentics!
