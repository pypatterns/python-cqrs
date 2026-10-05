# Contributing to python-cqrs

Thank you for your interest in contributing to `python-cqrs`! We welcome contributions of all kinds: bug fixes, new features, documentation improvements, and performance enhancements.

The default workflow below targets the **current major** (`master` / **5.x**). For bugfixes on the **4.x** maintenance line, see [Maintaining / fixing 4.x](#8-maintaining--fixing-4x).

---

## 1. Prerequisites

- **Python Version:** Python **3.10** or higher (3.10, 3.11, 3.12, 3.13 are supported).
- **Git** installed on your system.
- *(Optional, for integration tests)*: **Docker** & **Docker Compose**. `docker-compose-test.yml` provides MySQL, PostgreSQL, and Redis. Kafka is available separately in `docker-compose-dev.yml` for local broker experiments; RabbitMQ is not defined in either Compose file.

---

## 2. Local Setup

1. **Fork and Clone the Repository:**
   ```bash
   git clone https://github.com/<your-username>/python-cqrs.git
   cd python-cqrs
   ```

2. **Create and Activate a Virtual Environment:**
   ```bash
   python -m venv .venv

   # On Linux/macOS:
   source .venv/bin/activate

   # On Windows:
   .venv\Scripts\activate
   ```

3. **Install Dependencies in Editable Mode:**
   Minimal developer install (tooling and tests):
   ```bash
   pip install --upgrade pip
   pip install -e ".[dev]"
   ```

   Optionally also install example extras (FastAPI, FastStream, uvicorn, and related packages):
   ```bash
   pip install -e ".[dev,examples]"
   ```

4. **Set Up Pre-commit Hooks:**
   ```bash
   pre-commit install
   ```

---

## 3. Code Style & Quality Checks

We use several automated tools to maintain code quality. Please run these before submitting a pull request:

### Ruff (Linting & Formatting)
- Check linting rules:
  ```bash
  ruff check --config ruff.toml
  ```
- Automatically fix linting issues:
  ```bash
  ruff check --fix --config ruff.toml
  ```
- Check code formatting:
  ```bash
  ruff format --check --config ruff.toml
  ```
- Format code:
  ```bash
  ruff format --config ruff.toml
  ```

### Pyright (Static Type Checking)
Run type analysis across the codebase:
```bash
pyright src tests examples
```

### Vermin (Minimum Python Version Verification)
Ensure no features unsupported in Python 3.10 are introduced:
```bash
vermin --target=3.10- --violations --eval-annotations --backport typing_extensions --exclude=venv --exclude=build --exclude=.git --exclude=.venv src examples tests
```

---

## 4. Running Tests

Tests are organized under the `tests/` directory and executed with `pytest`.

### Unit Tests
Run the unit suite locally (no Docker required):
```bash
pytest -c ./tests/pytest-config.ini ./tests/unit
```

### Integration Tests with Docker
Integration tests need the services defined in `docker-compose-test.yml` (MySQL, PostgreSQL, Redis):
```bash
# Start test infrastructure
docker compose -f docker-compose-test.yml up -d

# Run integration tests
pytest -c ./tests/pytest-config.ini ./tests/integration

# Stop infrastructure when finished
docker compose -f docker-compose-test.yml down
```

---

## 5. Pull Request Guidelines

### Preferred PR Scope & Size
- **Small & Focused:** Keep PRs atomic and focused on a single change, fix, or feature. Smaller PRs are easier to review, test, and merge quickly.
- **Link Related Issues:** Reference the issue your PR resolves (e.g., `Fixes #123` or `Closes #123`) in the PR description.
- **Add Tests:** If you are adding a feature or fixing a bug, include corresponding unit or integration tests.
- **Update Documentation:** If your changes affect APIs or configuration, update the relevant documentation.

### PR Checklist
Before opening a PR, ensure:
- [ ] Code follows formatting standards (`ruff format`).
- [ ] Linter checks pass without errors (`ruff check`).
- [ ] Type checks pass (`pyright`).
- [ ] Unit tests pass locally (`pytest -c ./tests/pytest-config.ini ./tests/unit`).
- [ ] Commits have clear and descriptive messages.

---

## 6. Issue Labels

We use labels to categorize and track issues:
- `good first issue`: Ideal for newcomers looking to get familiar with the codebase.
- `help wanted`: Tasks where community assistance is actively sought.
- `bug`: A problem or unintended behavior in `python-cqrs`.
- `enhancement`: New feature requests or architectural improvements.
- `documentation`: Additions or improvements to guides, docstrings, or examples.
- `4.x`: Bug or security work that should land on the **4.x** maintenance branch (see below).

---

## 7. Community & Conduct

We are committed to providing a welcoming, inclusive, and respectful environment for everyone. Please be constructive and kind in discussions, code reviews, and issue reports.

---

## 8. Maintaining / fixing 4.x

`master` is **5.x** (features and breaking changes). Branch **`4.x`** exists from **4.15.0** for **bug fixes and security patches only** — no new features. Support window and pin guidance: [README Version Support](README.md#version-support) and [SECURITY.md](SECURITY.md).

### When to target `4.x` vs `master`

| Change | Target |
|--------|--------|
| New feature, breaking change, or 5.x-only fix | `master` (default workflow above) |
| Bugfix or security patch for users still on 4.x | `4.x` (this section) |
| Same bug on both lines | Fix on `4.x` first, then forward-port to `master` |

Label the issue with `4.x` when the fix belongs on the maintenance line.

### Branch and PR workflow

1. Check out the maintenance branch and create a fix branch from it:
   ```bash
   git fetch origin
   git checkout -b fix/<short-description> origin/4.x
   ```
2. Open a pull request with **base = `4.x`** (not `master`).
3. Keep the change focused on the bug or security issue; do not add features on `4.x`.

### Version bumps and releases

- Bump only the **patch** version (for example `4.15.0` → `4.15.1`). Do not introduce `4.16` or other minor/major bumps on this line for routine fixes.
- Maintainers tag releases from the **`4.x`** branch for PyPI.

### Forward-port to 5.x

If the same bug exists on 5.x, cherry-pick or port the fix to `master` after (or alongside) the `4.x` PR. The port may need adaptation because of breaking API or package changes on 5.x.

### EOL migration warning (after 5.0.0)

After **5.0.0**, the first **4.x** patch will emit a package-level EOL `DeprecationWarning` / optional log nudging users toward 5.x. To silence the optional log in noisy CI once that release exists:

```bash
export CQRS_SUPPRESS_V4_EOL_WARNING=1
```

Details: [SECURITY.md](SECURITY.md#eol-migration-notice-after-500).

> **Note for maintainers:** Keep this section in sync on both `master` and `4.x` so contributors checking out either branch see the same guidance.
