# Agents working in this repository

This file is for contributors changing python-cqrs itself. Application code that depends on the package should copy [`templates/AGENTS.md`](templates/AGENTS.md) instead.

## Public API

- Export new public names from `__all__` in [`src/cqrs/__init__.py`](src/cqrs/__init__.py).
- Pydantic, SQLAlchemy, and SQLModel stay lazy through `__getattr__`. Do not import them from the package root at import time.
- Do not add optional extras to `[project].dependencies`. Extras are declared in [`pyproject.toml`](pyproject.toml) and installed explicitly.

## Language floor

Python 3.10. Do not use `class Foo[T]` or other syntax newer than 3.10. Check with the vermin command in [CONTRIBUTING.md](CONTRIBUTING.md).

## Checks

From [CONTRIBUTING.md](CONTRIBUTING.md), before a pull request:

```bash
ruff check --config ruff.toml
ruff format --check --config ruff.toml
pyright src tests examples
vermin --target=3.10- --violations --eval-annotations --backport typing_extensions --exclude=venv --exclude=build --exclude=.git --exclude=.venv src examples tests
pytest -c ./tests/pytest-config.ini ./tests/unit
```

Integration tests use `docker-compose-test.yml`. New behavior needs a unit test under `tests/unit`.

## API shape

- Commands and queries are both `cqrs.Request`. Do not add a `Query` base, a `Command` base, or a `CommandBus`.
- One handler per request type (`RequestMap.bind` raises `KeyError` on a second bind). Many handlers per event (`EventMap.bind` appends).
- A new request with a body is `Request[Result]` and `RequestHandler[ThatRequest]`. Do not write a redundant `-> Result` on `handle`. A command with an empty body is `Request[None]`. Existing `Request` and `RequestHandler[Request, Result]` stay valid; `send` on an unparameterized `Request` is `Any`.
