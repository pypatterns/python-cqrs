"""DI / scoped-dependency examples.

| File | When to open |
|------|----------------|
| `di_basic.py` | Minimal `di` container + request handler |
| `dependency_injector_simple.py` | `DependencyInjectorCQRSContainer` basics |
| `dependency_injector_practical.py` | FastAPI + dependency-injector (needs examples extra) |
| `scoped_dependencies_di.py` | Generator UoW with `di` (issue #70) |
| `scoped_dependencies_dishka.py` | dishka adapter (`python-cqrs[dishka]`) |
| `scoped_dependencies_dependency_injector.py` | Why dependency-injector has no SupportsScope |
| `scoped_dependencies_custom_container.py` | Custom `SupportsScope` template |
| `scoped_dependencies_fastapi.py` | `bind_scope` + SEND; CLI demo works without FastAPI |

Docs: https://mkdocs.python-cqrs.dev/scoped_dependencies/
"""
