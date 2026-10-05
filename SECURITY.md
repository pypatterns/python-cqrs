# Security Policy

## Supported Versions

| Branch | PyPI           | Status                                      |
|--------|----------------|---------------------------------------------|
| `master` / 5.x | `5.x.y` | Active development (features + breaking)    |
| `4.x`          | `4.15.x`, … | Bug fixes and security only                 |

After **python-cqrs 5.0.0** is released, the **4.x** line remains supported for approximately **12 months** for bug fixes and security patches. New features land only on 5.x. How to contribute fixes on that line: [CONTRIBUTING.md — Maintaining / fixing 4.x](CONTRIBUTING.md#8-maintaining--fixing-4x).

Clients who are not ready to migrate should pin:

```bash
pip install "python-cqrs>=4,<5"
```

## EOL migration notice (after 5.0.0)

Starting with **4.15.1**, importing `cqrs` emits a package-level `DeprecationWarning` (and an optional log) that nudges users toward 5.x. It is **not** present on 4.15.0.

Suppress the optional log in noisy CI with `CQRS_SUPPRESS_V4_EOL_WARNING=1` (`1` / `true` / `yes`). The `DeprecationWarning` is still emitted and can be filtered with standard Python warning filters.

## Reporting a Vulnerability

Please report security issues privately via GitHub Security Advisories for this repository, or email the maintainers listed in `pyproject.toml`. Do not open a public issue for undisclosed vulnerabilities.
