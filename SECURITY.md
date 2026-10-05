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

A package-level `DeprecationWarning` (and optional log) that nudges users toward 5.x will be added on the **first 4.x patch release after 5.0.0** (for example `4.15.1`). It is **not** present on 4.15.0 and will not be added before 5.0.0 ships (so migration docs URLs exist first).

Suppress the optional log in noisy CI with `CQRS_SUPPRESS_V4_EOL_WARNING=1` once that release exists.

## Reporting a Vulnerability

Please report security issues privately via GitHub Security Advisories for this repository, or email the maintainers listed in `pyproject.toml`. Do not open a public issue for undisclosed vulnerabilities.
