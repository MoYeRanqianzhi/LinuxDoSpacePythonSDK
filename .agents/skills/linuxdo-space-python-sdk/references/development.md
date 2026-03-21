# Development Guide

Use this file for change workflow, validation, and update boundaries.

## Workdir

Run SDK commands in the Python SDK root:

```bash
cd sdk/python
```

From this `references/development.md` file, that same SDK root is `../../../../`.

## Required Validation

Run both commands after SDK code changes:

```bash
python -m unittest discover -s tests -v
python -m pip install -e .
```

What they validate:

- integration-style behavior in `tests/test_sdk.py`
- packaging and editable install correctness from `pyproject.toml`

## PyPI Release

GitHub workflow file:

- `.github/workflows/pypi-release.yml`

Release trigger:

- push tag `v<package-version>`
- example for current version `0.3.0a3`: `v0.3.0a3`

Workflow behavior:

- reads version from `pyproject.toml`
- rejects tag pushes whose tag does not match that version
- builds both sdist and wheel
- runs `twine check`
- publishes through PyPI Trusted Publishing

PyPI Trusted Publishing fields to configure on PyPI:

- owner: `MoYeRanqianzhi`
- repository name: `LinuxDoSpacePythonSDK`
- workflow name: `pypi-release.yml`
- environment name: `pypi`
- project name: `LinuxDoSpace`

## Files to Update Together

When public behavior changes, keep these files aligned:

- `../../../../LinuxDoSpace/__init__.py`
  - public exports
- `../../../../LinuxDoSpace/client.py`
  - implementation and lifecycle semantics
- `../../../../LinuxDoSpace/enums.py`
  - public enum surface
- `../../../../LinuxDoSpace/exceptions.py`
  - public error surface
- `../../../../LinuxDoSpace/models.py`
  - public typed models
- `../../../../README.md`
  - package usage and documented semantics
- `../../../../tests/test_sdk.py`
  - integration coverage for the changed behavior

## Preferred Documentation Style

- Prefer explicit examples using `client.mail.bind(...)`.
- Mention `client.mail(...)` only when documenting syntax sugar.
- Document behavior that actually exists in code and tests; do not document aspirational behavior.
- If lifecycle semantics change, update README examples and the invariants in `SKILL.md` together.

## Preferred Change Strategy

1. Read `references/api.md`.
2. Inspect the current implementation in `../../../../LinuxDoSpace/client.py`.
3. Add or update tests first when the change affects observable behavior.
4. Implement the code change.
5. Update `../../../../README.md`.
6. Run both validation commands.

## High-Risk Areas

- stream lifecycle and reconnection
- `Client.close()` and mailbox shutdown ordering
- mailbox matching order and `allow_overlap`
- `bind_many(...)` rollback semantics
- queue activation timing around `mail.listen(...)`
- `route(message)` semantics
- public import surface in `__init__.py`

## Non-Negotiable Invariants

- one `Client` == one upstream stream
- no per-mailbox upstream connections
- no hidden pre-listen mailbox buffering
- no separate exact-vs-regex priority ladder
- no remote non-HTTPS backend support
- no local HTTP/WebSocket dispatch layer for mailbox routing
