---
name: linuxdo-space-python-sdk
description: Use when implementing, fixing, refactoring, testing, or documenting the LinuxDoSpace Python SDK under `sdk/python`, or when writing code that consumes `from LinuxDoSpace import Client, Suffix`. Covers the SDK API surface, mailbox binding lifecycle, routing semantics, stream behavior, validation commands, and the required docs/tests update points.
---

# LinuxDoSpace Python SDK

Read [references/api.md](references/api.md) before changing any public behavior.
Read [references/development.md](references/development.md) before editing code, tests, or README.
Read [references/examples.md](references/examples.md) when you need task-shaped templates for common SDK changes.

## Workflow

1. Work in `../../../` unless the task explicitly targets this skill itself.
2. Treat `../../../LinuxDoSpace/__init__.py` as the public export contract.
3. Treat `../../../README.md` and `../../../tests/test_sdk.py` as part of the public API. If behavior changes, update both in the same change.
4. Prefer explicit examples with `client.mail.bind(...)`. Only use `client.mail(...)` when intentionally documenting syntax sugar.
5. Preserve these invariants:
   - one `Client` owns one upstream HTTPS stream
   - remote `base_url` must use `https://`; only localhost may use `http://`
   - `client.listen(...)` is the canonical full-intake interface
   - exact and regex bindings share one ordered matching chain per suffix
   - `allow_overlap=False` stops at the first match; `allow_overlap=True` continues
   - `bind(...)` registers matching metadata immediately
   - mailbox queues activate only during `mail.listen(...)`; there is no pre-listen backlog
   - leaving `with` or calling `close()` unbinds immediately
   - one `MailBox` allows only one active listener
   - `bind_many(...)` is transactional: partial success must roll back
   - `client.mail.route(message)` matches only `message.address` and reports current local matches, not historical queue delivery
6. If you add a public API, implement code, tests, and README changes together.
7. Validate in `../../../` after SDK changes:

```bash
python -m unittest discover -s tests -v
python -m pip install -e .
```

## Common Tasks

- Add or change API: read `references/api.md`, edit `../../../LinuxDoSpace/*.py`, update `../../../README.md`, update `../../../tests/test_sdk.py`, run both validation commands.
- Fix lifecycle, queue, or ordering bugs: inspect `../../../LinuxDoSpace/client.py` and the integration tests first; do not infer semantics from README alone.
- Add examples: keep them aligned with the preferred explicit API and current lifecycle semantics.
- Use ready-made task templates: read `references/examples.md`.
- Diagnose failures: prefer `AuthenticationError`, then `StreamError`, then generic `LinuxDoSpaceError` handling.

## Do Not Regress

- Do not add hidden pre-listen buffering.
- Do not split exact and regex bindings into separate priority systems.
- Do not make `route()` depend on every original recipient or imply historical delivery replay.
- Do not introduce per-mailbox upstream connections.
- Do not use local HTTP or WebSocket transport for client-side dispatch; keep local distribution in-process.
