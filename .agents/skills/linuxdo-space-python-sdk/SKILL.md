---
name: linuxdo-space-python-sdk
description: Use when writing or fixing Python code that consumes the LinuxDoSpace SDK, especially after the current PyPI install command `pip install linuxdospace`, for `from LinuxDoSpace import Client, Suffix`, mail streaming, mailbox bindings, regex matching, overlap control, routing, and exception handling. Also use when maintaining the SDK itself under `sdk/python`.
---

# LinuxDoSpace Python SDK

Read [references/consumer.md](references/consumer.md) first for normal SDK usage.
Read [references/api.md](references/api.md) before changing any public behavior.
Read [references/examples.md](references/examples.md) when you need task-shaped templates.
Read [references/development.md](references/development.md) only when editing the SDK itself.

## Workflow

1. For consumer code, prefer the public package only:
   - the current PyPI install command is `pip install linuxdospace`
   - `python -m pip install linuxdospace` is also acceptable when the environment requires an explicit interpreter
   - import only from `LinuxDoSpace`
2. For local repository work:
   - the SDK root relative to this `SKILL.md` is `../../../`
   - from the parent repository root, the SDK path is `sdk/python`
3. Prefer explicit examples with `client.mail.bind(...)`. Only use `client.mail(...)` when intentionally documenting syntax sugar.
4. Preserve these user-facing invariants:
   - one `Client` owns one upstream HTTPS stream
   - remote `base_url` must use `https://`; only localhost may use `http://`
   - `client.listen(...)` is the canonical full-intake interface
   - positive `timeout` values on both `client.listen(...)` and `mail.listen(...)` mean total wall-clock time for that iterator, not idle timeout
   - `Suffix.linuxdo_space` is semantic and resolves to `<owner_username>-mail.<default-root>`, so mailbox addresses look like `prefix@<owner_username>-mail.<default-root>` in the live distribution.
   - `Suffix.linuxdo_space.with_suffix("foo")` resolves to `<owner_username>-mailfoo.<default-root>` and yields addresses such as `prefix@<owner_username>-mailfoo.<default-root>` when suffix extensions are needed.
   - Legacy events might still surface `<owner_username>.linuxdo.space`, but that form is only kept for historical compatibility and should not be treated as the current binding target.
   - the SDK auto-syncs active dynamic `-mail<suffix>` filters to `/v1/token/email/filters`
   - exact and regex bindings share one ordered matching chain per suffix
   - `allow_overlap=False` stops at the first match; `allow_overlap=True` continues
   - `bind(...)` registers matching metadata immediately
   - mailbox queues activate only during `mail.listen(...)`; there is no pre-listen backlog
   - full-intake `MailMessage.address` is the current event projection address, while mailbox listeners receive one projection per matched recipient
   - leaving `with` or calling `close()` unbinds immediately
   - one `MailBox` allows only one active listener
   - `bind_many(...)` is transactional: partial success must roll back
   - `client.mail.route(message)` matches only `message.address` and reports current local matches, not historical queue delivery
5. If you edit the SDK itself, treat `../../../LinuxDoSpace/__init__.py` as the public export contract.
6. If behavior changes, update `../../../README.md` and `../../../tests/test_sdk.py` in the same change.
7. Validate in `../../../` after SDK changes:

```bash
python -m unittest discover -s tests -v
python -m pip install -e .
```

## Common Tasks

- Write consumer code: start with `references/consumer.md`.
- Check exact signatures and semantics: read `references/api.md`.
- Diagnose failures in consumer code: prefer `AuthenticationError`, then `StreamError`, then generic `LinuxDoSpaceError`.
- Add or change SDK API: read `references/development.md`, then update code, tests, and README together.
- Use ready-made task templates: read `references/examples.md`.

## Do Not Regress

- Do not add hidden pre-listen buffering.
- Do not split exact and regex bindings into separate priority systems.
- Do not make `route()` depend on every original recipient or imply historical delivery replay.
- Do not introduce per-mailbox upstream connections.
- Do not use local HTTP or WebSocket transport for client-side dispatch; keep local distribution in-process.
