# API Reference

This file is the complete public SDK reference for `sdk/python`.

Install/import naming:

- Current PyPI install command: `pip install linuxdospace`
- PyPI install name: `linuxdospace`
- Import package name: `LinuxDoSpace`

## Paths

- Package root: `../../../../LinuxDoSpace`
- Public exports: `../../../../LinuxDoSpace/__init__.py`
- Package README: `../../../../README.md`
- Integration tests: `../../../../tests/test_sdk.py`
- Packaging metadata: `../../../../pyproject.toml`

## Public Imports

```python
from LinuxDoSpace import (
    AuthenticationError,
    Client,
    LinuxDoSpaceError,
    MailBindingGroup,
    MailBindingSpec,
    MailBox,
    MailMessage,
    StreamError,
    Suffix,
)
```

## Runtime Architecture

- One `Client` owns exactly one upstream HTTPS stream.
- The upstream stream is opened immediately during `Client(...)` construction.
- The backend only knows about the API token, not local mailbox bindings.
- Full-token intake happens through `client.listen(...)`.
- Local mailbox filtering happens in-process inside the Python client.

## `Client`

### Constructor

```python
client = Client(
    token="lds_pat....",
    base_url="https://api.linuxdo.space",
    connect_timeout=10.0,
    stream_socket_timeout=30.0,
)
```

Parameters:

- `token: str`
- `base_url: str = "https://api.linuxdo.space"`
- `connect_timeout: float = 10.0`
- `stream_socket_timeout: float = 30.0`

Behavior:

- Empty `token` raises `ValueError`.
- Non-positive timeouts raise `ValueError`.
- Remote `base_url` must use `https://`.
- `http://` is allowed only for `localhost`, `127.0.0.1`, `::1`, and `*.localhost`.
- Construction waits for the initial stream connection attempt and may raise `StreamError` or `AuthenticationError`.

### Properties

- `client.connected -> bool`
  - `True` only while the shared stream is alive and the client is not closed.

### Context Management

```python
with Client(token="...") as client:
    ...
```

- Leaving the context calls `client.close()`.

### Methods

#### `client.listen(timeout: float = -1) -> Iterator[MailMessage]`

- Canonical full-intake API.
- Yields every mail event exposed by the token stream.
- `timeout < 0` means unbounded total listen time.
- Positive `timeout` means total wall-clock time for this iterator.
- Each yielded `MailMessage` is one projection of the current upstream mail
  event. In the full-intake path, `message.address` is the primary projection
  address for that event, while `message.recipients` preserves the full
  recipient tuple.

#### `client.close() -> None`

- Closes the upstream stream.
- Closes registered mailbox bindings.
- Stops full listeners and active mailbox listeners.
- Idempotent.

#### `client.catch_all(pattern=r".*", suffix=..., allow_overlap=False) -> MailBox`

- Convenience wrapper around `client.mail.catch_all(...)`.
- Preferred style in new code is still `client.mail.bind(pattern=...)` or `client.mail.catch_all(...)`.

### `client.mail`

`client.mail` is a callable facade object, not a plain method.

Preferred usage:

```python
mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
```

Syntax sugar:

```python
mailbox = client.mail(prefix="alice", suffix=Suffix.linuxdo_space)
```

## `MailBindingFacade`

Accessible through `client.mail`.

### `bind(...) -> MailBox`

```python
mailbox = client.mail.bind(
    prefix="alice",                # XOR with pattern
    pattern=None,                  # XOR with prefix
    suffix=Suffix.linuxdo_space,
    allow_overlap=False,
)
```

Rules:

- Exactly one of `prefix` or `pattern` must be provided.
- `prefix` is normalized to lowercase and trimmed.
- `pattern` accepts `str` or compiled `re.Pattern[str]`.
- Regex matching uses `fullmatch()`, not `search()`.
- `suffix` accepts either `Suffix` or `str`.
- Matching metadata is registered immediately at bind time.
- Local message buffering does not start until `mailbox.listen(...)`.

### `__call__(...) -> MailBox`

- Same as `bind(...)`.
- Use only when intentionally exposing syntax sugar.

### `catch_all(pattern=r".*", suffix=..., allow_overlap=False) -> MailBox`

```python
mailbox = client.mail.catch_all(suffix=Suffix.linuxdo_space)
```

- Regex helper for catch-all style bindings.

### `unbind(*targets) -> None`

```python
client.mail.unbind(mailbox)
client.mail.unbind(group)
client.mail.unbind(mailbox_a, mailbox_b)
```

- Accepts `MailBox` or `MailBindingGroup`.
- Calls `close()` on each target.

### `spec(...) -> MailBindingSpec`

```python
spec = client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space)
```

- Creates a declarative binding spec for later batch registration.
- Does not register anything by itself.

### `bind_many(*specs) -> MailBindingGroup`

```python
group = client.mail.bind_many(
    client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
    client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
)
```

Semantics:

- Preserves caller order.
- Transactional: if any spec is invalid, the whole batch rolls back.
- Returns `MailBindingGroup`.

### `route(message: MailMessage) -> tuple[MailBox, ...]`

```python
matches = client.mail.route(message)
```

Semantics:

- Uses only `message.address`.
- Returns the current ordered local matches for that address.
- Does not replay past delivery.
- Does not expand across every original SMTP recipient.
- Does not enqueue or consume messages.

## Matching Semantics

All bindings for the same suffix share one ordered chain.

Rules:

- Exact and regex bindings share the same chain.
- Creation order is the only priority rule.
- On match:
  - the binding receives the message
  - if `allow_overlap=False`, scanning stops
  - if `allow_overlap=True`, scanning continues

Example:

```python
client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
```

- `alice@<owner_username>.linuxdo.space` matches both bindings, in that order.

## `MailBox`

Created by `client.mail.bind(...)`, `client.mail(...)`, or `client.mail.catch_all(...)`.

### Public Attributes

- `mode: str`
  - `"exact"` or `"pattern"`
- `suffix: str`
- `allow_overlap: bool`
- `prefix: str | None`
- `pattern: str | None`
- `address: str | None`
  - concrete address for exact bindings
- `closed: bool`

### Context Management

```python
with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
    ...
```

- Leaving the context unregisters immediately.

### Methods

#### `mailbox.listen(timeout: float = -1) -> Iterator[MailMessage]`

- Activates the mailbox's local queue for the duration of the listen call.
- Messages sent before `listen()` starts are not backfilled.
- One `MailBox` supports only one active listener at a time.
- If a second concurrent listener starts, `LinuxDoSpaceError` is raised.
- `timeout < 0` means unbounded total listen time for this iterator.
- Positive `timeout` means total wall-clock time for this iterator.
- `timeout=0` returns immediately.
- Each yielded `MailMessage` is one matched-recipient projection, so mailbox
  listeners can see a different `message.address` projection from the
  corresponding full-intake event.

#### `mailbox.close() -> None`

- Unregisters the binding immediately.
- Stops the active listener if one exists.
- Idempotent.

## `MailBindingSpec`

```python
spec = MailBindingSpec(
    suffix=Suffix.linuxdo_space,
    prefix="alice",
    pattern=None,
    allow_overlap=False,
)
```

Fields:

- `suffix: Suffix | str`
- `prefix: str | None`
- `pattern: str | re.Pattern[str] | None`
- `allow_overlap: bool = False`

Use with `client.mail.bind_many(...)`.

## `MailBindingGroup`

Returned by `client.mail.bind_many(...)`.

Supports:

- iteration
- `len(group)`
- `group[index]`
- `group.closed`
- `group.close()`
- context manager usage

## `MailMessage`

Every received message is parsed into `MailMessage`.

Fields:

- `address: str`
- `sender: str`
- `recipients: tuple[str, ...]`
- `received_at: datetime`
- `subject: str`
- `message_id: str | None`
- `date: datetime | None`
- `from_header: str`
- `to_header: str`
- `cc_header: str`
- `reply_to_header: str`
- `from_addresses: tuple[str, ...]`
- `to_addresses: tuple[str, ...]`
- `cc_addresses: tuple[str, ...]`
- `reply_to_addresses: tuple[str, ...]`
- `text: str`
- `html: str`
- `headers: Mapping[str, str]`
- `raw: str`
- `raw_bytes: bytes`
- `message: EmailMessage`

Address interpretation depends on the intake path:

- In `client.listen(...)`, `address` is the primary projection address for the
  current upstream event.
- In `mailbox.listen(...)`, `address` is the matched recipient projection that
  activated that mailbox delivery.

## `Suffix`

Current enum members:

```python
Suffix.linuxdo_space
```

It is a semantic first-party suffix enum.

- `str(Suffix.linuxdo_space) == "linuxdo.space"` remains true
- mailbox binding resolution expands it to `<owner_username>.linuxdo.space`
- plain `str` suffix inputs still stay literal

## Exceptions

- `LinuxDoSpaceError`
  - base SDK error
- `AuthenticationError`
  - token rejected by backend
- `StreamError`
  - stream connection, timeout, or payload parsing failure

## Canonical Usage Patterns

### Full Intake

```python
with Client(token="...") as client:
    for item in client.listen(timeout=60):
        print(item.address, item.subject)
```

### Exact Mailbox Binding

```python
with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
        for item in mailbox.listen(timeout=60):
            print(item.subject)
```

### Regex Binding

```python
with Client(token="...") as client:
    with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
        for item in mailbox.listen(timeout=60):
            print(item.address)
```

### Batch Binding

```python
with Client(token="...") as client:
    with client.mail.bind_many(
        client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
        client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
    ) as group:
        catch_all = group[0]
        alice = group[1]
```

### Local Read-Only Routing

```python
with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
        for item in client.listen(timeout=60):
            matches = client.mail.route(item)
            print(matches)
```

Interpretation:

- `route(item)` shows what currently matches `item.address`.
- It is not a queue replay API.
