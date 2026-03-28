# Task Templates

Use this file when you already understand the API surface and need a fast path
for common SDK work.

## 1. Write Consumer Code Against the SDK

Use this for normal application development that depends on the package.

Checklist:

1. Prefer `pip install linuxdospace`.
2. Import only from `LinuxDoSpace`.
3. Choose one of:
   - `client.listen(...)` for full-token intake
   - `client.mail.bind(...)` for explicit mailbox consumption
   - `client.mail.bind_many(...)` for ordered batch setup
4. Catch `AuthenticationError`, `StreamError`, then `LinuxDoSpaceError`.

Starter:

```python
from LinuxDoSpace import AuthenticationError, Client, LinuxDoSpaceError, StreamError, Suffix

try:
    with Client(token="...") as client:
        with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
            for item in mailbox.listen(timeout=60):
                print(item.subject)
except AuthenticationError:
    ...
except StreamError:
    ...
except LinuxDoSpaceError:
    ...
```

`Suffix.linuxdo_space` 在运行时会被解析成 `<owner_username>-mail.<default-root>`，使得绑定地址呈现 `prefix@<owner_username>-mail.<default-root>` 的形式。
`Suffix.linuxdo_space.with_suffix("foo")` 解析成 `<owner_username>-mailfoo.<default-root>`，用于生成 `prefix@<owner_username>-mailfoo.<default-root>` 之类的地址。
SDK 会自动同步所有活动的动态 `-mail<suffix>` 过滤设置到 `/v1/token/email/filters`。
旧事件或旧配置可能仍然保留 `<owner_username>.linuxdo.space` 的格式，但那仅用于历史兼容，不能作为当前主语义。

## 2. Add or Change a Public API

Use this when changing constructor args, public methods, public exports, public
dataclasses, or documented semantics.

Checklist:

1. Read `api.md`.
2. Edit the implementation under `../../../../LinuxDoSpace/`.
3. Update `../../../../LinuxDoSpace/__init__.py` if the export surface changes.
4. Add or update integration tests in `../../../../tests/test_sdk.py`.
5. Update `../../../../README.md` examples and behavior notes.
6. Run:

```bash
python -m unittest discover -s tests -v
python -m pip install -e .
```

Typical patch set:

- `../../../../LinuxDoSpace/client.py`
- `../../../../LinuxDoSpace/__init__.py`
- `../../../../tests/test_sdk.py`
- `../../../../README.md`

## 3. Fix Lifecycle or Queue Bugs

Use this for:

- `Client.close()` behavior
- mailbox bind/unbind timing
- hidden buffering
- race conditions between `bind()`, `listen()`, and `close()`

Focus files:

- `../../../../LinuxDoSpace/client.py`
- `../../../../tests/test_sdk.py`

Required assertions to preserve:

- no per-mailbox upstream connections
- no pre-listen backlog
- one mailbox has at most one active listener
- `bind_many(...)` must roll back on failure
- `route(message)` must not pretend to be historical replay

Good regression-test pattern:

```python
def test_client_close_closes_registered_mailboxes(self) -> None:
    client = Client(...)
    mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
    client.close()
    self.assertTrue(mailbox.closed)
```

## 4. Add a New `Suffix`

Use this when supporting another managed email suffix.

Checklist:

1. Update `../../../../LinuxDoSpace/enums.py`.
2. Add at least one test that uses the new suffix.
3. Update README examples or suffix documentation if user-facing.

Minimal patch shape:

```python
class Suffix(str, Enum):
    linuxdo_space = "linuxdo.space"
    example_com = "example.com"

    def __str__(self) -> str:
        return self.value
```

Do not add a suffix only in README. Keep enum, tests, and docs aligned.

## 5. Add a New Exception Type

Use this only when callers need to branch on a new error category. Do not add
exception types for internal convenience only.

Checklist:

1. Update `../../../../LinuxDoSpace/exceptions.py`.
2. Export it from `../../../../LinuxDoSpace/__init__.py`.
3. Raise it from implementation code.
4. Add tests that assert the public exception type.
5. Update README exception-handling examples if relevant.

## 6. Add a New Typed Field to `MailMessage`

Use this when exposing more parsed mail metadata.

Checklist:

1. Update `../../../../LinuxDoSpace/models.py`.
2. Update envelope construction in `../../../../LinuxDoSpace/client.py`.
3. Add integration coverage in `../../../../tests/test_sdk.py`.
4. Add the field to the README's public-attributes section.

Never add a field to the dataclass without wiring it into the parser path.

## 7. Add or Change a README Example

Prefer these examples:

### Full intake

```python
with Client(token="...") as client:
    for item in client.listen(timeout=60):
        print(item.address, item.subject)
```

### Explicit bind

```python
with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
        for item in mailbox.listen(timeout=60):
            print(item.subject)
```

### Batch binding

```python
with Client(token="...") as client:
    with client.mail.bind_many(
        client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
        client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
    ) as group:
        ...
```

Rules:

- Prefer `client.mail.bind(...)` over `client.mail(...)`.
- If documenting `route(message)`, say it matches only `message.address`.
- Do not imply hidden buffering before `listen()` starts.
- Do not call sequential consumption “parallel”.
- If documenting `timeout`, say positive values are total wall-clock time for that iterator.

## 8. Add a Regression Test for Matching Order

Use when exact/pattern ordering or `allow_overlap` changes.

Pattern:

1. Create bindings in the intended order.
2. Start listeners.
3. Wait for listener readiness explicitly.
4. Publish one message.
5. Assert exact subject distribution.

Example structure:

```python
first = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
second = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
...
self.assertEqual(client.mail.route(message), (first, second))
```

## 9. Add Consumer-Facing Example Code Outside the SDK

Use this when another module or example project needs to consume the SDK.

Guidelines:

- Import from `LinuxDoSpace`, not from internal module paths.
- Catch `AuthenticationError`, `StreamError`, then `LinuxDoSpaceError`.
- Prefer one long-lived `Client`.
- Prefer explicit mailbox binding over repeated ad hoc syntax sugar.

Starter template:

```python
from LinuxDoSpace import AuthenticationError, Client, LinuxDoSpaceError, StreamError, Suffix

try:
    with Client(token="...") as client:
        with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
            for item in mailbox.listen(timeout=60):
                print(item.subject)
except AuthenticationError:
    ...
except StreamError:
    ...
except LinuxDoSpaceError:
    ...
```

## 10. Review Checklist Before Finishing

- Did you change public behavior without updating `tests/test_sdk.py`?
- Did you update README for user-visible semantics?
- Did you preserve the single-upstream-stream model?
- Did you avoid adding hidden pre-listen buffering?
- Did you avoid documenting `route()` as historical delivery replay?
- Did you run both validation commands?
