# Consumer Guide

Read this first when the task is to use the SDK in application code instead of
modifying the SDK itself.

## Install

Current PyPI install command:

```bash
pip install linuxdospace
```

Equivalent explicit-interpreter form:

```bash
python -m pip install linuxdospace
```

Local repository install:

```bash
cd sdk/python
python -m pip install -e .
```

## Public Import Rule

Import only from the public package surface:

```python
from LinuxDoSpace import (
    AuthenticationError,
    Client,
    LinuxDoSpaceError,
    StreamError,
    Suffix,
)
```

Do not import from internal modules unless the task is specifically to maintain
the SDK itself.

## Consumer Mental Model

- One `Client` opens one upstream HTTPS stream immediately.
- `client.listen(...)` gives full-token intake.
- `client.mail.bind(...)` creates local mailbox matching rules.
- `Suffix.linuxdo_space` resolves to `<owner_username>.linuxdo.space`.
- A mailbox starts receiving only while `mail.listen(...)` is active.
- Positive `timeout` values on both `client.listen(...)` and `mail.listen(...)` mean total wall-clock time, not idle timeout.
- `client.mail.route(message)` is a read-only helper for the current
  `message.address`; it is not queue history replay.
- `client.listen(...)` yields one projected `MailMessage` per upstream event,
  while `mail.listen(...)` yields one projected `MailMessage` per matched
  recipient address.

## Preferred Usage Patterns

### Full Intake

```python
from LinuxDoSpace import Client

with Client(token="...") as client:
    for item in client.listen(timeout=60):
        print(item.address, item.subject)
```

### Exact Mailbox Binding

```python
from LinuxDoSpace import Client, Suffix

with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
        for item in mailbox.listen(timeout=60):
            print(item.subject)
```

### Regex Mailbox Binding

```python
from LinuxDoSpace import Client, Suffix

with Client(token="...") as client:
    with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
        for item in mailbox.listen(timeout=60):
            print(item.address)
```

### Batch Registration

```python
from LinuxDoSpace import Client, Suffix

with Client(token="...") as client:
    with client.mail.bind_many(
        client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
        client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
    ) as group:
        catch_all = group[0]
        alice = group[1]
```

### Read-Only Local Routing

```python
from LinuxDoSpace import Client, Suffix

with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
        for item in client.listen(timeout=60):
            matches = client.mail.route(item)
            print(item.address, matches)
```

### Multiple Mailboxes On One Client

If more than one mailbox binding must stay active, keep them active at the same
time and route from the full stream. Do not treat sequential mailbox listeners
as parallel consumption.

```python
from LinuxDoSpace import Client, Suffix

with Client(token="...") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as alice:
        with client.mail.bind(prefix="bob", suffix=Suffix.linuxdo_space) as bob:
            for item in client.listen(timeout=60):
                for mailbox in client.mail.route(item):
                    if mailbox is alice:
                        print("alice", item.subject)
                    elif mailbox is bob:
                        print("bob", item.subject)
```

## Matching Rules

- `prefix` and `pattern` are mutually exclusive.
- Exact and regex bindings share one ordered chain for the same suffix.
- First matching binding always receives.
- `allow_overlap=False` stops there.
- `allow_overlap=True` allows later bindings to receive too.

## Exception Handling

```python
from LinuxDoSpace import AuthenticationError, Client, LinuxDoSpaceError, StreamError

try:
    with Client(token="...") as client:
        for item in client.listen(timeout=60):
            print(item.subject)
except AuthenticationError:
    print("Token invalid or rejected.")
except StreamError:
    print("Stream connection or parsing failed.")
except LinuxDoSpaceError as exc:
    print(f"SDK failure: {exc}")
```

## Consumer Do / Do Not

Do:

- keep one long-lived `Client` when possible
- prefer `client.mail.bind(...)` for explicit mailbox setup
- catch public SDK exceptions

Do not:

- open a new `Client` per mailbox
- assume `bind(...)` backfills mail before `listen()`
- assume sequential `alice.listen(...)` / `bob.listen(...)` calls keep both queues active
- treat `route(message)` as message replay
- rely on internal modules in normal application code
