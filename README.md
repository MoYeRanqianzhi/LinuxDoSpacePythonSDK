# LinuxDoSpace Python SDK

`LinuxDoSpace` 是 LinuxDoSpace 邮件 HTTPS 实时流的 Python SDK。

它当前专注在一个目标能力：

- 使用站点后台签发的 API Token
- 通过 `HTTPS` 长连接实时接收邮件事件
- 把原始邮件解析成带完整属性提示的 Python 对象

## 安装

当前 PyPI 安装命令是：

```bash
pip install linuxdospace
```

如果你更习惯显式调用当前 Python 解释器，也可以使用：

```bash
python -m pip install linuxdospace
```

如果你在当前仓库里本地开发 SDK，再使用可编辑安装：

```bash
python -m pip install -e .
```

## 快速开始

关于 `Suffix.linuxdo_space`：

- 它是语义后缀，不是字面父域名
- SDK 会在 `ready.owner_username` 到达后，把它解析成 `<owner_username>.linuxdo.space`
- 如果你需要字面自定义后缀，请直接传普通字符串

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    for item in client.listen(timeout=60):
        print(item.address)
        print(item.sender)
        print(item.subject)
        print(item.text)
```

更推荐使用显式注册接口：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    mail = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
    try:
        for item in mail.listen(timeout=60):
            print(item.address)
            print(item.sender)
            print(item.subject)
            print(item.text)
    finally:
        mail.close()
```

如果你更喜欢 `with`，它只是上面显式注册写法的语法糖，并且会在离开作用域时自动解绑：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mail:
        for item in mail.listen(timeout=60):
            print(item.address)
            print(item.sender)
            print(item.subject)
            print(item.text)
```

如果你要一次性注册多条绑定，可以显式批量注册：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind_many(
        client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
        client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
        client.mail.spec(prefix="bob", suffix=Suffix.linuxdo_space),
    ) as bindings:
        catch_all = bindings[0]
        alice = bindings[1]
        bob = bindings[2]

        for item in alice.listen(timeout=60):
            print("alice", item.subject)
```

多个邮箱可以复用同一个 `Client`：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as alice:
        with client.mail.bind(prefix="bob", suffix=Suffix.linuxdo_space) as bob:
            for item in client.listen(timeout=60):
                for mailbox in client.mail.route(item):
                    if mailbox is alice:
                        print("alice", item.subject)
                    elif mailbox is bob:
                        print("bob", item.subject)
```

如果你确实要消费多个 mailbox 自己的本地队列，就需要让这些
`mail.listen(...)` 同时处于活动状态，例如放到不同线程或任务里。
顺序调用 `alice.listen(...)` 再 `bob.listen(...)` 并不等于并行监听。

正则绑定也可以直接使用：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as catch_all:
        for item in catch_all.listen(timeout=60):
            print(item.address, item.subject)
```

如果你确实想保留旧的简写风格，`client.mail(...)` 仍然可用，但它只是 `client.mail.bind(...)` 的同义写法。

## 显式解绑

除了 `with` 自动解绑以外，也可以显式调用：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    alice = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
    catch_all = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)

    try:
        for item in alice.listen(timeout=60):
            print(item.subject)
    finally:
        client.mail.unbind(alice, catch_all)
```

## 全量流到子绑定的本地路由辅助

`client.listen(...)` 是完整接收接口。  
如果你在消费完整流时，想知道当前消息会命中哪些本地子绑定，可以使用只读路由辅助：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as catch_all:
        with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as alice:
            for item in client.listen(timeout=60):
                matched = client.mail.route(item)
                print(item.address, [mailbox.address or mailbox.pattern for mailbox in matched])
```

`client.mail.route(item)` 只会基于这条 `MailMessage` 当前的 `item.address` 做匹配，不会把整封原始多收件人事件重新展开。  
它返回的是“当前时刻的本地匹配结果”，不是对过去已经发生的队列投递做历史回放。

还需要注意一件事：

- `client.listen(...)` 是完整流视角，每个上游事件只会向你暴露一条 `MailMessage`
- 这条 `MailMessage.address` 是当前事件的投影地址
- 原始完整收件人列表仍然保留在 `item.recipients`
- `mail.listen(...)` 则是 mailbox 视角，会针对每个命中的收件地址分别投递

## 异常处理

```python
from LinuxDoSpace import AuthenticationError, Client, LinuxDoSpaceError, StreamError

try:
    with Client(token="你的 API Token") as client:
        for item in client.listen(timeout=60):
            print(item.subject)
except AuthenticationError:
    print("Token 无效，或后端拒绝了当前 Token。")
except StreamError:
    print("HTTPS 实时流建立失败，或者流中断。")
except LinuxDoSpaceError as exc:
    print(f"SDK 运行失败: {exc}")
```

## 设计说明

- `Client` 创建后会立即建立一条共享的 HTTPS 上游连接
- 一个 `Client` 始终只维护一条到 `/v1/token/email/stream` 的真实连接
- `Client` 会统一接收、统一解析、统一分发收到的所有邮件事件
- `client.listen(timeout=-1)` 是最核心的“全量接收”接口
- `client.listen(...)` 与 `mail.listen(...)` 的正数 `timeout` 都表示该迭代器的最长总时长，不是空闲超时
- `client.mail.bind(...)` 在创建时就会立即注册本地绑定
- `mail.close()` 会立即解绑；离开 `with` 作用域也会立即解绑
- `bind(...)` 不会为尚未开始 `listen()` 的 mailbox 悄悄积压历史消息
- `client.mail.bind(prefix=..., suffix=...).listen(...)` 是精确邮箱绑定
- `client.mail.bind(pattern=..., suffix=...).listen(...)` 是正则邮箱绑定
- `client.mail.bind_many(...)` 可以一次注册多条有序绑定
- `client.mail.route(message)` 只查看这条消息当前 `address` 会命中哪些本地子绑定
- `client.listen(...)` 每次返回一条上游事件投影，`message.address` 是当前投影地址，`message.recipients` 保留完整原始收件人列表
- `mail.listen(...)` 每次返回一条命中 mailbox 的收件地址投影，因此它看到的 `message.address` 可以和全量流视角不同
- `client.mail(...)` 只是 `client.mail.bind(...)` 的语法糖
- `Suffix.linuxdo_space` 会解析成 `<owner_username>.linuxdo.space`
- SDK 会忽略 `ready` 与 `heartbeat` 事件，只向你暴露真正的邮件事件
- 如果 `timeout` 为正数，则表示本次监听的最长总时长（秒）
- 返回对象会尽量把常用信息都变成属性，方便 IDE 自动补全

## 架构说明

- 服务端只知道一个 Token 对应一个客户端连接
- 服务端不会知道客户端内部绑定了哪些邮箱
- 客户端内部会根据邮件中的收件地址，在本地内存里完成筛选和分发
- 因此多个 `mail.bind()` 绑定不会增加服务端的上游连接数
- `client.listen(...)` 负责完整接收
- `client.mail.route(...)` 负责说明当前这条消息的 `address` 在本地会落到哪些子绑定
- `mail.listen(...)` 负责消费某一个已经注册完成、且当前处于监听状态的本地子队列

## 匹配规则

- `prefix` 和 `pattern` 必须二选一
- 精确绑定和正则绑定不会分成两套优先级
- 所有同一 `suffix` 下的绑定都按创建顺序进入同一条匹配链
- 某个绑定一旦匹配成功：
  - 它一定会收到消息
  - 如果 `allow_overlap=False`，则立即停止，不再继续检查后面的绑定
  - 如果 `allow_overlap=True`，则继续向后匹配，允许多个绑定同时收到
- 正则匹配使用的是邮箱本地前缀的 `fullmatch()`，不是 `search()`

示例：

- 如果先创建 `pattern=r".*"`，后创建 `prefix="alice"`，而第一个绑定没有开启 `allow_overlap`
  那么 `alice@<owner_username>.linuxdo.space` 会先命中 `.*`，并在那里停止，后面的精确绑定不会收到
- 如果先创建 `pattern=r".*"`, 且它设置了 `allow_overlap=True`
  后面创建的 `prefix="alice"` 也能继续收到

## 当前暴露的主要属性

每一封邮件会被解析成 `MailMessage` 对象，常用属性包括：

- `address`
- `sender`
- `recipients`
- `received_at`
- `subject`
- `message_id`
- `date`
- `from_header`
- `to_header`
- `cc_header`
- `reply_to_header`
- `from_addresses`
- `to_addresses`
- `cc_addresses`
- `reply_to_addresses`
- `text`
- `html`
- `headers`
- `raw`
- `raw_bytes`
- `message`

## 注意事项

- API Token 明文只会在创建时返回一次，请妥善保存
- Token 目标只有在客户端实际建立 HTTPS 流连接时才会收到邮件事件
- 如果服务端发现当前没有任何客户端连接，邮件事件会被直接丢弃，不会排队补发
- SDK 默认要求远程后端使用 `https://`；只有 `localhost` / `127.0.0.1` / `::1` 这类本地调试地址允许使用 `http://`
- 如果你需要对一个 Token 下的所有邮件做统一处理，请优先使用 `client.listen(...)`
- 同一个 `MailBox` 实例只允许一个活动监听器；如果你需要并行消费，请显式注册多个绑定实例
