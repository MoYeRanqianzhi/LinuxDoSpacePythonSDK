# LinuxDoSpace Python SDK

`LinuxDoSpace` 是 LinuxDoSpace 邮件 HTTPS 实时流的 Python SDK。

它当前专注在一个目标能力：

- 使用站点后台签发的 API Token
- 通过 `HTTPS` 长连接实时接收邮件事件
- 把原始邮件解析成带完整属性提示的 Python 对象

## 安装

当前建议直接以子项目方式使用：

```bash
cd sdk/python
python -m pip install -e .
```

## 快速开始

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

如果你更喜欢 `with`，它只是上面显式注册写法的语法糖：

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

多个邮箱可以并行复用同一个 `Client`：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as alice:
        with client.mail.bind(prefix="bob", suffix=Suffix.linuxdo_space) as bob:
            for item in alice.listen(timeout=60):
                print("alice", item.subject)
            for item in bob.listen(timeout=60):
                print("bob", item.subject)
```

正则绑定也可以直接使用：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as catch_all:
        for item in catch_all.listen(timeout=60):
            print(item.address, item.subject)
```

如果你确实想保留旧的简写风格，`client.mail(...)` 仍然可用，但它只是 `client.mail.bind(...)` 的同义写法。

## 设计说明

- `Client` 创建后会立即建立一条共享的 HTTPS 上游连接
- 一个 `Client` 始终只维护一条到 `/v1/token/email/stream` 的真实连接
- `Client` 会统一接收、统一解析、统一分发收到的所有邮件事件
- `client.listen(timeout=-1)` 是最核心的“全量接收”接口
- `client.mail.bind(prefix=..., suffix=...).listen(...)` 是精确邮箱绑定
- `client.mail.bind(pattern=..., suffix=...).listen(...)` 是正则邮箱绑定
- `client.mail(...)` 只是 `client.mail.bind(...)` 的语法糖
- `Suffix` 是一个专门的枚举类型，避免把后缀写成普通字符串
- SDK 会忽略 `ready` 与 `heartbeat` 事件，只向你暴露真正的邮件事件
- 如果 `timeout` 为正数，则表示本次监听的最长总时长（秒）
- 返回对象会尽量把常用信息都变成属性，方便 IDE 自动补全

## 架构说明

- 服务端只知道一个 Token 对应一个客户端连接
- 服务端不会知道客户端内部绑定了哪些邮箱
- 客户端内部会根据邮件中的收件地址，在本地内存里完成筛选和分发
- 因此多个 `mail.bind()` 绑定不会增加服务端的上游连接数
- 单个 `mail.bind()` 只是方便函数，底层依赖的是 `Client` 级全量接收

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
  那么 `alice@linuxdo.space` 会先命中 `.*`，并在那里停止，后面的精确绑定不会收到
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
