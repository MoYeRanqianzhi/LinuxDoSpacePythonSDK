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

也可以继续使用更方便的单邮箱监听：

```python
from LinuxDoSpace import Client, Suffix

with Client(token="你的 API Token") as client:
    with client.mail("alice", Suffix.linuxdo_space) as mail:
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
    with client.mail("alice", Suffix.linuxdo_space) as alice:
        with client.mail("bob", Suffix.linuxdo_space) as bob:
            for item in alice.listen(timeout=60):
                print("alice", item.subject)
            for item in bob.listen(timeout=60):
                print("bob", item.subject)
```

## 设计说明

- `Client` 创建后会立即建立一条共享的 HTTPS 上游连接
- 一个 `Client` 始终只维护一条到 `/v1/token/email/stream` 的真实连接
- `Client` 会统一接收、统一解析、统一分发收到的所有邮件事件
- `client.listen(timeout=-1)` 是最核心的“全量接收”接口
- `client.mail(prefix, suffix).listen(...)` 是建立在同一个 `Client` 分发器之上的本地过滤便利层
- `Suffix` 是一个专门的枚举类型，避免把后缀写成普通字符串
- SDK 会忽略 `ready` 与 `heartbeat` 事件，只向你暴露真正的邮件事件
- 如果 `timeout` 为正数，则表示本次监听的最长总时长（秒）
- 返回对象会尽量把常用信息都变成属性，方便 IDE 自动补全

## 架构说明

- 服务端只知道一个 Token 对应一个客户端连接
- 服务端不会知道客户端内部绑定了哪些邮箱
- 客户端内部会根据邮件中的收件地址，在本地内存里完成筛选和分发
- 因此多个 `mail()` 绑定不会增加服务端的上游连接数
- 单个 `mail()` 只是方便函数，底层依赖的是 `Client` 级全量接收

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
