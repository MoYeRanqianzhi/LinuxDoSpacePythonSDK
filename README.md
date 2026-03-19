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

client = Client(token="你的 API Token")

with client.mail("alice", Suffix.linuxdo_space) as mail:
    for item in mail.listen(timeout=60):
        print(item.address)
        print(item.sender)
        print(item.subject)
        print(item.text)
```

## 设计说明

- `Client` 只负责 Token、基础 URL 和连接参数
- `Suffix` 是一个专门的枚举类型，避免把后缀写成普通字符串
- `client.mail(prefix, suffix)` 会返回一个上下文管理器
- `listen(timeout=-1)` 会建立到 `/v1/token/email/stream` 的 HTTPS 流
- SDK 会忽略 `ready` 与 `heartbeat` 事件，只向你暴露真正的邮件事件
- 如果 `timeout` 为正数，则表示本次监听的最长总时长（秒）
- 返回对象会尽量把常用信息都变成属性，方便 IDE 自动补全

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
- 如果服务端发现当前没有连接，邮件事件会被直接丢弃，不会排队补发
