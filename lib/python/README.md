# Yi Python 客户端

零第三方依赖的 Yi 服务端客户端，同时支持生产者和消费者。实现是单文件
`yi.py`，只使用标准库，兼容 Python 3.9+。

## 安装

从本目录安装（可编辑模式）：

```sh
cd lib/python
python3 -m pip install -e .
```

也可以不安装，直接把 `lib/python` 加入 `PYTHONPATH`。

## 生产者

```python
from yi import Client, Event

client = Client("http://127.0.0.1:8081")

print(client.health())                 # "ok"

# 快捷方式:写入一条 raw.captured 事件
client.capture("下周三前把合同发给小王", source="phone")

# 完整控制所有字段
result = client.publish(Event.new(
    source="phone",
    types=["raw.captured"],
    data={"text": "买牛奶", "tags": ["errand"]},
))
print(result.duplicate, result.matched)

for event in client.events(limit=10):
    print(event.time, event.source, event.types, event.data)
```

事件的唯一性由 `(source, id)` 决定；重复发布时 `publish` 返回
`PublishResult(duplicate=True, ...)`，服务端不会再次保存。

## 消费者

```python
from yi import Client, Consumer

client = Client("http://127.0.0.1:8081")

consumer = Consumer(client, "classifier", ["raw.captured"])
try:
    for message in consumer:          # 无限迭代,直到 close()
        try:
            handle(message.event)
        except Exception as exc:
            message.nack(exc)         # 记录失败并跳过
        else:
            message.ack()             # 推进游标
finally:
    consumer.deregister()             # 显式从服务端删除注册
```

也可以不用迭代器：

```python
message = consumer.next()             # 长轮询,超时返回 None
if message is not None:
    message.ack()
```

`with Consumer(...) as consumer:` 会在退出时调用 `close()`，但这只停止本地
使用，**不会**删除服务端注册；需要删除时调用 `deregister()`。

长轮询默认等待 25 秒，可以通过 `Consumer(..., timeout=5)` 或
`consumer.next(timeout=5)` 调整。服务端会把等待时间限制在 60 秒以内。

### 正则订阅

`types` 是精确匹配；`patterns` 是 Go RE2 正则表达式，对每个事件类型逐个匹配。
一个事件只要命中任意一个 type **或** 任意一个 pattern 就会被投递（并集）。

正则默认是**非锚定**的：`mail\.` 能匹配 `mail.received`，也能匹配
`inbox.mail.received`。需要整串匹配时自行加 `^` 和 `$`，例如 `^note$`。
只支持 Go RE2 语法，**不支持反向引用和环视**（`(?=...)`、`(?<=...)`）。

```python
from yi import Client, Consumer

client = Client("http://127.0.0.1:8081")

# 精确类型 + 正则，取并集
consumer = Consumer(
    client,
    "classifier",
    ["raw.captured"],
    patterns=[r"^mail\.", r"^note$"],
)

# 只订阅正则:types 可以为空,但 types 和 patterns 不能同时为空
notes = Consumer(client, "notes", [], patterns=[r"^note\."])

# 也可以直接调用 register;patterns 为空时不会出现在请求体里
client.register("debug", ["raw.captured"], patterns=[r"^debug\."])
```

服务端会 trim、排序、去重；非法正则或空正则会返回 HTTP 400
（`HTTPError.status == 400`）。不传 `patterns` 或传空序列时，请求体只有
`types`，与旧版服务端保持兼容。`ConsumerStats.patterns` 是对应的正则元组，
旧版服务端不返回该字段时为空元组。

## 认证

服务端配置了 `auth-token` 时，服务端口的所有请求都必须带
`Authorization: Bearer <token>`。构造客户端时传入同一个 token 即可：

```python
from yi import Client, Consumer

client = Client("http://127.0.0.1:8081", token="0123456789abcdef0123456789abcdef")
consumer = Consumer(client, "classifier", ["raw.captured"])
```

`Consumer`、`Message` 复用同一个 `Client`，因此 ack/nack 也自动带 token。
`token` 默认为空字符串，此时不发送 `Authorization` 头，行为与未开启认证时
完全一致。

## 错误处理

```python
from yi import ConflictError, HTTPError, YiError

try:
    client.register("classifier", ["other.type"])
except ConflictError as exc:          # HTTP 409
    print(exc.status, exc.body)
except HTTPError as exc:              # 其他非 2xx 响应
    print(exc.status, exc.body)
except YiError as exc:                # 网络错误、超时、坏响应
    print(exc)
```

- `HTTPError.status` 是 HTTP 状态码，`HTTPError.body` 是原始响应文本。
- `ConflictError` 继承自 `HTTPError`，对应 409。
- 所有网络层异常和 urllib 的 `HTTPError` 都会被包装为本库异常，不会泄漏给
  调用方。

## 测试

```sh
cd lib/python
python3 -m unittest discover -s tests -t . -v
```

测试使用 `tests/fake_server.py` 中的内存版 HTTP 服务，不需要启动 Go 服务。

## 压力测试

`stress/stress.py` 编排 N 个并发生产者和 M 个独立消费者，并校验：去重后的总数、每个消费者收到的事件数、重复投递、服务端游标、delivered/failed 计数、注入的重复发布。

先启动服务，建议用空数据目录：

```sh
./dist/yi -frontend=false -data /tmp/yi-stress

python3 lib/python/stress/stress.py \
  --base-url http://127.0.0.1:8081 \
  --producers 6 --consumers 6 --events-per-producer 500 \
  --duplicate-every 50 --nack-rate 0.02
```

服务开启认证时加 `--token <token>`，或设置环境变量 `YI_TOKEN`。

常用参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--producers` | 4 | 并发生产者数 |
| `--consumers` | 3 | 独立消费者数，每个都消费全量 |
| `--events-per-producer` | 250 | 每个生产者发布的事件数 |
| `--nack-rate` | 0.01 | 消费者随机放弃的比例 |
| `--duplicate-every` | 50 | 每 N 条注入一次重复发布，0 关闭 |
| `--timeout` | 5 | 长轮询秒数 |
| `--deadline` | 180 | 整场最长秒数 |
| `--keep` | 关 | 保留消费者注册，便于事后查看 |
| `--json PATH` | — | 输出机器可读结果 |

新消费者会读到数据库里的历史事件，脚本会先测量存量并调整期望值；延迟分位只统计本轮事件。建议在空数据库上跑，数字更干净。
