# Yi

个人管理器:一条持久化的事件日志 + 若干拉取式消费者。server 形式,两个端口。

当前版本 `0.1.0`,变更见 [CHANGELOG.md](CHANGELOG.md)。

## 结构

```
src/bus/        事件总线(事件、消费者、游标、SQLite 存储)
src/web/        前端页面与资源,编译时嵌入二进制
src/main.go     参数、两个 HTTP server、优雅退出
src/frontend.go 前端端口:页面 + /api/*
src/service.go  服务端口:JSON
lib/python/     Python 客户端(生产者 + 消费者,零依赖)
dist/           构建产物
data/           运行时数据
```

## 命令

```sh
go test ./...
go build -o dist/yi ./src

./dist/yi                                     # 127.0.0.1,前端 :8080,服务 :8081
./dist/yi -host 0.0.0.0 -frontend-port 9000 -service-port 9001
./dist/yi -frontend=false                     # 只起服务端口
./dist/yi -data /var/lib/yi
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-host` | `127.0.0.1` | 两个端口共用的监听地址 |
| `-service-port` | `8081` | 服务端口 |
| `-frontend-port` | `8080` | 前端端口 |
| `-frontend` | `true` | 是否启动前端 |
| `-data` | `data` | SQLite 数据库所在目录 |
| `-archive-batch` | `256` | 归档批量;`0` 禁用归档 |
| `-version` | — | 打印版本并退出 |

## 前端

浏览器打开 `http://127.0.0.1:8080/`。Dashboard 布局:深色侧栏、KPI 行、生产者与消费者面板;卡片 12px 圆角,强调色 `#6366f1`,数字使用等宽对齐。

三个视图,用 hash 路由切换:

- `#/overview`(默认):KPI、紧凑快速记录、生产者状态、消费者状态
- `#/events`:事件列表,带类型筛选、每页条数和分页
- `#/failures`:失败记录,分页展示被放弃的事件与错误

状态面板通过 **SSE** 推送,没有刷新按钮:

```
GET /api/stream        text/event-stream

event: snapshot
data: {"events":[...最新50条...],"total":4336,
       "consumers":[...],"failures":[...],"failure_total":44,
       "producers":[{"source":"web","count":123,"last_at":"..."}],
       "types":[{"type":"raw.captured","count":3944},{"type":"note","count":432}]}

: ping                 每 20 秒
```

连接建立时推一次快照,之后每次发布、ack、nack、注册/注销消费者都会再推一次。浏览器断线后由 `EventSource` 自动重连,页面保留最后一份数据。

事件页的分页与筛选走服务端,每页条数可选 25 / 50 / 100 / 200(默认 50,记在 `localStorage`):

```
GET /api/events?limit=50&offset=0&type=mail.received
-> {"events":[...],"total":456,"offset":0,"limit":50}
```

`offset` 从最新一页开始算。事件页第 1 页且无筛选时,总数增加会自动刷新;翻页或筛选时只在标题旁提示「有新数据」,不打断当前阅读。

事件类型是数组(`types`),一条事件可以同时命中多个类型;筛选按“包含该类型”匹配,类型 chip 显示原始字符串。

失败记录同样分页:

```
GET /api/failures?limit=50&offset=0
-> {"failures":[...],"total":36,"offset":0,"limit":50}
```

SSE 快照里的 `failure_total` 是失败总数,`failures` 数组只用于增量判断。

吞吐量与队列深度(监控接口,按 1 秒一个桶,窗口默认 120 秒;前端不再绘图):

```
GET /api/metrics?window=120
-> {"step":1,"window":120,
    "published":[...],"acked":[...],"failed":[...],
    "pending":12793}
```

SSE 快照里的 `producers` 是各输入来源的累计状态(`source`、`count`、`last_at`)。

采集仍用:

```
POST /api/capture  {"text":"...","source":"web"}
-> {"id":"...","duplicate":false,"matched":1}
```

`/api/stats` 仍然保留,供脚本或调试直接读取。

## 服务端口

```sh
curl localhost:8081/healthz
curl 'localhost:8081/events?limit=50'
curl localhost:8081/stats
curl localhost:8081/failures

curl -X POST localhost:8081/events -H 'Content-Type: application/json' -d '{
  "id": "01JC8Z3Q7K9V2M4N6P8R0T2W4X",
  "source": "phone",
  "types": ["raw.captured"],
  "time": "2026-11-14T08:32:11+08:00",
  "data": {"text": "下周三前把合同发给小王"}
}'
```

发布返回:

```json
{"duplicate": false, "matched": 1}
```

`matched` 是发布这一刻订阅了该类型的消费者数量。`0` 不是错误,只是“此刻没人接”;之后注册的消费者仍会读到历史事件。

### 远程消费者

```sh
curl -X PUT localhost:8081/consumers/py -H 'Content-Type: application/json' \
  -d '{"types":["raw.captured"]}'

curl 'localhost:8081/consumers/py/next?timeout=25'   # 204 表示超时
curl -X POST localhost:8081/consumers/py/ack  -d '{"id":"..."}'
curl -X POST localhost:8081/consumers/py/nack -d '{"id":"...","error":"boom"}'
curl -X DELETE localhost:8081/consumers/py
```

未确认之前重复 `next` 返回同一条事件,这就是重试。

### 归档

没有对外接口。后端会自动把“所有消费者都已越过”的事件移出活跃集合:

- 水位 = 所有消费者游标的最小值,只在内部使用。
- 每次 Ack/Nack 后检查;水位以下积压达到 256 条时,把这些行的 `archived` 置 1。
- 关闭服务时把水位以下剩余的也归档。
- 归档行仍保存在 `events` 表里,但启动时只加载 `archived = 0` 的行,也不进内存索引。
- 因此 `/events`、分页、事件总数都只覆盖活跃事件;新消费者只能读到未归档的历史。
- 去重仍然有效:唯一索引覆盖归档行,重复发布会返回 `duplicate: true`。
- 一个长期不拉取的消费者会压住水位,归档随之暂停——这是真实进度。

### Python 客户端

```python
from yi import Client, Consumer

client = Client("http://127.0.0.1:8081")
client.capture("随手记一句", source="laptop")

with Consumer(client, "classifier", ["raw.captured"]) as consumer:
    for message in consumer:
        try:
            handle(message.event)
        except Exception as exc:
            message.nack(exc)
        else:
            message.ack()
```

详见 [lib/python/README.md](lib/python/README.md)。

## 持久化

`data/yi.db`(SQLite,WAL),三张表:

```
events      seq 主键,只追加;UNIQUE(source, id) 用于去重
consumers   name 主键,保存 types / cursor / 统计
failures    Nack 记录
```

- 重启后事件、消费者游标、失败记录都会恢复。
- `inflight` 不落盘:已取出但未确认的事件在重启后重新投递,所以处理器要按事件 ID 幂等。
- 启动时把事件载入内存做索引;数据库是持久层,内存是读缓存。

## 语义约定

- 事件对外只有五个字段:`id`、`source`、`types`、`time`、`data`;`types` 是非空数组。
- 位置和接收时间属于总线内部,不出现在任何响应里。
- 游标只在 `Ack` / `Nack` 时推进;未确认的事件会重复投递。
- 没有消费者的事件不会消失,也不会报错;后来注册的消费者能读到历史。
- 没有“全部送达”状态;判断滞后看各消费者的 `pending`。
- 事件不会因为被消费而删除,日志只增不减。

## 测试

```sh
make test     # Go 单测 + Python 客户端测试
make race     # go test -race
make bench    # 核心路径基准
make stress   # 压力测试,需要先启动服务
make logic    # 模型对照的逻辑测试,需要空数据库
```

逻辑测试会对照本地模型检查游标、pending、去重、分页、类型筛选和失败记录;建议用 `-archive-batch 0` 启动一个空库,保证结果确定。
