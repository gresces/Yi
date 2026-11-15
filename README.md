# Yi

个人管理器:一条持久化的事件日志 + 若干拉取式消费者。server 形式,两个端口。

当前版本 `0.1.5`。

## 结构

```
src/bus/        事件总线(事件、消费者、游标、SQLite 存储)
src/web/        前端页面与资源,编译时嵌入二进制
src/main.go     参数、两个 HTTP server、优雅退出
src/config.go   配置文件解析与命令行合并
src/auth.go     Bearer token 与前端会话认证
src/install.go  systemd 服务安装/卸载
src/frontend.go 前端端口:页面 + /api/*
src/service.go  服务端口:JSON
lib/python/     Python 客户端(单文件 yi.py,生产者 + 消费者,零依赖)
lib/cpp/        C++17 单头文件客户端(libcurl + nlohmann/json)
lib/rust/       Rust 单文件客户端(std,零依赖)
lib/ts/         TypeScript 单文件客户端(fetch,零依赖)
app/            Android WebView 客户端 + 后台主题提醒
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
./dist/yi -config /etc/yi/yi.config           # 显式指定配置文件
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-config` | `/etc/yi/yi.config` | 配置文件路径 |
| `-host` | `127.0.0.1` | 两个端口共用的监听地址 |
| `-service-port` | `8081` | 服务端口 |
| `-frontend-port` | `8080` | 前端端口 |
| `-frontend` | `true` | 是否启动前端 |
| `-data` | `data` | SQLite 数据库所在目录 |
| `-archive-batch` | `256` | 归档批量;`0` 禁用归档 |
| `-auth-token` | 空 | Bearer 认证令牌;为空关闭认证 |
| `-version` | — | 打印版本并退出 |

## 配置文件

默认路径 `/etc/yi/yi.config`。默认文件不存在时直接使用内置默认值;显式传入
`-config` 但文件不存在会报错。优先级:**内置默认值 < 配置文件 < 命令行参数**。

格式是简单的 `key = value`,键名与上面的参数一致,`-` 和 `_` 等价。空行和以
`#`、`;` 开头的注释行会被忽略;值可以用单引号或双引号包起来(含空格时有用)。

```ini
# /etc/yi/yi.config
host = 127.0.0.1
service-port = 8081
frontend-port = 8080
frontend = true
data = /var/lib/yi
archive-batch = 256
auth-token = 0123456789abcdef0123456789abcdef
```

`data` 是 SQLite 数据库目录。以 `yi install` 安装的 systemd 服务使用
`/var/lib/yi`,并由 systemd 的 `StateDirectory` 自动创建和授权。

## 身份验证

配置 `auth-token`(至少 16 个字符)后,服务端口除 `/healthz` 外都要求:

```
Authorization: Bearer <token>
```

前端使用同一个 token 登录。登录成功后服务端设置 HttpOnly、SameSite=Strict
的 `yi_session` Cookie,`/api/*` 和 SSE `/api/stream` 都用它认证:

```
POST /api/login   {"token":"..."}   -> 204 + Set-Cookie
POST /api/logout                     -> 204
GET  /api/session                    -> {"authenticated":true,"auth_enabled":true}
```

静态页面、`/static/*`、`/api/login`、`/api/logout`、`/api/session` 和服务端口的
`/healthz` 不需要认证。`auth-token` 为空时认证关闭,启动日志会明确提示。

- 推荐用 `yi install` 生成配置,它会自动生成随机 token 并在首次写入时打印;
  已存在的配置不会被覆盖,需要时手动编辑其中的 `auth-token`。
- 生产环境优先把 token 放在配置文件;`-auth-token` 会出现在进程参数里。
- 前端登录、Python/C++/Rust/TypeScript 客户端都需要配置同一个 token:
  Python `Client(..., token=...)`、C++ `ClientOptions::token`、
  Rust `Client::with_token(...)`、TypeScript `new Client(url, { token })`。
- 当前服务是明文 HTTP;公网使用时请放在 TLS 反向代理后面,或用 SSH 隧道访问。

## systemd 服务

需要 root:

```sh
# 系统级(需要 root)
sudo ./dist/yi install
sudo systemctl status yi

# 当前用户的 systemd 服务(不需要 sudo)
./dist/yi install --user
systemctl --user status yi
```

`yi install` 会:

1. 把当前二进制安装到 `/usr/local/bin/yi`;
2. 在 `/etc/yi/yi.config` 写入默认配置(已存在则保留),新配置会自动生成 `auth-token` 并打印一次;
3. 写入 `/etc/systemd/system/yi.service`;
4. 执行 `systemctl daemon-reload`、`enable` 和 `restart`。

服务使用 `DynamicUser=yes` 和 `StateDirectory=yi`,不需要手工创建用户;
数据默认在 `/var/lib/yi`。unit 里带了 `ProtectSystem=strict`、`ProtectHome`、
`PrivateTmp`、`NoNewPrivileges`、`UMask=0077` 等基本加固。

常用选项:

| 参数 | 默认 | 说明 |
|---|---|---|
| `-user` | 关 | 安装为当前用户的 systemd 服务 |
| `-config` | `/etc/yi/yi.config` | 生成的配置文件路径 |
| `-unit-dir` | `/etc/systemd/system` | unit 文件目录 |
| `-name` | `yi` | 服务名 |
| `-bin` | `/usr/local/bin/yi` | 安装后的二进制路径 |
| `-exec` | 当前进程 | 要复制的二进制 |
| `-no-start` | 关 | 只写文件,不调用 `systemctl` |
| `-dry-run` | 关 | 只打印生成内容,不写文件 |

卸载:

```sh
sudo ./dist/yi uninstall
```

卸载会停止并禁用服务、删除 unit 文件,但保留二进制、配置和 `/var/lib/yi`
数据。`yi install` 和 `yi uninstall` 都支持 `-no-start`(适合容器或无 systemd
环境)和 `-dry-run`。

`--user` 模式的默认路径:

```text
~/.local/bin/yi
~/.config/yi/yi.config
~/.config/systemd/user/yi.service
~/.local/share/yi
```

它使用 `systemctl --user`,并尽力执行 `loginctl enable-linger`,让服务在
退出登录后继续运行;如果 linger 需要额外授权,命令会打印手动处理方法。

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
curl 'localhost:8081/events?limit=50&offset=0&type=note'
curl localhost:8081/stats
curl localhost:8081/failures
curl -H 'Authorization: Bearer <token>' localhost:8081/metrics

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

`/events` 的 `limit <= 0` 返回全部活跃事件;`type` 按“包含该类型”过滤,为空表示不筛选。

### Prometheus

服务端口提供 Prometheus 文本格式的 `GET /metrics`;配置了 `auth-token` 时
需要 Bearer 头:

```text
yi_up 1
yi_events_active 4336
yi_events_published_total 128000
yi_events_acked_total 127500
yi_events_nacked_total 500
yi_failures 44
yi_consumers 4
yi_consumer_pending{consumer="classifier"} 37
yi_consumer_cursor{consumer="classifier"} 982
yi_consumer_delivered_total{consumer="classifier"} 127000
yi_consumer_failed_total{consumer="classifier"} 500
yi_producer_events{source="phone"} 1024
yi_events_by_type{type="raw.captured"} 3944
```

Prometheus 抓取配置示例:

```yaml
scrape_configs:
  - job_name: yi
    static_configs:
      - targets: ["127.0.0.1:9092"]
    authorization:
      type: Bearer
      credentials: "<auth-token>"
```

指标是进程内累计值;进程重启后 counter 会从 0 开始,Prometheus 会自动处理
counter reset。Grafana 不是必需的:只使用 Prometheus 就可以查询、画基础曲线
和配置告警;Grafana 只是可选的展示层。

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

订阅也支持正则表达式:

```sh
curl -X PUT localhost:8081/consumers/mail -H 'Content-Type: application/json' -d '{
  "types": [],
  "patterns": ["^mail\\.", "^note$"]
}'
```

- `types` 是精确匹配;`patterns` 是 Go RE2 正则,对事件的任意一个类型执行
  `MatchString`(未自动加锚点,需要完整匹配时自己写 `^...$`)。
- 两类条件取并集;可以只写 `types`、只写 `patterns`,或两者都写。
- 至少需要一个非空类型或模式;空模式、非法正则、超过 64 个模式会返回 400。
- 模式会去空白、去重并排序;`/stats` 和 SSE 的消费者记录里有 `patterns` 字段。
- 正则不接受反向引用和 lookaround(Go RE2 限制)。

### 事件管理

前端的管理操作使用前端端口的 JSON 接口,全部需要登录态:

```sh
# 删除指定事件(按 source + id)
curl -b cookies.txt -X POST localhost:8080/api/events/delete \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"source":"phone","id":"..."}]}'

# 删除订阅者(消费者)
curl -b cookies.txt -X POST localhost:8080/api/consumers/delete \
  -H 'Content-Type: application/json' \
  -d '{"names":["classifier"]}'

# 删除生产者及其全部事件
curl -b cookies.txt -X POST localhost:8080/api/producers/delete \
  -H 'Content-Type: application/json' \
  -d '{"sources":["phone"]}'
```

单个请求最多 1000 项,返回 `{"deleted":N}`。事件删除是从活跃视图隐藏
(软删除),底层 seq 和 `(source,id)` 去重键保留,以保证消费者游标稳定;
消费者 pending 会重算,归档和分页只统计未删除事件。

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

详见 [lib/python/README.md](lib/python/README.md)。客户端是单文件 `lib/python/yi.py`,没有第三方依赖。

### C++ 客户端

C++17 单头文件库,依赖 `libcurl` 与 `nlohmann/json`,生产者和拉取式消费者都有:

```cpp
#include "yi.hpp"

yi::Client client("http://127.0.0.1:8081");

yi::CaptureOptions capture;
capture.source = "laptop";
client.capture("随手记一句", capture);

yi::Consumer consumer(client, "classifier", {"raw.captured"});
while (auto message = consumer.next()) {
    try {
        handle(message->event());
        message->ack();
    } catch (const std::exception &error) {
        message->nack(error.what());
    }
}
```

```sh
c++ -std=c++17 -Ilib/cpp app.cpp -lcurl
make -C lib/cpp test BASE_URL=http://127.0.0.1:8081
```

详见 [lib/cpp/README.md](lib/cpp/README.md)。

### Rust 客户端

单文件 `lib/rust/yi.rs`,零第三方依赖,自带 JSON 和 HTTP/1.1:

```rust
use yi::{Client, Consumer};

let client = Client::new("http://127.0.0.1:8081")?;
client.capture("随手记一句")?;

let consumer = Consumer::new(&client, "classifier", ["raw.captured"])?;
while let Some(message) = consumer.next()? {
    message.ack()?;
}
```

```sh
cargo test --manifest-path lib/rust/Cargo.toml
YI_BASE_URL=http://127.0.0.1:8081 \
  cargo test --manifest-path lib/rust/Cargo.toml live_e2e -- --nocapture
```

详见 [lib/rust/README.md](lib/rust/README.md)。

### TypeScript 客户端

单文件 `lib/ts/yi.ts`,零运行时依赖,使用标准 `fetch`:

```ts
import { Client, Consumer } from "./lib/ts/yi.ts";

const client = new Client("http://127.0.0.1:8081");
await client.capture("随手记一句", { source: "laptop" });

const consumer = await Consumer.open(client, "classifier", ["raw.captured"]);
for await (const message of consumer) {
    try {
        await handle(message.event);
        await message.ack();
    } catch (error) {
        await message.nack(error);
    }
}
```

```sh
node --test lib/ts/tests/unit.test.mjs
node --test lib/ts/tests/e2e.test.mjs   # 需要先启动服务
```

详见 [lib/ts/README.md](lib/ts/README.md)。

### Android 客户端

`app/` 是 WebView 客户端:前台直接加载 Yi 前端;可选的常驻前台服务按事件类型/
关键词轮询服务端口并发送 Android 通知,支持开机恢复和通知权限。

```sh
./app/build.sh
adb install -r app/build/yi-debug.apk
adb shell am start -n com.gresces.yi/.MainActivity
```

详见 [app/README.md](app/README.md)。

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
make test                  # Go 单测 + Python 客户端测试
make ts-test               # TypeScript 单元测试
TOKEN=... make ts-e2e      # TypeScript 端到端,需要服务;认证时传 token
make rust-test             # Rust 单元测试
TOKEN=... make rust-e2e    # Rust 端到端,需要服务;认证时传 token
YI_TOKEN=... make -C lib/cpp test BASE_URL=http://127.0.0.1:8081
./app/build.sh             # Android APK(需要 Android SDK)
make race                  # go test -race
make bench                 # 核心路径基准
make stress                # 压力测试,需要先启动服务
make logic                 # 模型对照的逻辑测试,需要空数据库
```

逻辑测试会对照本地模型检查游标、pending、去重、分页、类型筛选和失败记录;建议用 `-archive-batch 0` 启动一个空库,保证结果确定。
