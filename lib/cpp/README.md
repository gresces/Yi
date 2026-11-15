# Yi C++ 客户端

现代 C++17 单头文件客户端,覆盖生产者和拉取式消费者。

- **单头文件**:只需要 `#include "yi.hpp"`,没有待编译的库源码。
- **依赖**:`libcurl`(链接 `-lcurl`)与 [`nlohmann/json`](https://github.com/nlohmann/json)(单头文件 JSON 库)。
- **风格**:RAII、`std::optional`、`std::string_view`、`std::chrono`、异常。

```sh
# Debian / Ubuntu
sudo apt install libcurl4-openssl-dev nlohmann-json3-dev

# 编译一个使用者
c++ -std=c++17 -Ilib/cpp app.cpp -lcurl
```

## 生产者

```cpp
#include "yi.hpp"

yi::Client client("http://127.0.0.1:8081");

yi::CaptureOptions capture;
capture.source = "laptop";
capture.types = {"raw.captured"};
capture.data = {{"tags", {"errand"}}};
client.capture("下周三前把合同发给小王", capture);

auto event = yi::Event::create(
    "phone", {"raw.captured"}, {{"text", "买牛奶"}});
auto result = client.publish(event);
if (result.duplicate) {
    // (source, id) 已存在
}
```

`Event::create` 生成 32 位十六进制 id 和 UTC RFC3339 时间;`data` 必须是 JSON 对象。
`publish` 重复发布同一个 `(source, id)` 时返回 `duplicate == true`。

## 消费者

```cpp
yi::ConsumerOptions options;
options.poll_timeout = std::chrono::seconds(25);

yi::Consumer consumer(client, "classifier", {"raw.captured"}, options);

while (auto message = consumer.next()) {   // 长轮询,超时返回 nullopt
    try {
        handle(message->event());
        message->ack();
    } catch (const std::exception &error) {
        message->nack(error.what());
    }
}
```

也可以直接范围遍历:

```cpp
for (const auto &message : consumer) {
    message.ack();
}
```

`Consumer` 构造时立即在服务端注册;析构**不会**删除服务端注册,需要显式调用
`consumer.deregister()`。`consumer.close()` 只停止本地使用。

长轮询默认 25 秒,服务端会把等待时间限制在 60 秒以内。未确认之前重复 `next()`
返回同一条事件,所以重试就是“不 ack 再取”。

### 正则订阅

`types` 是精确匹配;`ConsumerOptions::patterns` 是 Go RE2 正则表达式,对每个
事件类型逐个匹配。事件命中任意一个 type **或** 任意一个 pattern 即投递(并集)。

正则默认**非锚定**:`mail\.` 同时匹配 `mail.received` 和
`inbox.mail.received`;需要整串匹配时自己加 `^`/`$`。只支持 Go RE2,
**不支持反向引用和环视**。

```cpp
yi::ConsumerOptions options;
options.poll_timeout = std::chrono::seconds(25);
options.patterns = {R"(^mail\.)", R"(^note$)"};

// 精确类型 + 正则
yi::Consumer consumer(client, "classifier", {"raw.captured"}, options);

// 只订阅正则:types 可以为空,但不能和 patterns 同时为空
yi::Consumer notes(client, "notes", {}, options);

// 直接注册也可以;patterns 为空时请求体不含该字段
client.register_consumer("debug", {"raw.captured"}, {R"(^debug\.)"});
```

服务端会 trim、排序、去重;非法或空正则返回 HTTP 400。`Consumer::patterns()`
返回规范化后的本地副本,`ConsumerStats::patterns` 是服务端返回的订阅正则。

## 认证

服务端配置了 `auth-token` 时,服务端口的所有请求都必须带
`Authorization: Bearer <token>`。把同一个 token 放进 `ClientOptions` 即可:

```cpp
yi::ClientOptions options;
options.token = "0123456789abcdef0123456789abcdef";              // 留空则不发 Authorization 头

yi::Client client("http://127.0.0.1:8081", options);
yi::Consumer consumer(client, "classifier", {"raw.captured"});
```

`Consumer` 和 `Message` 内部持有 `Client` 的副本,所以 token 会随复制/移动一起
保留;ack、nack、长轮询等所有请求都会带上同一个头。`options.token` 默认为空
字符串,此时行为与未开启认证的服务完全一致。

## 查询与管理

```cpp
std::cout << client.health() << "\n";

yi::EventsQuery query;
query.limit = 50;          // <= 0 返回全部活跃事件
query.offset = 0;          // 从最新一页开始
query.type = "note";       // 可选类型过滤
auto events = client.events(query);

auto stats = client.stats();        // 全部消费者
auto own = consumer.stats();        // 当前消费者
auto failures = client.failures();

client.register_consumer("debug", {"note"});
client.deregister("debug");
```

## 错误

```cpp
try {
    client.register_consumer("classifier", {"other.type"});
} catch (const yi::ConflictError &error) {   // HTTP 409
    std::cerr << error.status() << " " << error.body() << "\n";
} catch (const yi::HttpError &error) {       // 其他非 2xx
    std::cerr << error.status() << "\n";
} catch (const yi::TransportError &error) {  // 连接失败、超时、CURLE_*
    std::cerr << error.code() << " " << error.what() << "\n";
} catch (const yi::Error &error) {
    std::cerr << error.what() << "\n";
}
```

- `yi::Client` 构造后不可变,可以在多个线程之间共享。
- `yi::Consumer` 是服务端注册的本地句柄,不要在多个线程里同时使用。
- `Consumer` 和 `Message` 内部保存 `Client` 的轻量副本,不依赖原 `Client` 的生命周期。
- 所有网络错误和 JSON 解析错误都会抛异常,不会静默返回空数据。

## 测试

```sh
make -C lib/cpp test BASE_URL=http://127.0.0.1:8081
```

`test` 会先跑不需要服务端的单元测试,再对运行中的服务跑端到端测试。端到端测试
使用唯一类型和消费者名,可以跑在已有数据的数据库上。服务开启认证时,通过环境
变量传入同一个 token:

```sh
YI_TOKEN=s3cret make -C lib/cpp test BASE_URL=http://127.0.0.1:8081
```
