# Yi Rust 客户端

零第三方依赖、单文件实现的 Rust 客户端,覆盖生产者和拉取式消费者。

- **单文件**:全部代码和测试都在 `yi.rs` 里;`Cargo.toml` 只是构建元数据。
- **零依赖**:HTTP/1.1 基于 `std::net::TcpStream`,JSON 解析/序列化也在文件内实现。
- **仅 `http://`**:Yi 服务本身是明文 HTTP;需要 TLS 时请在前面放反向代理。
- **风格**:`Result<T, Error>`、`Option`、`Duration`、RAII 消费者句柄。

## 使用

```toml
[dependencies]
yi-client = { path = "lib/rust" }
```

```rust
use yi::{Client, Consumer};

fn main() -> Result<(), yi::Error> {
    let client = Client::new("http://127.0.0.1:8081")?;
    client.capture("随手记一句")?;

    let consumer = Consumer::new(&client, "classifier", ["raw.captured"])?;
    while let Some(message) = consumer.next()? {
        match handle(message.event()) {
            Ok(()) => message.ack()?,
            Err(error) => message.nack(&error.to_string())?,
        }
    }
    Ok(())
}
```

也可以不用 Cargo,直接编译:

```sh
rustc --crate-type lib lib/rust/yi.rs
rustc --test lib/rust/yi.rs -o /tmp/yi-rust-tests
/tmp/yi-rust-tests
```

## 生产者

```rust
use yi::{CaptureOptions, Client, Event, Json};

fn main() -> Result<(), yi::Error> {
    let client = Client::new("http://127.0.0.1:8081")?;

    // 快捷方式:写入一条 raw.captured 事件
    client.capture("下周三前把合同发给小王")?;

    // 完整控制字段
    let mut capture = CaptureOptions::default();
    capture.source = "phone".into();
    capture.types = vec!["raw.captured".into()];
    capture.data = Json::from_pairs([("tags", Json::from("errand"))]);
    client.capture_with("买牛奶", capture)?;

    let event = Event::new(
        "phone",
        ["raw.captured"],
        Json::from_pairs([("text", Json::from("买牛奶"))]),
    )?;
    let result = client.publish(&event)?;
    println!("duplicate={} matched={}", result.duplicate, result.matched);
    Ok(())
}
```

`Event::new` 生成 32 位十六进制 id 和 UTC RFC3339 时间;`data` 必须是 JSON 对象。
重复发布同一个 `(source, id)` 时返回 `duplicate == true`。

`Json` 是库内的 JSON 类型,支持 `Null`、布尔、整数/浮点、字符串、数组和对象:

```rust
let data = Json::from_pairs([
    ("text", Json::from("hello")),
    ("count", Json::from(3_i64)),
    ("done", Json::from(true)),
]);
```

## 消费者

```rust
use std::time::Duration;
use yi::{Client, Consumer};

fn main() -> Result<(), yi::Error> {
    let client = Client::new("http://127.0.0.1:8081")?;
    let mut consumer = Consumer::with_timeout(
        &client,
        "classifier",
        ["raw.captured"],
        Duration::from_secs(25),
    )?;

    while let Some(message) = consumer.next()? {
        match handle(message.event()) {
            Ok(()) => message.ack()?,
            Err(error) => message.nack(&error.to_string())?,
        }
    }
    Ok(())
}
```

- `next()` 长轮询,超时返回 `Ok(None)`。
- `Consumer` 构造时立即在服务端注册;析构**不会**删除服务端注册,需要显式
  `consumer.deregister()?`。
- `consumer.close()` 只停止本地使用。
- 未确认之前重复 `next()` 返回同一条事件,所以重试就是“不 ack 再取”。
- `consumer.stats()?` 返回当前消费者的统计。

## 查询与管理

```rust
use yi::{Client, EventsQuery};

fn main() -> Result<(), yi::Error> {
    let client = Client::new("http://127.0.0.1:8081")?;
    println!("{}", client.health()?);

    let events = client.events(EventsQuery {
        limit: 50,                  // 0 返回全部活跃事件
        offset: 0,                  // 从最新一页开始
        event_type: Some("note".into()),
    })?;

    let consumers = client.stats()?;
    let failures = client.failures()?;

    client.register("debug", ["note"])?;
    client.deregister("debug")?;
    Ok(())
}
```

## 错误

```rust
use yi::{Client, Error};

fn main() -> Result<(), yi::Error> {
    match Client::new("http://127.0.0.1:8081")?.register("classifier", ["other.type"]) {
        Ok(_) => {}
        Err(Error::Conflict(error)) => eprintln!("HTTP {}: {}", error.status, error.body),
        Err(Error::Http(error)) => eprintln!("HTTP {}: {}", error.status, error.message),
        Err(Error::Io(error)) => eprintln!("network: {error}"),
        Err(error) => eprintln!("{error}"),
    }
    Ok(())
}
```

- `Error::Conflict` 对应 HTTP 409,`Error::Http` 对应其他非 2xx。
- `Error::Io` 包含连接失败、读写超时。
- 所有 JSON 解析错误都会变成 `Error::Json`,不会静默返回空数据。
- `Client` 构造后不可变,可以在多个线程之间共享。
- `Consumer` 是服务端注册的本地句柄,不要在多个线程里同时使用。

## 测试

```sh
cd lib/rust
cargo test

# 对运行中的服务跑端到端测试
YI_BASE_URL=http://127.0.0.1:8081 cargo test live_e2e -- --nocapture
```

单元测试覆盖 JSON、模型、URL 编码和 HTTP 分块/Content-Length 响应;端到端测试默认
跳过,只有设置 `YI_BASE_URL` 时才执行。
