# Yi TypeScript 客户端

零运行时依赖的 TypeScript 客户端,整个实现只有一个文件:`yi.ts`。

- 使用标准 `fetch`,可运行在浏览器、Node 18+、Deno、Bun。
- 同时支持生产者、拉取式消费者和查询/管理接口。
- TypeScript 类型完整,`fetch` 可注入(便于测试、代理和自定义 agent)。
- 没有第三方运行时依赖。

Node 23.6+ 可以直接执行 `.ts` 文件(type stripping);Node 22.6–23.5 需要加
`--experimental-strip-types`,更老的运行时可以用 `tsc` 编译后再使用。

## 生产者

```ts
import { Client, createEvent } from "./yi.ts";

const client = new Client("http://127.0.0.1:8081");

await client.capture("下周三前把合同发给小王", {
  source: "laptop",
  types: ["raw.captured"],
  data: { tags: ["errand"] },
});

const event = createEvent({
  source: "phone",
  types: ["raw.captured"],
  data: { text: "买牛奶" },
});

const result = await client.publish(event);
if (result.duplicate) {
  // (source, id) 已存在
}
```

`createEvent` 生成 32 位十六进制 id 和 UTC RFC3339 时间;`data` 必须是 JSON 对象。
重复发布同一个 `(source, id)` 时返回 `duplicate: true`。

## 消费者

```ts
import { Client, Consumer } from "./yi.ts";

const client = new Client("http://127.0.0.1:8081");

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

`Consumer.open` 会立即在服务端注册;因为注册是网络请求,所以它是 async 工厂,
而不是构造函数。析构不会自动注销,需要显式调用:

```ts
await consumer.deregister();
```

`consumer.close()` 只停止本地使用,保留服务端注册。范围遍历会在 `close()` 后
自然结束;空闲长轮询超时会自动跳过。

不用异步迭代时也可以手动拉取:

```ts
const message = await consumer.next();
if (message !== null) {
  await message.ack();
}
```

长轮询默认 25 秒,服务端会把等待时间限制在 60 秒以内。未确认之前重复 `next()`
返回同一条事件,所以重试就是“不 ack 再取”。

## 查询与管理

```ts
console.log(await client.health());

const events = await client.events({
  limit: 50,       // <= 0 返回全部活跃事件
  offset: 0,       // 从最新一页开始
  type: "note",    // 可选类型过滤
});

const stats = await client.stats();        // 全部消费者
const own = await consumer.stats();        // 当前消费者
const failures = await client.failures();

await client.register("debug", ["note"]);
await client.deregister("debug");
```

## 错误

```ts
import {
  ConflictError,
  HttpError,
  TimeoutError,
  TransportError,
  YiError,
} from "./yi.ts";

try {
  await client.register("classifier", ["other.type"]);
} catch (error) {
  if (error instanceof ConflictError) {
    console.error(error.status, error.body);   // HTTP 409
  } else if (error instanceof HttpError) {
    console.error(error.status);                // 其他非 2xx
  } else if (error instanceof TimeoutError) {
    console.error("request timed out");
  } else if (error instanceof TransportError) {
    console.error(error.message);               // 连接失败
  } else if (error instanceof YiError) {
    console.error(error.message);               // JSON/协议错误
  }
}
```

- `Client` 构造后不可变,可以在多个异步任务之间共享。
- `Consumer` 是服务端注册的本地句柄,不要在多个任务中并发拉取同一个消费者。
- 所有网络错误、超时和 JSON 解析错误都会抛出异常,不会静默返回空数据。

## 测试

```sh
# 不需要服务端
node --test lib/ts/tests/unit.test.mjs

# 需要先启动 Yi 服务
YI_BASE_URL=http://127.0.0.1:8081 \
  node --test lib/ts/tests/e2e.test.mjs
```

单元测试注入假的 `fetch`,覆盖请求路径、超时、409、错误响应和异步迭代。
端到端测试使用唯一类型和消费者名,可以跑在已有数据的数据库上。
