# Yi TypeScript 客户端

零运行时依赖的 TypeScript 客户端,整个实现只有一个文件:`yi.ts`。

- 使用标准 `fetch`,可运行在浏览器、Node 18+、Deno、Bun。
- 同时支持生产者、拉取式消费者和查询/管理接口。
- TypeScript 类型完整,`fetch` 可注入(便于测试、代理和自定义 agent)。
- 支持可选的 Bearer token 认证。
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

### 正则订阅

`types` 是精确匹配;需要按模式订阅时,在 `ConsumerOptions.patterns` 中传入
Go RE2 正则。两者按并集匹配,任意一个命中都会投递:

```ts
const consumer = await Consumer.open(client, "mail-notes", ["raw.captured"], {
  patterns: ["^mail\\.", "^note$"],
});
```

也可以只按模式订阅,此时 `types` 传空数组:

```ts
const mailOnly = await Consumer.open(client, "mail-only", [], {
  patterns: ["^mail\\."],
});
```

- 正则按事件类型逐个匹配;默认**非锚定**(服务端使用 Go 的 `MatchString`),
  需要整体匹配时自行加 `^`/`$`。
- `types` 和 `patterns` 至少有一个非空;只传模式时 `types` 传 `[]`。
- 服务端会 trim、排序、去重;非法或空模式返回 HTTP 400。
- Go RE2 不支持反向引用和环视(lookaround)。
- `client.register(name, types, patterns)` 的第三个参数同样可选;
  `client.stats()` / `consumer.stats()` 返回的 `patterns` 只在服务端保存了
  模式时出现。

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
await client.register("mail-debug", [], ["^mail\\."]);
await client.deregister("debug");
```

## 认证

服务端配置了 `auth-token` 后,所有服务端口请求都必须带
`Authorization: Bearer <token>`。默认不发送认证头;在 `ClientOptions` 中传入
`token` 即可:

```ts
const client = new Client("http://127.0.0.1:8081", {
  token: process.env.YI_TOKEN,
});

await client.capture("带认证的一句");
```

`token` 对 health、发布、查询、消费者注册/长轮询/ack/nack 等所有请求生效,
包括 `Consumer` 内部发起的请求。服务端未配置 token 时省略该选项,行为与之前
完全一致。

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

# 服务端启用 auth-token 时同时提供 token
YI_BASE_URL=http://127.0.0.1:8081 YI_TOKEN=secret \
  node --test lib/ts/tests/e2e.test.mjs
```

单元测试注入假的 `fetch`,覆盖请求路径、超时、409、错误响应、异步迭代以及
认证头;端到端测试使用唯一类型和消费者名,可以跑在已有数据的数据库上。
