import assert from "node:assert/strict";
import test from "node:test";

import {
  Client,
  ConflictError,
  Consumer,
  HttpError,
  TimeoutError,
  TransportError,
  YiError,
  createEvent,
  normalizeTypes,
} from "../yi.ts";

function jsonResponse(status, value) {
  return {
    status,
    async text() {
      return JSON.stringify(value);
    },
  };
}

function textResponse(status, text = "") {
  return {
    status,
    async text() {
      return text;
    },
  };
}

function queuedClient(queue, options = {}) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url, init });
    const next = queue.shift();
    if (next === undefined) {
      throw new Error(`unexpected request: ${init.method} ${url}`);
    }
    return next;
  };
  return {
    client: new Client("http://example.test/", { ...options, fetch: fetchImpl }),
    calls,
  };
}

test("normalizeTypes trims, dedupes and sorts", () => {
  assert.deepEqual(normalizeTypes([" b ", "a", "a", ""]), ["a", "b"]);
});

test("createEvent generates id, time and normalized types", () => {
  const event = createEvent({
    source: "phone",
    types: ["note", "raw.captured", "note", "  "],
    data: { text: "hello" },
  });

  assert.equal(event.id.length, 32);
  assert.match(event.id, /^[0-9a-f]{32}$/);
  assert.equal(event.source, "phone");
  assert.deepEqual(event.types, ["note", "raw.captured"]);
  assert.match(event.time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
  assert.deepEqual(event.data, { text: "hello" });
  assert.throws(() => createEvent({ source: "" }), YiError);
  assert.throws(() => createEvent({ source: "s", types: [] }), YiError);
  assert.throws(() => createEvent(null), YiError);
});

test("client builds requests and parses responses", async () => {
  const event = createEvent({
    id: "e1",
    source: "phone",
    types: ["note"],
    time: "2026-11-15T10:00:00Z",
    data: { text: "hi" },
  });
  const stats = {
    name: "c",
    types: ["note"],
    cursor: 1,
    pending: 0,
    delivered: 1,
    failed: 0,
    last_event: "e1",
    last_at: "2026-11-15T10:00:00Z",
  };

  const { client, calls } = queuedClient([
    textResponse(200, "ok\n"),
    jsonResponse(201, { duplicate: false, matched: 1 }),
    jsonResponse(201, { duplicate: false, matched: 0 }),
    jsonResponse(200, [event]),
    jsonResponse(200, [stats]),
    jsonResponse(200, [
      {
        consumer: "c",
        event_id: "e1",
        source: "phone",
        error: "boom",
        at: "2026-11-15T10:00:00Z",
      },
    ]),
    jsonResponse(201, stats),
    textResponse(204),
    jsonResponse(200, { event }),
    textResponse(204),
    textResponse(204),
  ]);

  assert.equal(await client.health(), "ok");
  assert.equal(calls[0].url, "http://example.test/healthz");
  assert.equal(calls[0].init.method, "GET");
  assert.equal(calls[0].init.headers.get("accept"), "application/json");
  assert.equal(calls[0].init.headers.get("authorization"), null);

  const published = await client.publish(event);
  assert.deepEqual(published, { duplicate: false, matched: 1 });
  assert.equal(calls[1].init.method, "POST");
  assert.equal(calls[1].url, "http://example.test/events");
  assert.equal(calls[1].init.headers.get("content-type"), "application/json");
  assert.deepEqual(JSON.parse(calls[1].init.body), event);

  const captured = await client.capture("hello", {
    source: "laptop",
    types: ["note"],
    data: { tag: "x" },
  });
  assert.equal(captured.matched, 0);
  const capturedBody = JSON.parse(calls[2].init.body);
  assert.equal(capturedBody.source, "laptop");
  assert.deepEqual(capturedBody.types, ["note"]);
  assert.deepEqual(capturedBody.data, { tag: "x", text: "hello" });

  const events = await client.events({
    limit: 10,
    offset: 5,
    type: "note",
  });
  assert.deepEqual(events, [event]);
  const eventsUrl = new URL(calls[3].url);
  assert.equal(eventsUrl.pathname, "/events");
  assert.equal(eventsUrl.searchParams.get("limit"), "10");
  assert.equal(eventsUrl.searchParams.get("offset"), "5");
  assert.equal(eventsUrl.searchParams.get("type"), "note");

  assert.deepEqual(await client.stats(), [stats]);
  assert.equal((await client.failures())[0].error, "boom");

  const registered = await client.register("a b", ["note", "note"]);
  assert.equal(registered.name, "c");
  assert.equal(calls[6].init.method, "PUT");
  assert.equal(calls[6].url, "http://example.test/consumers/a%20b");
  assert.deepEqual(JSON.parse(calls[6].init.body), { types: ["note"] });

  assert.equal(await client.next("c", { timeoutMs: 100 }), null);
  assert.equal(
    calls[7].url,
    "http://example.test/consumers/c/next?timeout=0.100",
  );

  assert.deepEqual(await client.next("c", { timeoutMs: 100 }), event);

  await client.ack("c", "e1");
  assert.deepEqual(JSON.parse(calls[9].init.body), { id: "e1" });

  await client.nack("c", "boom", "e1");
  assert.deepEqual(JSON.parse(calls[10].init.body), {
    error: "boom",
    id: "e1",
  });
});

test("client sends the bearer token on every request", async () => {
  const stats = {
    name: "c",
    types: ["note"],
    cursor: 0,
    pending: 0,
    delivered: 0,
    failed: 0,
  };
  const event = createEvent({
    id: "e1",
    source: "phone",
    types: ["note"],
    time: "2026-11-15T10:00:00Z",
    data: {},
  });

  const { client, calls } = queuedClient(
    [
      textResponse(200, "ok\n"), // health
      jsonResponse(201, { duplicate: false, matched: 0 }), // publish
      jsonResponse(201, { duplicate: false, matched: 0 }), // capture
      jsonResponse(200, [event]), // events
      jsonResponse(200, [stats]), // stats
      jsonResponse(200, []), // failures
      jsonResponse(201, stats), // register
      textResponse(204), // next
      textResponse(204), // ack
      textResponse(204), // nack
      textResponse(204), // deregister
    ],
    { token: "s3cret" },
  );

  await client.health();
  await client.publish(event);
  await client.capture("hello");
  await client.events();
  await client.stats();
  await client.failures();
  await client.register("c", ["note"]);
  assert.equal(await client.next("c", { timeoutMs: 100 }), null);
  await client.ack("c", "e1");
  await client.nack("c", "boom", "e1");
  await client.deregister("c");

  assert.equal(calls.length, 11);
  for (const call of calls) {
    assert.equal(
      call.init.headers.get("authorization"),
      "Bearer s3cret",
      `${call.init.method} ${call.url} did not carry the bearer token`,
    );
  }

  const empty = queuedClient([textResponse(200, "ok\n")], { token: "  " });
  await empty.client.health();
  assert.equal(empty.calls[0].init.headers.get("authorization"), null);

  assert.throws(
    () => new Client("http://example.test", { token: "a\r\nX-Evil: 1" }),
    YiError,
  );
});

test("HTTP errors carry status and body", async () => {
  const { client } = queuedClient([
    jsonResponse(409, { error: "consumer exists with different types" }),
    textResponse(500, "boom"),
  ]);

  await assert.rejects(client.register("c", ["note"]), (error) => {
    assert.ok(error instanceof ConflictError);
    assert.ok(error instanceof HttpError);
    assert.equal(error.status, 409);
    assert.match(error.message, /consumer exists with different types/);
    assert.equal(error.body, '{"error":"consumer exists with different types"}');
    return true;
  });

  await assert.rejects(client.health(), (error) => {
    assert.ok(error instanceof HttpError);
    assert.equal(error.status, 500);
    assert.match(error.message, /boom/);
    return true;
  });
});

test("transport and timeout failures are distinct", async () => {
  const transport = new Client("http://example.test", {
    fetch: async () => {
      throw new TypeError("connection refused");
    },
  });
  await assert.rejects(transport.health(), (error) => {
    assert.ok(error instanceof TransportError);
    assert.match(error.message, /connection refused/);
    return true;
  });

  const timeout = new Client("http://example.test", {
    timeoutMs: 10,
    fetch: (_url, init) =>
      new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => {
          reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
        });
      }),
  });
  await assert.rejects(timeout.health(), TimeoutError);
});

test("consumer open, iteration, stats and deregistration", async () => {
  const stats = {
    name: "c",
    types: ["note"],
    cursor: 1,
    pending: 0,
    delivered: 1,
    failed: 0,
  };
  const event = createEvent({
    id: "e1",
    source: "phone",
    types: ["note"],
    time: "2026-11-15T10:00:00Z",
    data: { text: "hi" },
  });

  let nextCalls = 0;
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url, init });
    if (init.method === "PUT") {
      return jsonResponse(201, stats);
    }
    if (url.includes("/next?")) {
      nextCalls += 1;
      return nextCalls === 1
        ? jsonResponse(200, { event })
        : textResponse(204);
    }
    if (url.endsWith("/ack")) {
      return textResponse(204);
    }
    if (url.endsWith("/stats")) {
      return jsonResponse(200, [stats]);
    }
    if (init.method === "DELETE") {
      return textResponse(204);
    }
    throw new Error(`unexpected request: ${init.method} ${url}`);
  };

  const client = new Client("http://example.test", { fetch: fetchImpl });
  const consumer = await Consumer.open(client, "c", ["note"]);

  const seen = [];
  for await (const message of consumer) {
    seen.push(message.event.id);
    await message.ack();
    consumer.close();
  }

  assert.deepEqual(seen, ["e1"]);
  assert.equal(consumer.isClosed, true);
  await assert.rejects(consumer.next(), YiError);
  assert.equal((await consumer.stats()).name, "c");
  await consumer.deregister();
});

test("consumer open registers patterns", async () => {
  const stats = {
    name: "p",
    types: [],
    patterns: ["^mail\\.", "^note$"],
    cursor: 0,
    pending: 0,
    delivered: 0,
    failed: 0,
  };
  const calls = [];
  const client = new Client("http://example.test", {
    fetch: async (url, init) => {
      calls.push({ url, init });
      if (init.method === "PUT") {
        return jsonResponse(201, stats);
      }
      throw new Error(`unexpected request: ${init.method} ${url}`);
    },
  });

  const consumer = await Consumer.open(client, "p", [], {
    patterns: ["^note$", "^mail\\.", "^mail\\."],
  });

  assert.deepEqual(consumer.types, []);
  assert.deepEqual(consumer.patterns, ["^mail\\.", "^note$"]);
  assert.equal(calls[0].url, "http://example.test/consumers/p");
  assert.deepEqual(JSON.parse(calls[0].init.body), {
    types: [],
    patterns: ["^mail\\.", "^note$"],
  });

  await assert.rejects(Consumer.open(client, "empty", [], {}), YiError);
});

test("client.register sends patterns and parses stats patterns", async () => {
  const stats = {
    name: "c",
    types: ["note"],
    patterns: ["^mail\\.", "^note$"],
    cursor: 0,
    pending: 0,
    delivered: 0,
    failed: 0,
  };
  const { client, calls } = queuedClient([
    jsonResponse(201, stats),
    jsonResponse(201, {
      name: "p",
      types: [],
      patterns: ["^mail\\."],
      cursor: 0,
      pending: 0,
      delivered: 0,
      failed: 0,
    }),
    jsonResponse(200, [
      stats,
      {
        name: "legacy",
        types: ["raw.captured"],
        cursor: 0,
        pending: 0,
        delivered: 0,
        failed: 0,
      },
    ]),
  ]);

  const registered = await client.register("c", ["note"], [
    "^note$",
    "^mail\\.",
    "^mail\\.",
  ]);
  assert.deepEqual(JSON.parse(calls[0].init.body), {
    types: ["note"],
    patterns: ["^mail\\.", "^note$"],
  });
  assert.deepEqual(registered.patterns, ["^mail\\.", "^note$"]);

  const patternsOnly = await client.register("p", [], ["^mail\\."]);
  assert.deepEqual(JSON.parse(calls[1].init.body), {
    types: [],
    patterns: ["^mail\\."],
  });
  assert.deepEqual(patternsOnly.types, []);
  assert.deepEqual(patternsOnly.patterns, ["^mail\\."]);

  const all = await client.stats();
  assert.deepEqual(all[0].patterns, ["^mail\\.", "^note$"]);
  assert.equal(all[1].patterns, undefined);

  await assert.rejects(client.register("empty", [], []), YiError);
  await assert.rejects(client.register("blank", [" "], ["  "]), YiError);
});

test("consumer registration propagates conflicts", async () => {
  const client = new Client("http://example.test", {
    fetch: async () => jsonResponse(409, { error: "exists" }),
  });

  await assert.rejects(Consumer.open(client, "c", ["note"]), ConflictError);
});
