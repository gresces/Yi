import assert from "node:assert/strict";
import test from "node:test";

import {
  Client,
  ConflictError,
  Consumer,
  createEvent,
} from "../yi.ts";

const baseUrl = process.env.YI_BASE_URL ?? "http://127.0.0.1:8081";
const token = process.env.YI_TOKEN;
const clientOptions = token === undefined || token === "" ? {} : { token };

function uniqueSuffix() {
  return `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function hasType(event, type) {
  return event.types.includes(type);
}

test("TypeScript client end-to-end", async () => {
  const suffix = uniqueSuffix();
  const type = `ts.e2e.${suffix}`;
  const name = `ts-e2e-${suffix}`;
  const client = new Client(baseUrl, clientOptions);

  assert.equal(await client.health(), "ok");

  // Producer: explicit event and duplicate detection.
  const explicit = createEvent({
    source: "ts-lib",
    types: [`${type}.explicit`, "note"],
    data: { text: "explicit event" },
  });
  assert.equal((await client.publish(explicit)).duplicate, false);
  assert.equal((await client.publish(explicit)).duplicate, true);

  // Capture + consumer.
  const captured = await client.capture("hello from TypeScript", {
    source: "ts-lib",
    types: [type],
    data: { tag: "capture" },
  });
  assert.equal(captured.duplicate, false);

  const consumer = await Consumer.open(client, name, [type]);

  let message = await consumer.next();
  assert.ok(message);
  assert.equal(message.event.data.text, "hello from TypeScript");
  assert.equal(message.event.data.tag, "capture");
  assert.ok(hasType(message.event, type));
  await message.ack();

  // Nack records a failure and advances the cursor.
  const nacked = createEvent({
    source: "ts-lib",
    types: [type],
    data: { text: "nack me" },
  });
  assert.equal((await client.publish(nacked)).duplicate, false);

  message = await consumer.next();
  assert.ok(message);
  assert.equal(message.event.id, nacked.id);
  await message.nack(new Error("e2e nack"));

  const own = await consumer.stats();
  assert.equal(own.name, name);
  assert.equal(own.delivered, 1);
  assert.equal(own.failed, 1);
  assert.equal(own.pending, 0);
  assert.equal(own.last_error, "e2e nack");

  // A different type set for an existing name is a 409.
  await assert.rejects(
    Consumer.open(client, name, ["other.type"]),
    (error) => error instanceof ConflictError && error.status === 409,
  );

  // Type filter on the service port.
  const events = await client.events({ limit: 0, type });
  assert.equal(events.length, 2);
  assert.ok(events.every((event) => hasType(event, type)));
  assert.deepEqual(await client.events({ limit: 0, type: `${type}.absent` }), []);

  // Failures are queryable.
  const failures = await client.failures();
  assert.ok(
    failures.some(
      (failure) => failure.consumer === name && failure.error === "e2e nack",
    ),
  );

  // Async iteration.
  const iterType = `${type}.iter`;
  const iterName = `${name}-iter`;
  const iterConsumer = await Consumer.open(client, iterName, [iterType]);
  const iterEvent = createEvent({
    source: "ts-lib",
    types: [iterType],
    data: { text: "iterate" },
  });
  assert.equal((await client.publish(iterEvent)).duplicate, false);

  let seen = 0;
  for await (const item of iterConsumer) {
    seen += 1;
    assert.equal(item.event.id, iterEvent.id);
    await item.ack();
    iterConsumer.close();
  }
  assert.equal(seen, 1);

  // Long-poll timeout: a registered consumer with no matching events.
  const idleName = `${name}-idle`;
  await client.register(idleName, [`${type}.idle`]);
  assert.equal(await client.next(idleName, { timeoutMs: 100 }), null);
  await client.deregister(idleName);

  await consumer.deregister();
  await client.deregister(iterName);
});
