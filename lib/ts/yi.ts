/**
 * Zero-dependency TypeScript client for the Yi event service.
 *
 * One file, two roles:
 *  - `Client`   — producer and admin calls against the service port.
 *  - `Consumer` — pull-based consumer with explicit ack/nack.
 *
 * The client uses the standard `fetch` API, so it runs in browsers, Node 18+,
 * Deno, Bun and similar runtimes. There is no runtime dependency.
 *
 * Node can execute this file directly on Node 23.6+ (type stripping);
 * Node 22.6–23.5 needs --experimental-strip-types, and older runtimes can
 * compile it with `tsc`.
 */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}

export interface Event {
  id: string;
  source: string;
  types: string[];
  time: string;
  data: JsonObject;
}

export interface NewEvent {
  id?: string;
  source: string;
  types?: readonly string[];
  time?: string;
  data?: JsonObject;
}

export interface PublishResult {
  duplicate: boolean;
  matched: number;
}

export interface ConsumerStats {
  name: string;
  types: string[];
  /** Regex subscription patterns reported by the server, when any. */
  patterns?: string[];
  cursor: number;
  pending: number;
  delivered: number;
  failed: number;
  last_error?: string | null;
  last_event?: string | null;
  last_at?: string | null;
}

export interface Failure {
  consumer: string;
  event_id: string;
  source: string;
  error: string;
  at: string;
}

export interface EventsQuery {
  /** Number of events to return. `<= 0` returns every matching event. */
  limit?: number;
  /** Offset from the newest page. */
  offset?: number;
  /** Optional event-type filter. Empty/undefined disables it. */
  type?: string;
}

export interface CaptureOptions {
  source?: string;
  types?: readonly string[];
  data?: JsonObject;
}

export interface ClientOptions {
  /** Timeout for ordinary requests, in milliseconds. Default 10000. */
  timeoutMs?: number;
  /** Default long-poll wait for `Client.next`, in milliseconds. Default 25000. */
  pollTimeoutMs?: number;
  /** Custom fetch implementation (useful for tests, proxies and agents). */
  fetch?: typeof fetch;
  /** Extra HTTP headers for every request. */
  headers?: Record<string, string>;
  /**
   * Optional bearer token. When set, every request carries
   * `Authorization: Bearer <token>`.
   */
  token?: string;
}

export interface ConsumerOptions {
  /** Long-poll wait for `Consumer.next`, in milliseconds. */
  pollTimeoutMs?: number;
  /**
   * Go RE2 patterns matched against each event type. Combined with `types`
   * using union semantics; either may be empty, but not both.
   */
  patterns?: readonly string[];
}

// ---- errors ---------------------------------------------------------------

export class YiError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class HttpError extends YiError {
  readonly status: number;
  readonly body: string;

  constructor(message: string, status: number, body: string) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export class ConflictError extends HttpError {}

export class TransportError extends YiError {}

export class TimeoutError extends TransportError {}

// ---- public helpers -------------------------------------------------------

/** Trim, drop empties, dedupe and sort event types. */
export function normalizeTypes(types: readonly string[]): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const raw of types) {
    const type = String(raw).trim();
    if (type === "" || seen.has(type)) {
      continue;
    }
    seen.add(type);
    normalized.push(type);
  }
  normalized.sort();
  return normalized;
}

/** Build an event with a random 32-hex id and a UTC RFC3339 timestamp. */
export function createEvent(input: NewEvent): Event {
  if (!isRecord(input)) {
    throw new YiError("event must be an object");
  }
  const source = typeof input.source === "string" ? input.source : "";
  if (source.trim() === "") {
    throw new YiError("event source is required");
  }

  const types = normalizeTypes(input.types ?? ["raw.captured"]);
  if (types.length === 0) {
    throw new YiError("event needs at least one type");
  }

  const data = input.data ?? {};
  if (!isRecord(data)) {
    throw new YiError("event data must be a JSON object");
  }

  return {
    id: typeof input.id === "string" && input.id !== "" ? input.id : randomId(),
    source,
    types,
    time:
      typeof input.time === "string" && input.time !== ""
        ? input.time
        : utcNow(),
    data: data as JsonObject,
  };
}

/** Current time as UTC RFC3339 with second precision. */
export function utcNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** 32 hexadecimal characters, matching the server's Python client. */
export function randomId(): string {
  const bytes = new Uint8Array(16);
  const cryptoApi = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoApi && typeof cryptoApi.getRandomValues === "function") {
    cryptoApi.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  const hex = Array.from(bytes, (byte) =>
    byte.toString(16).padStart(2, "0"),
  );
  return hex.join("");
}

// ---- parsing helpers ------------------------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asJsonObject(value: unknown, what: string): JsonObject {
  if (!isRecord(value)) {
    throw new YiError(`${what} must be a JSON object`);
  }
  return value as JsonObject;
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function numberValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function optionalString(value: unknown): string | null | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (value === null) {
    return null;
  }
  return undefined;
}

function parseStringArray(
  raw: unknown,
  what: string,
): string[] | undefined {
  if (raw === undefined || raw === null) {
    return undefined;
  }
  if (!Array.isArray(raw)) {
    throw new YiError(`${what} must be a JSON array`);
  }
  const items: string[] = [];
  for (const item of raw) {
    if (typeof item === "string") {
      items.push(item);
    }
  }
  return items;
}

function parseJson(text: string, what: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch (error) {
    throw new YiError(`${what}: response is not valid JSON`, { cause: error });
  }
}

function parseJsonArray(text: string, what: string): unknown[] {
  const parsed = parseJson(text, what);
  if (!Array.isArray(parsed)) {
    throw new YiError(`${what}: expected a JSON array`);
  }
  return parsed;
}

function parseEvent(value: unknown): Event {
  if (!isRecord(value)) {
    throw new YiError("event must be a JSON object");
  }

  const types: string[] = [];
  const rawTypes = value.types;
  if (rawTypes !== undefined && rawTypes !== null) {
    if (!Array.isArray(rawTypes)) {
      throw new YiError("event types must be a JSON array");
    }
    for (const item of rawTypes) {
      if (typeof item === "string") {
        types.push(item);
      }
    }
  } else if (typeof value.type === "string") {
    types.push(value.type);
  }

  const rawData = value.data;
  const data: JsonObject =
    rawData === undefined || rawData === null
      ? {}
      : asJsonObject(rawData, "event data");

  return {
    id: stringValue(value.id),
    source: stringValue(value.source),
    types: normalizeTypes(types),
    time: stringValue(value.time),
    data,
  };
}

function parsePublishResult(value: unknown): PublishResult {
  if (!isRecord(value)) {
    throw new YiError("publish result must be a JSON object");
  }
  return {
    duplicate: value.duplicate === true,
    matched: numberValue(value.matched),
  };
}

function parseConsumerStats(value: unknown): ConsumerStats {
  if (!isRecord(value)) {
    throw new YiError("consumer stats must be a JSON object");
  }

  const types = parseStringArray(value.types, "consumer stats types") ?? [];
  const patterns = parseStringArray(
    value.patterns,
    "consumer stats patterns",
  );

  const stats: ConsumerStats = {
    name: stringValue(value.name),
    types,
    cursor: numberValue(value.cursor),
    pending: numberValue(value.pending),
    delivered: numberValue(value.delivered),
    failed: numberValue(value.failed),
  };

  if (patterns !== undefined) {
    stats.patterns = patterns;
  }

  const lastError = optionalString(value.last_error);
  if (lastError !== undefined) {
    stats.last_error = lastError;
  }
  const lastEvent = optionalString(value.last_event);
  if (lastEvent !== undefined) {
    stats.last_event = lastEvent;
  }
  const lastAt = optionalString(value.last_at);
  if (lastAt !== undefined) {
    stats.last_at = lastAt;
  }
  return stats;
}

function parseFailure(value: unknown): Failure {
  if (!isRecord(value)) {
    throw new YiError("failure must be a JSON object");
  }
  return {
    consumer: stringValue(value.consumer),
    event_id: stringValue(value.event_id),
    source: stringValue(value.source),
    error: stringValue(value.error),
    at: stringValue(value.at),
  };
}

// ---- URL and HTTP helpers -------------------------------------------------

interface HttpResponse {
  status: number;
  body: string;
}

function integerOption(value: number | undefined, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(0, Math.trunc(value));
}

function eventsPath(query: EventsQuery): string {
  const params = new URLSearchParams();
  params.set("limit", String(integerOption(query.limit, 50)));
  params.set("offset", String(integerOption(query.offset, 0)));
  const type = typeof query.type === "string" ? query.type.trim() : "";
  if (type !== "") {
    params.set("type", type);
  }
  return `/events?${params.toString()}`;
}

function consumerPath(name: string, suffix = ""): string {
  return `/consumers/${encodeURIComponent(name)}${suffix}`;
}

function formatSeconds(milliseconds: number): string {
  const millis = Math.max(1, Math.round(milliseconds));
  const seconds = Math.floor(millis / 1000);
  const remainder = millis % 1000;
  return `${seconds}.${String(remainder).padStart(3, "0")}`;
}

function nextPath(name: string, timeoutMs: number): string {
  return `${consumerPath(name)}/next?timeout=${formatSeconds(timeoutMs)}`;
}

function requireName(name: string): string {
  if (typeof name !== "string" || name.trim() === "") {
    throw new YiError("consumer name is required");
  }
  return name;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isAbortError(error: unknown): boolean {
  return isRecord(error) && error.name === "AbortError";
}

function errorDetail(body: string): string {
  const text = body.trim();
  if (text === "") {
    return "";
  }
  try {
    const parsed = JSON.parse(text) as unknown;
    if (isRecord(parsed) && typeof parsed.error === "string") {
      return parsed.error;
    }
  } catch {
    // The body is not JSON; return it verbatim.
  }
  return text;
}

function expectStatus(
  response: HttpResponse,
  accepted: readonly number[],
  method: string,
  url: string,
): void {
  if (accepted.includes(response.status)) {
    return;
  }

  const detail =
    errorDetail(response.body) || `HTTP ${response.status}`;
  const message = `${method} ${url} failed with HTTP ${response.status}: ${detail}`;
  if (response.status === 409) {
    throw new ConflictError(message, response.status, response.body);
  }
  throw new HttpError(message, response.status, response.body);
}

// ---- client ---------------------------------------------------------------

export class Client {
  readonly baseUrl: string;
  readonly requestTimeoutMs: number;
  readonly pollTimeoutMs: number;

  private readonly fetchImpl: typeof fetch;
  private readonly defaultHeaders: Record<string, string>;

  constructor(
    baseUrl = "http://127.0.0.1:8081",
    options: ClientOptions = {},
  ) {
    if (typeof baseUrl !== "string") {
      throw new YiError("base URL must be a string");
    }
    this.baseUrl = baseUrl.trim().replace(/\/+$/, "");
    if (this.baseUrl === "") {
      throw new YiError("base URL is required");
    }

    const requestTimeoutMs = options.timeoutMs ?? 10000;
    if (
      !Number.isFinite(requestTimeoutMs) ||
      requestTimeoutMs <= 0
    ) {
      throw new YiError("request timeout must be positive");
    }
    this.requestTimeoutMs = requestTimeoutMs;

    const pollTimeoutMs = options.pollTimeoutMs ?? 25000;
    if (!Number.isFinite(pollTimeoutMs) || pollTimeoutMs <= 0) {
      throw new YiError("poll timeout must be positive");
    }
    this.pollTimeoutMs = pollTimeoutMs;

    const fetchImpl =
      options.fetch ??
      (typeof globalThis.fetch === "function"
        ? globalThis.fetch.bind(globalThis)
        : undefined);
    if (typeof fetchImpl !== "function") {
      throw new YiError(
        "fetch is not available; pass options.fetch explicitly",
      );
    }
    this.fetchImpl = fetchImpl;

    const token = options.token;
    if (token !== undefined && typeof token !== "string") {
      throw new YiError("auth token must be a string");
    }
    if (token !== undefined && (token.includes("\r") || token.includes("\n"))) {
      throw new YiError("auth token must not contain CR or LF");
    }
    const bearer: Record<string, string> =
      token !== undefined && token.trim() !== ""
        ? { Authorization: `Bearer ${token}` }
        : {};

    this.defaultHeaders = {
      Accept: "application/json",
      ...bearer,
      ...(options.headers ?? {}),
    };
  }

  async health(): Promise<string> {
    const path = "/healthz";
    const response = await this.request(
      "GET",
      path,
      undefined,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200], "GET", this.baseUrl + path);
    return response.body.trim();
  }

  async publish(event: Event | NewEvent): Promise<PublishResult> {
    const prepared = createEvent(event);
    const path = "/events";
    const response = await this.request(
      "POST",
      path,
      prepared,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200, 201], "POST", this.baseUrl + path);
    return parsePublishResult(parseJson(response.body, "publish"));
  }

  async capture(
    text: string,
    options: CaptureOptions = {},
  ): Promise<PublishResult> {
    if (typeof text !== "string") {
      throw new YiError("capture text must be a string");
    }
    if (options.data !== undefined && !isRecord(options.data)) {
      throw new YiError("capture data must be a JSON object");
    }

    const data: JsonObject = {
      ...(options.data ?? {}),
      text,
    };
    return this.publish({
      source: options.source ?? "typescript",
      types: options.types ?? ["raw.captured"],
      data,
    });
  }

  async events(query: EventsQuery = {}): Promise<Event[]> {
    const path = eventsPath(query);
    const response = await this.request(
      "GET",
      path,
      undefined,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200], "GET", this.baseUrl + path);
    return parseJsonArray(response.body, "events").map(parseEvent);
  }

  async stats(): Promise<ConsumerStats[]> {
    const path = "/stats";
    const response = await this.request(
      "GET",
      path,
      undefined,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200], "GET", this.baseUrl + path);
    return parseJsonArray(response.body, "stats").map(parseConsumerStats);
  }

  async failures(): Promise<Failure[]> {
    const path = "/failures";
    const response = await this.request(
      "GET",
      path,
      undefined,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200], "GET", this.baseUrl + path);
    return parseJsonArray(response.body, "failures").map(parseFailure);
  }

  /**
   * Register a consumer with exact types and/or Go RE2 patterns.
   *
   * Patterns match each event type (unanchored unless `^`/`$` are used) and
   * are combined with `types` using union semantics. At least one non-empty
   * type or pattern is required. Patterns are only added to the request body
   * when non-empty.
   */
  async register(
    name: string,
    types: readonly string[],
    patterns: readonly string[] = [],
  ): Promise<ConsumerStats> {
    const consumer = requireName(name);
    const normalized = normalizeTypes(types);
    // Patterns follow the same trim/dedupe/sort rules as types.
    const normalizedPatterns = normalizeTypes(patterns);
    if (normalized.length === 0 && normalizedPatterns.length === 0) {
      throw new YiError("consumer needs at least one type or pattern");
    }

    const body: { types: string[]; patterns?: string[] } = {
      types: normalized,
    };
    if (normalizedPatterns.length > 0) {
      body.patterns = normalizedPatterns;
    }

    const path = consumerPath(consumer);
    const response = await this.request(
      "PUT",
      path,
      body,
      this.requestTimeoutMs,
    );
    expectStatus(response, [200, 201], "PUT", this.baseUrl + path);
    return parseConsumerStats(parseJson(response.body, "register"));
  }

  async deregister(name: string): Promise<void> {
    const consumer = requireName(name);
    const path = consumerPath(consumer);
    const response = await this.request(
      "DELETE",
      path,
      undefined,
      this.requestTimeoutMs,
    );
    expectStatus(response, [204], "DELETE", this.baseUrl + path);
  }

  async next(
    name: string,
    options: { timeoutMs?: number } = {},
  ): Promise<Event | null> {
    const consumer = requireName(name);
    const timeoutMs = options.timeoutMs ?? this.pollTimeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new YiError("poll timeout must be positive");
    }

    const path = nextPath(consumer, timeoutMs);
    const socketTimeout = Math.max(
      this.requestTimeoutMs,
      timeoutMs + 5000,
    );
    const response = await this.request(
      "GET",
      path,
      undefined,
      socketTimeout,
    );

    if (response.status === 204) {
      return null;
    }
    expectStatus(response, [200], "GET", this.baseUrl + path);

    const parsed = parseJson(response.body, "next");
    if (!isRecord(parsed) || !("event" in parsed)) {
      throw new YiError("next: response does not contain an event");
    }
    return parseEvent(parsed.event);
  }

  async ack(name: string, eventId?: string): Promise<void> {
    const consumer = requireName(name);
    const body: { id?: string } = {};
    if (typeof eventId === "string" && eventId !== "") {
      body.id = eventId;
    }

    const path = consumerPath(consumer, "/ack");
    const response = await this.request(
      "POST",
      path,
      body,
      this.requestTimeoutMs,
    );
    expectStatus(response, [204], "POST", this.baseUrl + path);
  }

  async nack(
    name: string,
    error: string,
    eventId?: string,
  ): Promise<void> {
    const consumer = requireName(name);
    const body: { error: string; id?: string } = {
      error: typeof error === "string" ? error : String(error),
    };
    if (typeof eventId === "string" && eventId !== "") {
      body.id = eventId;
    }

    const path = consumerPath(consumer, "/nack");
    const response = await this.request(
      "POST",
      path,
      body,
      this.requestTimeoutMs,
    );
    expectStatus(response, [204], "POST", this.baseUrl + path);
  }

  private async request(
    method: string,
    path: string,
    body: unknown,
    timeoutMs: number,
  ): Promise<HttpResponse> {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new YiError("request timeout must be positive");
    }

    let payload: string | undefined;
    if (body !== undefined) {
      try {
        payload = JSON.stringify(body);
      } catch (error) {
        throw new YiError("cannot encode request body", { cause: error });
      }
    }

    const url = this.baseUrl + path;
    const headers = new Headers(this.defaultHeaders);
    if (payload !== undefined && !headers.has("content-type")) {
      headers.set("content-type", "application/json");
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await this.fetchImpl(url, {
        method,
        headers,
        body: payload,
        signal: controller.signal,
      });
      const text = await response.text();
      return { status: response.status, body: text };
    } catch (error) {
      if (error instanceof YiError) {
        throw error;
      }
      if (isAbortError(error)) {
        throw new TimeoutError(
          `${method} ${url} timed out after ${timeoutMs}ms`,
          { cause: error },
        );
      }
      throw new TransportError(
        `${method} ${url} failed: ${errorMessage(error)}`,
        { cause: error },
      );
    } finally {
      clearTimeout(timer);
    }
  }
}

// ---- message --------------------------------------------------------------

export class Message {
  readonly event: Event;
  readonly consumerName: string;

  private readonly client: Client;

  constructor(client: Client, consumerName: string, event: Event) {
    this.client = client;
    this.consumerName = consumerName;
    this.event = event;
  }

  async ack(): Promise<void> {
    await this.client.ack(this.consumerName, this.event.id);
  }

  async nack(error: unknown): Promise<void> {
    const text = errorMessage(error);
    await this.client.nack(this.consumerName, text, this.event.id);
  }
}

// ---- consumer -------------------------------------------------------------

export class Consumer implements AsyncIterable<Message> {
  readonly name: string;
  readonly types: ReadonlyArray<string>;
  readonly patterns: ReadonlyArray<string>;
  readonly pollTimeoutMs: number;

  private readonly client: Client;
  private closed = false;

  private constructor(
    client: Client,
    name: string,
    types: readonly string[],
    options: ConsumerOptions,
  ) {
    this.client = client;
    this.name = requireName(name);
    this.types = normalizeTypes(types);
    this.patterns = normalizeTypes(options.patterns ?? []);
    if (this.types.length === 0 && this.patterns.length === 0) {
      throw new YiError("consumer needs at least one type or pattern");
    }

    const pollTimeoutMs = options.pollTimeoutMs ?? client.pollTimeoutMs;
    if (!Number.isFinite(pollTimeoutMs) || pollTimeoutMs <= 0) {
      throw new YiError("poll timeout must be positive");
    }
    this.pollTimeoutMs = pollTimeoutMs;
  }

  /**
   * Register `name` on the server and return a local consumer.
   *
   * Pass regex patterns through `options.patterns`; they are combined with
   * `types` using union semantics and either list may be empty (but not both).
   *
   * Construction is async because the server registration is an HTTP call.
   */
  static async open(
    client: Client,
    name: string,
    types: readonly string[],
    options: ConsumerOptions = {},
  ): Promise<Consumer> {
    const consumer = new Consumer(client, name, types, options);
    await client.register(consumer.name, consumer.types, consumer.patterns);
    return consumer;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  async next(
    options: { timeoutMs?: number } = {},
  ): Promise<Message | null> {
    if (this.closed) {
      throw new YiError(`consumer '${this.name}' is closed`);
    }

    const event = await this.client.next(this.name, {
      timeoutMs: options.timeoutMs ?? this.pollTimeoutMs,
    });
    if (event === null) {
      return null;
    }
    return new Message(this.client, this.name, event);
  }

  /** Stop local use. The server registration is left untouched. */
  close(): void {
    this.closed = true;
  }

  /** Delete the server registration and stop local use. */
  async deregister(): Promise<void> {
    await this.client.deregister(this.name);
    this.closed = true;
  }

  /** Current stats for this consumer. */
  async stats(): Promise<ConsumerStats> {
    const all = await this.client.stats();
    const found = all.find((item) => item.name === this.name);
    if (found === undefined) {
      throw new YiError(`consumer '${this.name}' not found in stats`);
    }
    return found;
  }

  /**
   * Iterate until `close()`.
   *
   * Idle long-poll timeouts are skipped, exactly like the Python client.
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<Message, void, void> {
    while (!this.closed) {
      const message = await this.next();
      if (message !== null) {
        yield message;
      }
    }
  }
}
