#!/usr/bin/env python3
"""Yi 压力测试:并发生产者 + 并发消费者。

用法:
    python3 lib/python/stress/stress.py --producers 4 --consumers 3 \\
        --events-per-producer 250

校验项:
    - 去重后的事件总数 == producers × events-per-producer
    - 每个消费者收到的事件数 == 期望值,且没有重复
    - 服务端 delivered / failed 与客户端确认次数一致
    - 注入的重复发布全部被识别
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yi import Client, Consumer, Event  # noqa: E402


@dataclass
class ProducerResult:
    published: int = 0
    duplicate_hits: int = 0
    errors: List[str] = field(default_factory=list)
    elapsed: float = 0.0


@dataclass
class ConsumerResult:
    name: str = ""
    received: int = 0
    acked: int = 0
    nacked: int = 0
    duplicates: int = 0
    latencies: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    elapsed: float = 0.0


def run_producer(args: argparse.Namespace, index: int, run_id: str,
                 start: threading.Event, result: ProducerResult) -> None:
    client = Client(args.base_url, timeout=args.http_timeout)
    start.wait()
    began = time.monotonic()

    try:
        for i in range(args.events_per_producer):
            types = ["raw.captured"]
            if args.multi_type_rate > 0 and random.random() < args.multi_type_rate:
                types.append("note")
            event = Event.new(
                source="stress-p%d" % index,
                types=types,
                data={
                    "text": ("stress payload %d " % i) + "x" * args.payload_padding,
                    "t0": time.time(),
                    "seq": i,
                    "run": run_id,
                },
            )
            event = replace(event, id="%s-p%d-%d" % (run_id, index, i))

            res = client.publish(event)
            result.published += 1
            if res.duplicate:
                result.errors.append("unexpected duplicate: " + event.id)

            if args.duplicate_every and i % args.duplicate_every == 0:
                again = client.publish(event)
                if again.duplicate:
                    result.duplicate_hits += 1
                else:
                    result.errors.append("duplicate not detected: " + event.id)
    except Exception as exc:  # noqa: BLE001 - 压测要记录任何失败
        result.errors.append(repr(exc))
    finally:
        result.elapsed = time.monotonic() - began


def run_consumer(args: argparse.Namespace, index: int, run_id: str,
                 expected: int, start: threading.Event,
                 deadline: float, result: ConsumerResult) -> None:
    name = "stress-%s-c%d" % (run_id, index)
    result.name = name
    client = Client(args.base_url, timeout=max(args.http_timeout, args.timeout + 5))

    consumer: Optional[Consumer] = None
    try:
        consumer = Consumer(client, name, ["raw.captured"], timeout=args.timeout)
        start.wait()
        began = time.monotonic()
        rng = random.Random((hash(run_id) ^ (index * 7919)) & 0x7FFFFFFF)
        seen = set()

        while result.received < expected:
            if time.monotonic() > deadline:
                result.errors.append(
                    "deadline: %d/%d consumed" % (result.received, expected))
                break

            message = consumer.next()
            if message is None:
                continue

            event = message.event
            if event.id in seen:
                result.duplicates += 1
            else:
                seen.add(event.id)
            result.received += 1

            t0 = event.data.get("t0") if isinstance(event.data, dict) else None
            if (isinstance(t0, (int, float)) and isinstance(event.data, dict)
                    and event.data.get("run") == run_id):
                result.latencies.append(time.time() - float(t0))

            if args.nack_rate > 0 and rng.random() < args.nack_rate:
                message.nack("stress-nack")
                result.nacked += 1
            else:
                message.ack()
                result.acked += 1

        result.elapsed = time.monotonic() - began
    except Exception as exc:  # noqa: BLE001
        result.errors.append(repr(exc))
    finally:
        if consumer is not None and args.keep:
            consumer.close()


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((p / 100.0) * (len(ordered) - 1)))
    return ordered[min(len(ordered) - 1, max(0, idx))]


def fmt_ms(seconds: float) -> str:
    return "%.0fms" % (seconds * 1000.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Yi stress test")
    parser.add_argument("--base-url", default="http://127.0.0.1:8081")
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--events-per-producer", type=int, default=250)
    parser.add_argument("--nack-rate", type=float, default=0.01)
    parser.add_argument("--duplicate-every", type=int, default=50,
                        help="每 N 条重复发布一次,0 表示关闭")
    parser.add_argument("--payload-padding", type=int, default=64)
    parser.add_argument("--multi-type-rate", type=float, default=0.0,
                        help="额外附加 note 类型的比例")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="消费者长轮询秒数")
    parser.add_argument("--http-timeout", type=float, default=15.0)
    parser.add_argument("--deadline", type=float, default=180.0,
                        help="整场压测的最长秒数")
    parser.add_argument("--keep", action="store_true",
                        help="结束后保留消费者注册(默认注销)")
    parser.add_argument("--json", metavar="PATH", help="把结果写到 JSON 文件")
    args = parser.parse_args()

    if args.producers < 1 or args.consumers < 1 or args.events_per_producer < 1:
        parser.error("producers / consumers / events-per-producer 必须为正数")

    run_id = "%d-%d" % (int(time.time()), os.getpid())
    expected = args.producers * args.events_per_producer
    injected = 0
    if args.duplicate_every > 0:
        injected = ((args.events_per_producer - 1) // args.duplicate_every + 1) * args.producers

    client = Client(args.base_url, timeout=args.http_timeout)

    # 新消费者会从游标 0 开始,因此也会读到本轮之前的历史事件。
    baseline = len(client.events(limit=0))
    expected_total = baseline + expected

    deadline = time.monotonic() + args.deadline
    start = threading.Event()

    producer_results = [ProducerResult() for _ in range(args.producers)]
    consumer_results = [ConsumerResult() for _ in range(args.consumers)]

    threads: List[threading.Thread] = []
    for i in range(args.consumers):
        t = threading.Thread(
            target=run_consumer,
            args=(args, i, run_id, expected_total, start, deadline, consumer_results[i]),
            daemon=True,
        )
        threads.append(t)
        t.start()
    for i in range(args.producers):
        t = threading.Thread(
            target=run_producer,
            args=(args, i, run_id, start, producer_results[i]),
            daemon=True,
        )
        threads.append(t)
        t.start()

    began = time.monotonic()
    start.set()
    for t in threads:
        t.join()
    wall = time.monotonic() - began

    # ---- 校验 ----
    problems: List[str] = []

    total = len(client.events(limit=0))
    # 运行期间归档会把水位以下的旧事件移出活跃集合,total 可能小于 expected_total。
    if total > expected_total:
        problems.append("active total = %d > expected %d" % (total, expected_total))
    archived = max(0, expected_total - total)

    stats = {s.name: s for s in client.stats()}
    for r in consumer_results:
        s = stats.get(r.name)
        if s is None:
            problems.append("consumer %s missing from stats" % r.name)
            continue
        if r.received != expected_total:
            problems.append("%s received %d/%d" % (r.name, r.received, expected_total))
        if r.duplicates:
            problems.append("%s saw %d duplicate deliveries" % (r.name, r.duplicates))
        if s.delivered != r.acked:
            problems.append("%s server delivered %d != client acked %d"
                            % (r.name, s.delivered, r.acked))
        if s.failed != r.nacked:
            problems.append("%s server failed %d != client nacked %d"
                            % (r.name, s.failed, r.nacked))
        if s.pending != 0:
            problems.append("%s pending %d != 0" % (r.name, s.pending))
        if r.errors:
            problems.extend("%s: %s" % (r.name, e) for e in r.errors)

    for i, r in enumerate(producer_results):
        if r.published != args.events_per_producer:
            problems.append("producer %d published %d/%d"
                            % (i, r.published, args.events_per_producer))
        if r.errors:
            problems.extend("producer %d: %s" % (i, e) for e in r.errors)

    dup_hits = sum(r.duplicate_hits for r in producer_results)
    if dup_hits != injected:
        problems.append("duplicate hits = %d, want %d" % (dup_hits, injected))

    # ---- 汇总 ----
    publish_elapsed = max((r.elapsed for r in producer_results), default=0.0)
    consume_elapsed = max((r.elapsed for r in consumer_results), default=0.0)
    latencies = [v for r in consumer_results for v in r.latencies]
    acks = sum(r.acked + r.nacked for r in consumer_results)

    print("事件:      活跃 %d 条(存量 %d + 本轮 %d,runtime 归档约 %d)"
          % (total, baseline, expected, archived))
    print("生产者:    %d × %d" % (args.producers, args.events_per_producer))
    print("消费者:    %d(每个都消费全量)" % args.consumers)
    print("发布:      %.2fs  %.0f ev/s" % (publish_elapsed, expected / max(publish_elapsed, 1e-9)))
    print("消费:      %.2fs  %.0f ack/s  %.0f ev/s/消费者"
          % (consume_elapsed, acks / max(consume_elapsed, 1e-9),
             expected / max(consume_elapsed, 1e-9)))
    print("端到端延迟: p50 %s  p95 %s  p99 %s"
          % (fmt_ms(percentile(latencies, 50)),
             fmt_ms(percentile(latencies, 95)),
             fmt_ms(percentile(latencies, 99))))
    print("重复发布:  注入 %d,识别 %d" % (injected, dup_hits))
    print("Nack:      %d" % sum(r.nacked for r in consumer_results))
    print("总耗时:    %.2fs" % wall)

    if problems:
        print("\n校验失败:")
        for p in problems:
            print("  - " + p)
    else:
        print("\n校验: PASS")

    payload = {
        "run_id": run_id,
        "expected": expected,
        "total": total,
        "wall": wall,
        "publish_elapsed": publish_elapsed,
        "consume_elapsed": consume_elapsed,
        "duplicate_injected": injected,
        "duplicate_hits": dup_hits,
        "producers": [r.__dict__ for r in producer_results],
        "consumers": [
            {k: v for k, v in r.__dict__.items() if k != "latencies"}
            | {"latency_p50": percentile(r.latencies, 50),
               "latency_p95": percentile(r.latencies, 95)}
            for r in consumer_results
        ],
        "problems": problems,
    }
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        print("结果已写入 %s" % args.json)

    if not args.keep:
        for r in consumer_results:
            try:
                client.deregister(r.name)
            except Exception:  # noqa: BLE001
                pass

    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
