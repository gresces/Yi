#!/usr/bin/env python3
"""模型驱动的逻辑测试:对照本地模型检查游标、pending、去重、分页与失败记录。

建议对空数据库运行:
    ./dist/yi -frontend=false -data /tmp/yi-logic
    python3 lib/python/stress/logic_check.py --base-url http://127.0.0.1:8081
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yi import Client, Consumer, Event  # noqa: E402

TYPE_POOL = ["raw.captured", "note", "mail.received", "link.saved"]


@dataclass
class Model:
    name: str
    types: Set[str]
    pos: int = -1          # 最后一个已确认事件在 events 里的下标
    delivered: int = 0
    failed: int = 0

    def pending(self, events: List[Dict[str, Any]]) -> int:
        return sum(
            1
            for i, event in enumerate(events)
            if i > self.pos and self.types & set(event["types"])
        )

    def next_index(self, events: List[Dict[str, Any]]) -> Optional[int]:
        for i, event in enumerate(events):
            if i > self.pos and self.types & set(event["types"]):
                return i
        return None


def http_json(base: str, path: str, token: str = "") -> Any:
    request = urllib.request.Request(base + path)
    if token:
        request.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def fetch_events_page(
    base: str, limit: int, offset: int, typ: str = "", token: str = ""
) -> Dict[str, Any]:
    query = {"limit": str(limit), "offset": str(offset)}
    if typ:
        query["type"] = typ
    return http_json(base, "/api/events?" + urllib.parse.urlencode(query), token)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Yi model-based logic check")
    parser.add_argument("--base-url", default="http://127.0.0.1:8081")
    parser.add_argument("--frontend-url", default="http://127.0.0.1:8080")
    parser.add_argument("--token", default=os.environ.get("YI_TOKEN", ""),
                        help="Bearer 认证令牌,默认读 YI_TOKEN")
    parser.add_argument("--events", type=int, default=300)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    run_id = "logic-%d" % os.getpid()
    client = Client(args.base_url, timeout=10, token=args.token)

    baseline = len(client.events(limit=0))
    check(baseline == 0, "logic check expects an empty database, found %d events" % baseline)

    # ---- 生成事件,10% 注入重复 ----
    events: List[Dict[str, Any]] = []
    duplicates = 0
    for i in range(args.events):
        if events and rng.random() < 0.10:
            source_event = events[rng.randrange(len(events))]
            result = client.publish(
                Event(
                    id=source_event["id"],
                    source=source_event["source"],
                    types=tuple(source_event["types"]),
                    time=source_event["time"],
                    data=source_event["data"],
                )
            )
            check(result.duplicate, "duplicate publish was not detected: %s" % source_event["id"])
            duplicates += 1
            continue

        count = rng.choice([1, 1, 1, 2, 3])
        types = sorted(rng.sample(TYPE_POOL, count))
        event = Event.new(
            source="logic-p%d" % (i % 4),
            types=types,
            data={"text": "event %d" % i},
        )
        event = Event(
            id="%s-e%d" % (run_id, i),
            source=event.source,
            types=tuple(event.types),
            time=datetime.now(timezone.utc).isoformat(),
            data=event.data,
        )
        result = client.publish(event)
        check(not result.duplicate, "unexpected duplicate: %s" % event.id)
        events.append(
            {
                "id": event.id,
                "source": event.source,
                "types": list(event.types),
                "time": event.time,
                "data": event.data,
            }
        )

    # ---- 订阅与模型 ----
    models: List[Model] = []
    consumers: Dict[str, Consumer] = {}
    for i in range(args.consumers):
        count = rng.choice([1, 1, 2])
        types = set(rng.sample(TYPE_POOL, count))
        name = "%s-c%d" % (run_id, i)
        consumers[name] = Consumer(client, name, sorted(types), timeout=1)
        models.append(Model(name=name, types=types))

    def verify_all(label: str) -> None:
        stats = {s.name: s for s in client.stats()}
        for model in models:
            actual = stats[model.name]
            check(actual.delivered == model.delivered,
                  "%s: %s delivered %d != %d" % (label, model.name, actual.delivered, model.delivered))
            check(actual.failed == model.failed,
                  "%s: %s failed %d != %d" % (label, model.name, actual.failed, model.failed))
            pending = model.pending(events)
            check(actual.pending == pending,
                  "%s: %s pending %d != %d" % (label, model.name, actual.pending, pending))

    # ---- 多轮处理 ----
    for round_index in range(args.rounds):
        for model in models:
            available = sum(
                1 for i in range(model.pos + 1, len(events))
                if model.types & set(events[i]["types"])
            )
            if available == 0:
                continue
            take = rng.randint(0, min(available, 12))
            consumer = consumers[model.name]

            for _ in range(take):
                expected_index = model.next_index(events)
                check(expected_index is not None, "model expected no more events")
                expected = events[expected_index]

                message = consumer.next()
                check(message is not None, "consumer timed out with pending events")
                check(message.event.id == expected["id"],
                      "order mismatch: got %s want %s" % (message.event.id, expected["id"]))
                check(set(message.event.types) & model.types,
                      "delivered event does not match consumer types")

                if rng.random() < 0.15:
                    message.nack("logic-nack")
                    model.failed += 1
                else:
                    message.ack()
                    model.delivered += 1
                model.pos = expected_index

            verify_all("round %d / %s" % (round_index, model.name))

        # 每轮之后重新核对一次全部消费者
        verify_all("round %d" % round_index)

    # ---- 去重与总数 ----
    all_events = client.events(limit=0)
    check(len(all_events) == len(events),
          "event total %d != model %d" % (len(all_events), len(events)))
    check([e.id for e in all_events] == [e["id"] for e in events], "event order mismatch")

    # ---- 分页 ----
    offset = 0
    collected: List[str] = []
    while True:
        page = fetch_events_page(args.frontend_url, 50, offset, token=args.token)
        check(page["total"] == len(events), "page total mismatch")
        if not page["events"]:
            break
        collected = [event["id"] for event in page["events"]] + collected
        offset += len(page["events"])
    check(collected == [e["id"] for e in events], "pagination mismatch")

    # ---- 类型筛选 ----
    for typ in TYPE_POOL:
        expected = [e["id"] for e in events if typ in e["types"]]
        page = fetch_events_page(args.frontend_url, 200, 0, typ, token=args.token)
        check(page["total"] == len(expected),
              "filter %s total %d != %d" % (typ, page["total"], len(expected)))
        collected = []
        offset = 0
        while True:
            page = fetch_events_page(args.frontend_url, 200, offset, typ, token=args.token)
            if not page["events"]:
                break
            collected = [event["id"] for event in page["events"]] + collected
            offset += len(page["events"])
        check(collected == expected, "filter %s pagination mismatch" % typ)

    # ---- 失败记录 ----
    failures = client.failures()
    expected_failures = sum(m.failed for m in models)
    check(len(failures) == expected_failures,
          "failure total %d != %d" % (len(failures), expected_failures))
    check(all(f.consumer.startswith(run_id) for f in failures), "unexpected failure consumer")

    for model in models:
        consumers[model.name].deregister()

    print("OK  events=%d duplicates=%d consumers=%d failures=%d"
          % (len(events), duplicates, len(models), expected_failures))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print("FAIL:", exc)
        sys.exit(1)
