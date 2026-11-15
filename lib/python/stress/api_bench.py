#!/usr/bin/env python3
"""对运行中的 Yi 做 API 延迟基准。"""

from __future__ import annotations

import argparse
import os
import statistics
import time
import urllib.request


def fetch_ms(url: str, timeout: float, token: str = "") -> float:
    request = urllib.request.Request(url)
    if token:
        request.add_header("Authorization", "Bearer " + token)
    start = time.perf_counter()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()
    return (time.perf_counter() - start) * 1000.0


def percentile(values, p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = int(round((p / 100.0) * (len(ordered) - 1)))
    return ordered[min(len(ordered) - 1, max(0, index))]


def bench(label: str, url: str, requests: int, timeout: float,
          token: str = "") -> None:
    for _ in range(min(5, requests)):
        fetch_ms(url, timeout, token)
    samples = [fetch_ms(url, timeout, token) for _ in range(requests)]
    print(
        "%-28s p50 %7.2fms  p95 %7.2fms  p99 %7.2fms  max %7.2fms"
        % (
            label,
            percentile(samples, 50),
            percentile(samples, 95),
            percentile(samples, 99),
            max(samples),
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Yi API latency benchmark")
    parser.add_argument("--frontend-url", default="http://127.0.0.1:8080")
    parser.add_argument("--service-url", default="http://127.0.0.1:8081")
    parser.add_argument("--token", default=os.environ.get("YI_TOKEN", ""),
                        help="Bearer 认证令牌,默认读 YI_TOKEN")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    front = args.frontend_url.rstrip("/")
    service = args.service_url.rstrip("/")

    targets = [
        ("GET /healthz", service + "/healthz"),
        ("GET /events?limit=50", front + "/api/events?limit=50"),
        ("GET /events?limit=200", front + "/api/events?limit=200"),
        ("GET /events?offset=10000", front + "/api/events?limit=50&offset=10000"),
        ("GET /events?type=note", front + "/api/events?limit=50&type=note"),
        ("GET /stats", front + "/api/stats"),
        ("GET /failures?limit=50", front + "/api/failures?limit=50"),
        ("GET /metrics?window=120", front + "/api/metrics?window=120"),
    ]

    for label, url in targets:
        bench(label, url, args.requests, args.timeout, args.token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
