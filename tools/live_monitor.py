#!/usr/bin/env python3
import argparse
import http.client
import random
import socket
import statistics
import threading
import time
from collections import deque


def parse_stat_payload(payload: bytes):
    text = payload.decode(errors="replace")
    result = {}
    for line in text.replace("#", "\n").splitlines():
        if "::" not in line:
            continue
        key, value = line.split("::", 1)
        key = key.strip()
        value = value.strip()
        try:
            result[key] = int(value)
        except ValueError:
            try:
                result[key] = float(value)
            except ValueError:
                result[key] = value
    return result


def parse_http_latency_ms(headers):
    values = {k: v for k, v in headers}
    try:
        arrival = float(values["Stat-Req-Arrival"].lstrip(":"))
        dispatch = float(values["Stat-Req-Dispatch"].lstrip(":"))
    except (KeyError, ValueError):
        return None
    return max(0.0, (dispatch - arrival) * 1000.0)


class SharedState:
    def __init__(self, thread_count: int):
        self.lock = threading.Lock()
        self.thread_stats = {tid: {} for tid in range(1, thread_count + 1)}
        self.http_latencies_ms = deque(maxlen=500)
        self.http_ok = 0
        self.http_err = 0



def load_worker(host, port, stop_event: threading.Event, state: SharedState, sleep_s: float):
    targets = [
        ("GET", "/home.html"),
        ("GET", "/favicon.ico"),
        ("GET", "/output.cgi?0.1"),
        ("POST", "/home.html"),
    ]
    while not stop_event.is_set():
        method, path = random.choice(targets)
        try:
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request(method, path)
            resp = conn.getresponse()
            _ = resp.read()
            latency_ms = parse_http_latency_ms(resp.getheaders())
            with state.lock:
                state.http_ok += 1
                if latency_ms is not None:
                    state.http_latencies_ms.append(latency_ms)
            conn.close()
        except Exception:
            with state.lock:
                state.http_err += 1
        if sleep_s > 0:
            time.sleep(sleep_s)


def udp_monitor(host, udp_port, thread_count, stop_event: threading.Event, state: SharedState):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.15)
    try:
        while not stop_event.is_set():
            for tid in range(1, thread_count + 1):
                try:
                    sock.sendto(str(tid).encode(), (host, udp_port))
                    payload, _ = sock.recvfrom(4096)
                    parsed = parse_stat_payload(payload)
                    with state.lock:
                        state.thread_stats[tid] = parsed
                except Exception:
                    continue
            time.sleep(0.2)
    finally:
        sock.close()


def print_dashboard(stop_event: threading.Event, state: SharedState, refresh_s: float):
    while not stop_event.is_set():
        with state.lock:
            latencies = list(state.http_latencies_ms)
            http_ok = state.http_ok
            http_err = state.http_err
            stats = dict(state.thread_stats)

        if latencies:
            avg = statistics.fmean(latencies)
            p95 = sorted(latencies)[int(0.95 * (len(latencies) - 1))]
        else:
            avg = 0.0
            p95 = 0.0

        print("\033[2J\033[H", end="")
        print("NetPulse Live Monitor")
        print(f"HTTP OK: {http_ok} | HTTP Errors: {http_err} | Latency Avg/P95 (ms): {avg:.2f}/{p95:.2f}")
        print("-" * 78)
        print(f"{'Thread':<8}{'Count':<10}{'Static':<10}{'Dynamic':<10}{'POST':<10}")
        print("-" * 78)
        for tid in sorted(stats.keys()):
            row = stats[tid]
            print(
                f"{tid:<8}"
                f"{row.get('Stat-Thread-Count', '-'): <10}"
                f"{row.get('Stat-Thread-Static', '-'): <10}"
                f"{row.get('Stat-Thread-Dynamic', '-'): <10}"
                f"{row.get('Stat-Thread-Post', '-'): <10}"
            )
        print("\nPress Ctrl+C to stop.")
        time.sleep(refresh_s)


def main():
    parser = argparse.ArgumentParser(description="Live UDP telemetry monitor and HTTP load generator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp-port", type=int, required=True)
    parser.add_argument("--udp-port", type=int, required=True)
    parser.add_argument("--threads", type=int, required=True, help="Server worker thread count")
    parser.add_argument("--clients", type=int, default=8, help="Concurrent load-generator clients")
    parser.add_argument("--client-delay", type=float, default=0.05, help="Delay between client requests")
    parser.add_argument("--refresh", type=float, default=0.5, help="Dashboard refresh interval")
    args = parser.parse_args()

    stop_event = threading.Event()
    state = SharedState(args.threads)

    workers = [
        threading.Thread(
            target=load_worker,
            args=(args.host, args.tcp_port, stop_event, state, args.client_delay),
            daemon=True,
        )
        for _ in range(args.clients)
    ]
    monitor = threading.Thread(
        target=udp_monitor,
        args=(args.host, args.udp_port, args.threads, stop_event, state),
        daemon=True,
    )
    dashboard = threading.Thread(
        target=print_dashboard,
        args=(stop_event, state, args.refresh),
        daemon=True,
    )

    try:
        for w in workers:
            w.start()
        monitor.start()
        dashboard.start()

        while True:
            time.sleep(0.25)
    except KeyboardInterrupt:
        stop_event.set()
        for w in workers:
            w.join(timeout=1)
        monitor.join(timeout=1)
        dashboard.join(timeout=1)


if __name__ == "__main__":
    main()
