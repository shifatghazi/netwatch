#!/usr/bin/env python3
"""
telemetry_simulator.py
----------------------
Generates realistic optical network telemetry and writes it to a shared
file that the C++ detection engine reads. In production this would be
replaced by a real SNMP/gNMI collector talking to physical hardware.

Metrics we simulate (all things Ciena/Nokia gear actually reports):
  - latency_ms     : end-to-end round-trip time in milliseconds
  - packet_loss_pct: percentage of dropped packets (0.0 - 100.0)
  - jitter_ms      : variance in packet arrival times
  - link_util_pct  : percentage of link bandwidth in use
  - error_rate     : bit error rate (BER) as a float

Anomaly injection:
  Every ~30 seconds we randomly inject one of three fault scenarios:
    1. Latency spike    - simulates a congested or degraded optical path
    2. Packet storm     - simulates a broadcast storm or routing loop
    3. Link degradation - simulates physical layer issues (dirty fiber, etc.)

Why we write to a file instead of a socket:
  Keeps the simulator decoupled from the C++ engine. The engine can crash
  and restart without losing data. This is the same pattern used in
  real network OSS/BSS pipelines (think Kafka, but lighter weight here).
"""

import json
import math
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone


OUTPUT_FILE = "/tmp/netwatch/telemetry.jsonl"
INTERVAL_SECONDS = 1.0
ANOMALY_PROBABILITY = 0.03   # ~3% chance per tick of injecting a fault


# Baseline "normal" ranges for a healthy optical network link
NORMAL = {
    "latency_ms":      {"mean": 12.0,  "std": 1.5},
    "packet_loss_pct": {"mean": 0.01,  "std": 0.005},
    "jitter_ms":       {"mean": 0.8,   "std": 0.2},
    "link_util_pct":   {"mean": 45.0,  "std": 8.0},
    "error_rate":      {"mean": 1e-9,  "std": 5e-10},
}

ANOMALY_PROFILES = {
    "latency_spike": {
        "latency_ms":      {"mean": 180.0, "std": 30.0},
        "packet_loss_pct": {"mean": 0.5,   "std": 0.1},
        "jitter_ms":       {"mean": 25.0,  "std": 5.0},
        "link_util_pct":   {"mean": 90.0,  "std": 5.0},
        "error_rate":      {"mean": 1e-9,  "std": 5e-10},
    },
    "packet_storm": {
        "latency_ms":      {"mean": 45.0,  "std": 10.0},
        "packet_loss_pct": {"mean": 15.0,  "std": 3.0},
        "jitter_ms":       {"mean": 8.0,   "std": 2.0},
        "link_util_pct":   {"mean": 99.0,  "std": 0.5},
        "error_rate":      {"mean": 1e-8,  "std": 1e-9},
    },
    "link_degradation": {
        "latency_ms":      {"mean": 20.0,  "std": 3.0},
        "packet_loss_pct": {"mean": 2.5,   "std": 0.5},
        "jitter_ms":       {"mean": 3.0,   "std": 1.0},
        "link_util_pct":   {"mean": 44.0,  "std": 8.0},
        "error_rate":      {"mean": 1e-6,  "std": 1e-7},
    },
}


def sample(profile: dict, key: str) -> float:
    """Draw one sample from a Gaussian with the given mean/std, clipped to 0."""
    val = random.gauss(profile[key]["mean"], profile[key]["std"])
    return max(0.0, val)


def generate_tick(node_id: str, anomaly_type: str | None) -> dict:
    profile = ANOMALY_PROFILES[anomaly_type] if anomaly_type else NORMAL
    return {
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "node_id":         node_id,
        "anomaly_injected": anomaly_type,          # null in normal traffic
        "metrics": {
            "latency_ms":      round(sample(profile, "latency_ms"), 3),
            "packet_loss_pct": round(sample(profile, "packet_loss_pct"), 5),
            "jitter_ms":       round(sample(profile, "jitter_ms"), 3),
            "link_util_pct":   round(min(sample(profile, "link_util_pct"), 100.0), 2),
            "error_rate":      sample(profile, "error_rate"),
        }
    }


def run():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    nodes = ["node-toronto-01", "node-ottawa-02", "node-montreal-03"]
    active_anomaly = None
    anomaly_ticks_remaining = 0

    print(f"[simulator] Writing telemetry to {OUTPUT_FILE} at {INTERVAL_SECONDS}s intervals")
    print(f"[simulator] Nodes: {nodes}")
    print(f"[simulator] Anomaly injection probability: {ANOMALY_PROBABILITY*100:.1f}% per tick")

    with open(OUTPUT_FILE, "a") as f:
        while True:
            for node in nodes:
                # Decide if we're injecting a fault this tick
                if anomaly_ticks_remaining > 0:
                    anomaly_ticks_remaining -= 1
                    if anomaly_ticks_remaining == 0:
                        print(f"[simulator] Anomaly '{active_anomaly}' cleared on {node}")
                        active_anomaly = None
                elif random.random() < ANOMALY_PROBABILITY:
                    active_anomaly = random.choice(list(ANOMALY_PROFILES.keys()))
                    anomaly_ticks_remaining = random.randint(5, 20)
                    print(f"[simulator] >>> Injecting '{active_anomaly}' on {node} "
                          f"for {anomaly_ticks_remaining} ticks")

                tick = generate_tick(node, active_anomaly)
                line = json.dumps(tick)
                f.write(line + "\n")
                f.flush()

            time.sleep(INTERVAL_SECONDS)


def handle_signal(sig, frame):
    print("\n[simulator] Shutting down gracefully.")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    run()
