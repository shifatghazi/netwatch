# NetWatch — Real-Time Network Anomaly Detection Engine

A production-grade network monitoring system that detects anomalies in optical
network telemetry using adaptive statistical analysis. Built with a C++ detection
core, gRPC API, and live web dashboard.

```
┌─────────────────┐    JSONL     ┌──────────────────┐    JSONL     ┌─────────────────┐
│  Python         │─────────────▶│  C++ Detection   │─────────────▶│  gRPC Server    │
│  Simulator      │              │  Engine          │              │  (Python)       │
│                 │              │  (inotify + Z-   │              │  port 50051     │
│  Generates      │              │   score rolling  │              │                 │
│  telemetry at   │              │   window)        │              │  Streaming RPC  │
│  1Hz per node   │              │                  │              │  + unary RPC    │
└─────────────────┘              └──────────────────┘              └────────┬────────┘
                                                                            │
                                                                   ┌────────▼────────┐
                                                                   │  Dashboard API  │
                                                                   │  Flask + SSE    │
                                                                   │  port 8080      │
                                                                   └────────┬────────┘
                                                                            │
                                                                   ┌────────▼────────┐
                                                                   │  Web Dashboard  │
                                                                   │  Chart.js       │
                                                                   │  Live alerts    │
                                                                   └─────────────────┘
```

## What it does

- **Ingests** simulated optical network telemetry (latency, packet loss, jitter, link utilization, bit error rate) for three network nodes at 1Hz
- **Detects** anomalies using a rolling-window Z-score algorithm (adaptive thresholding) — no ML training data required
- **Classifies** alerts by severity (WARNING / MINOR / MAJOR / CRITICAL) with metric-specific escalation rules
- **Exposes** a gRPC API with server-streaming support for real-time alert subscriptions
- **Visualizes** live telemetry and alert history on a web dashboard with Chart.js charts

## Tech stack

| Layer | Technology | Why |
|---|---|---|
| Detection engine | C++20 + inotify | Maximum throughput, minimal latency, OS-level I/O efficiency |
| Anomaly algorithm | Rolling Z-score | Adaptive to drifting baselines, deterministic, explainable |
| IPC | JSONL files | Decoupled; engine and API can restart independently |
| API | gRPC + Protobuf | Binary protocol, server-streaming, industry standard in telecom |
| Web API | Flask + SSE | Browser-compatible bridge to the gRPC layer |
| Dashboard | Vanilla JS + Chart.js | No framework overhead, real-time via Server-Sent Events |
| Containerization | Docker + multi-stage | 10x smaller images; reproducible builds |

## Quick start

### Option 1 — Single file demo (no Docker, no compiler needed)
```bash
git clone https://github.com/shifatghazi/netwatch
cd netwatch
python3 demo.py
```
Then open **http://localhost:8080** in your browser. After ~30 seconds of baseline warmup, alerts will start firing. Press `Ctrl+C` to stop.

### Option 2 — Full Docker stack (all 4 services as separate containers)
```bash
git clone https://github.com/shifatghazi/netwatch
cd netwatch
docker compose up --build
```
Then open **http://localhost:8080/dashboard/index.html**

### Connect to the gRPC API directly (requires grpcurl)
```bash
grpcurl -plaintext localhost:50051 netwatch.NetWatchService/HealthCheck
grpcurl -plaintext -d '{"min_severity": "MAJOR"}' localhost:50051 netwatch.NetWatchService/StreamAlerts
```

## Run tests

```bash
# C++ unit tests
mkdir -p build && cd build
cmake .. && make test_detector
./test_detector

# Python tests
pip install -r requirements.txt pytest
pytest tests/test_simulator.py -v
```

## Algorithm: rolling Z-score anomaly detection

For each metric on each node, a sliding window of the last 128 samples is maintained. When a new measurement arrives:

```
z = (observed_value - window_mean) / window_stddev
```

If `|z| > threshold` (default 3.0), an alert is emitted. The threshold is calibrated per metric:

- `latency_ms`, `jitter_ms`: threshold = 3.0 (standard three-sigma)
- `link_util_pct`: threshold = 3.5 (higher tolerance — utilization spikes are expected)
- `error_rate`: threshold = 2.5 (more sensitive — BER changes indicate physical layer issues)

Alert severity escalates with z-score magnitude, and packet loss / error rate alerts are bumped one severity level due to their operational impact.

**Why not ML?** Statistical detectors are preferred as a first-pass because they are deterministic (same input = same output), require no training data, and are fully explainable to operations teams. An ML classifier can be added downstream to categorize anomaly *types* after the statistical layer identifies *candidates*.

## Project structure

```
netwatch/
├── src/
│   ├── engine.cpp          # C++ daemon — detection loop, inotify, alert emission
│   ├── detector.hpp        # Rolling Z-score detector (header-only)
│   ├── telemetry.hpp       # Data structures (TelemetryFrame, Alert, Severity)
│   ├── grpc_server.py      # gRPC service implementation
│   └── dashboard_api.py    # Flask REST/SSE bridge for the web dashboard
├── proto/
│   └── netwatch.proto      # Service contract (4 RPCs, 6 message types)
├── scripts/
│   └── telemetry_simulator.py  # Gaussian telemetry generator with fault injection
├── dashboard/
│   └── index.html          # Single-file live dashboard
├── tests/
│   ├── test_detector.cpp   # C++ unit tests (6 test cases)
│   └── test_simulator.py   # Python unit tests (11 test cases)
├── docker/
│   ├── Dockerfile.python   # Python services
│   └── Dockerfile.cpp      # Multi-stage C++ build
├── CMakeLists.txt
├── docker-compose.yml
└── requirements.txt
```

## Key design decisions

**Why inotify instead of polling?** The kernel wakes the engine process only when the telemetry file changes — zero CPU wasted on busy-waiting. This is how production log shippers (Filebeat, Fluent Bit) work.

**Why JSONL instead of a database?** For this scale (3 nodes × 5 metrics × 1Hz), a database is over-engineering. JSONL is human-readable, appendable, and trivially replaceable with Kafka or InfluxDB when the scale demands it.

**Why separate gRPC server and dashboard API?** Browser JS cannot speak native gRPC (the HTTP/2 framing isn't exposed). The Flask layer is a thin translation layer — the same pattern used by Envoy's gRPC-Web transcoding in production.

**Why per-metric detectors?** Latency and packet loss have completely different statistical distributions and operational tolerances. Sharing one detector across metrics would make the thresholds meaningless.
