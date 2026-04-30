#!/usr/bin/env python3
"""
dashboard_api.py
----------------
Lightweight Flask REST API that the dashboard frontend polls.
Reads directly from the alerts and telemetry JSONL files.

WHY NOT QUERY THE GRPC SERVER FROM THE DASHBOARD?
  The dashboard is a web browser — browsers can't speak gRPC natively
  (the HTTP/2 framing gRPC uses isn't exposed to browser JS). You'd need
  gRPC-Web or a transcoding proxy (like Envoy). For this project we add
  a thin Flask HTTP layer that the browser CAN talk to. In production
  you'd use Envoy or grpc-gateway. This is a real architecture decision
  you'd make at Nokia/Tesla — mention it if asked.

ENDPOINTS:
  GET /api/alerts         - recent alerts, optional ?node=&severity=&limit=
  GET /api/telemetry      - last N telemetry readings per node
  GET /api/nodes          - list of known nodes
  GET /api/stats          - summary statistics
  GET /api/health         - health check
  GET /events             - Server-Sent Events stream for live updates
"""

import json
import os
import time
import threading
from collections import defaultdict
from flask import Flask, jsonify, request, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow the dashboard HTML to call this API from any origin

ALERTS_FILE    = os.environ.get("ALERTS_FILE",    "/tmp/netwatch/alerts.jsonl")
TELEMETRY_FILE = os.environ.get("TELEMETRY_FILE", "/tmp/netwatch/telemetry.jsonl")

SEVERITY_ORDER = {"WARNING": 0, "MINOR": 1, "MAJOR": 2, "CRITICAL": 3}


def read_jsonl(path: str, limit: int = 0) -> list[dict]:
    """Read a JSONL file. If limit > 0, return only the last N lines."""
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        lines = f.readlines()
    if limit:
        lines = lines[-limit:]
    result = []
    for line in lines:
        line = line.strip()
        if line:
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return result


@app.route("/api/health")
def health():
    return jsonify({
        "ok": True,
        "alerts_file_exists": os.path.exists(ALERTS_FILE),
        "telemetry_file_exists": os.path.exists(TELEMETRY_FILE),
        "timestamp": time.time(),
    })


@app.route("/api/nodes")
def get_nodes():
    alerts = read_jsonl(ALERTS_FILE)
    nodes = sorted({a.get("node_id", "") for a in alerts if a.get("node_id")})
    return jsonify({"nodes": nodes})


@app.route("/api/alerts")
def get_alerts():
    node     = request.args.get("node", "")
    severity = request.args.get("severity", "")
    limit    = int(request.args.get("limit", 200))

    alerts = read_jsonl(ALERTS_FILE)

    if node:
        alerts = [a for a in alerts if a.get("node_id") == node]
    if severity:
        min_sev = SEVERITY_ORDER.get(severity, 0)
        alerts = [a for a in alerts
                  if SEVERITY_ORDER.get(a.get("severity", ""), 0) >= min_sev]

    # Newest first
    alerts.sort(key=lambda a: a.get("timestamp", ""), reverse=True)
    alerts = alerts[:limit]

    return jsonify({
        "alerts": alerts,
        "total": len(alerts),
    })


@app.route("/api/telemetry")
def get_telemetry():
    node  = request.args.get("node", "")
    limit = int(request.args.get("limit", 120))

    frames = read_jsonl(TELEMETRY_FILE, limit=limit * 3)  # read more, then filter

    if node:
        frames = [f for f in frames if f.get("node_id") == node]

    frames = frames[-limit:]
    return jsonify({"frames": frames, "total": len(frames)})


@app.route("/api/stats")
def get_stats():
    alerts    = read_jsonl(ALERTS_FILE)
    telemetry = read_jsonl(TELEMETRY_FILE, limit=300)

    counts_by_severity = defaultdict(int)
    counts_by_node     = defaultdict(int)
    counts_by_metric   = defaultdict(int)

    for a in alerts:
        counts_by_severity[a.get("severity", "UNKNOWN")] += 1
        counts_by_node[a.get("node_id", "unknown")] += 1
        counts_by_metric[a.get("metric", "unknown")] += 1

    # Compute latest metric averages per node from telemetry
    node_latest: dict[str, dict] = {}
    for frame in reversed(telemetry):
        nid = frame.get("node_id", "")
        if nid and nid not in node_latest:
            node_latest[nid] = frame.get("metrics", {})

    return jsonify({
        "total_alerts": len(alerts),
        "by_severity":  dict(counts_by_severity),
        "by_node":      dict(counts_by_node),
        "by_metric":    dict(counts_by_metric),
        "node_latest":  node_latest,
        "nodes":        sorted(node_latest.keys()),
    })


@app.route("/events")
def sse_stream():
    """
    Server-Sent Events endpoint for live dashboard updates.
    The browser opens one long-lived HTTP connection and we push
    new data as it arrives — no polling needed from the client side.
    
    SSE is simpler than WebSockets for one-directional push.
    Format: "data: <json>\n\n"
    """
    def generate():
        last_alert_count = 0
        while True:
            alerts = read_jsonl(ALERTS_FILE)
            if len(alerts) != last_alert_count:
                last_alert_count = len(alerts)
                new_alerts = alerts[-5:]  # send last 5
                data = json.dumps({"type": "alerts", "alerts": new_alerts})
                yield f"data: {data}\n\n"
            else:
                # Heartbeat to keep connection alive
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
            time.sleep(1.0)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    print(f"[dashboard-api] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
