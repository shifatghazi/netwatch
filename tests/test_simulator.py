#!/usr/bin/env python3
"""
test_simulator.py
-----------------
Unit tests for the telemetry simulator and dashboard API logic.
Run with: python3 -m pytest tests/test_simulator.py -v
"""

import json
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# ── Simulator tests ───────────────────────────────────────────────────────

from telemetry_simulator import generate_tick, NORMAL, ANOMALY_PROFILES, sample


class TestTelemetrySimulator:

    def test_normal_tick_has_required_fields(self):
        tick = generate_tick("test-node", None)
        assert "timestamp" in tick
        assert "node_id" in tick
        assert "metrics" in tick
        assert tick["anomaly_injected"] is None

    def test_normal_tick_metrics_in_range(self):
        """Normal traffic should produce values near the baseline."""
        latencies = [generate_tick("n1", None)["metrics"]["latency_ms"] for _ in range(100)]
        avg = sum(latencies) / len(latencies)
        # Average should be within 3ms of expected mean (12ms)
        assert abs(avg - NORMAL["latency_ms"]["mean"]) < 3.0, \
            f"Expected latency ~12ms, got {avg:.2f}ms"

    def test_anomaly_tick_has_elevated_metrics(self):
        """Anomaly traffic should be significantly different from normal."""
        anomaly_latencies = [
            generate_tick("n1", "latency_spike")["metrics"]["latency_ms"]
            for _ in range(50)
        ]
        normal_latencies = [
            generate_tick("n1", None)["metrics"]["latency_ms"]
            for _ in range(50)
        ]
        avg_anomaly = sum(anomaly_latencies) / len(anomaly_latencies)
        avg_normal  = sum(normal_latencies) / len(normal_latencies)
        # Anomaly should be at least 5x higher latency
        assert avg_anomaly > avg_normal * 5, \
            f"Anomaly latency ({avg_anomaly:.1f}) not much higher than normal ({avg_normal:.1f})"

    def test_packet_storm_high_utilization(self):
        frames = [generate_tick("n1", "packet_storm") for _ in range(20)]
        avg_util = sum(f["metrics"]["link_util_pct"] for f in frames) / 20
        assert avg_util > 80, f"Packet storm should have high utilization, got {avg_util:.1f}%"

    def test_all_metric_values_non_negative(self):
        """All metrics should be >= 0 — no negative latency, etc."""
        for anomaly in [None, "latency_spike", "packet_storm", "link_degradation"]:
            for _ in range(20):
                tick = generate_tick("n1", anomaly)
                m = tick["metrics"]
                assert m["latency_ms"] >= 0
                assert m["packet_loss_pct"] >= 0
                assert m["jitter_ms"] >= 0
                assert 0 <= m["link_util_pct"] <= 100

    def test_tick_json_serializable(self):
        """Every tick should serialize cleanly to JSON."""
        tick = generate_tick("test-node-01", "latency_spike")
        try:
            json.dumps(tick)
        except (TypeError, ValueError) as e:
            pytest.fail(f"Tick not JSON serializable: {e}")

    def test_node_id_preserved(self):
        node = "node-custom-99"
        tick = generate_tick(node, None)
        assert tick["node_id"] == node


# ── Dashboard API tests ───────────────────────────────────────────────────

from dashboard_api import app as flask_app, SEVERITY_ORDER


@pytest.fixture
def client(tmp_path):
    """Create a test client with temp JSONL files."""
    alerts_file    = tmp_path / "alerts.jsonl"
    telemetry_file = tmp_path / "telemetry.jsonl"

    # Write some test alerts
    test_alerts = [
        {"timestamp": "2025-01-01T00:00:01Z", "node_id": "node-a", "metric": "latency_ms",
         "observed_value": 180.0, "z_score": 5.2, "severity": "CRITICAL", "description": "latency spike"},
        {"timestamp": "2025-01-01T00:00:02Z", "node_id": "node-b", "metric": "packet_loss_pct",
         "observed_value": 15.0, "z_score": 4.1, "severity": "MAJOR", "description": "packet storm"},
        {"timestamp": "2025-01-01T00:00:03Z", "node_id": "node-a", "metric": "jitter_ms",
         "observed_value": 8.5, "z_score": 3.1, "severity": "MINOR", "description": "jitter elevated"},
    ]
    with open(alerts_file, "w") as f:
        for a in test_alerts:
            f.write(json.dumps(a) + "\n")

    flask_app.config["TESTING"] = True
    import dashboard_api
    dashboard_api.ALERTS_FILE    = str(alerts_file)
    dashboard_api.TELEMETRY_FILE = str(telemetry_file)

    with flask_app.test_client() as c:
        yield c


class TestDashboardAPI:

    def test_health_endpoint(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        data = r.get_json()
        assert data["ok"] is True

    def test_get_all_alerts(self, client):
        r = client.get("/api/alerts")
        assert r.status_code == 200
        data = r.get_json()
        assert data["total"] == 3

    def test_filter_by_node(self, client):
        r = client.get("/api/alerts?node=node-a")
        data = r.get_json()
        assert all(a["node_id"] == "node-a" for a in data["alerts"])
        assert data["total"] == 2

    def test_filter_by_severity(self, client):
        r = client.get("/api/alerts?severity=MAJOR")
        data = r.get_json()
        for alert in data["alerts"]:
            assert SEVERITY_ORDER[alert["severity"]] >= SEVERITY_ORDER["MAJOR"]

    def test_alerts_sorted_newest_first(self, client):
        r = client.get("/api/alerts")
        data = r.get_json()
        timestamps = [a["timestamp"] for a in data["alerts"]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_stats_endpoint(self, client):
        r = client.get("/api/stats")
        assert r.status_code == 200
        data = r.get_json()
        assert data["total_alerts"] == 3
        assert data["by_severity"]["CRITICAL"] == 1
        assert data["by_severity"]["MAJOR"] == 1
        assert set(data["nodes"]) == {"node-a", "node-b"}

    def test_get_nodes(self, client):
        r = client.get("/api/nodes")
        data = r.get_json()
        assert set(data["nodes"]) == {"node-a", "node-b"}
