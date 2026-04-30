#!/usr/bin/env python3
"""
NetWatch — All-in-one demo
--------------------------
Run this single file to see the entire project working:

    python3 demo.py

Then open:  http://localhost:8080

What this script does:
  1. Starts a background thread that acts as the 3 network nodes,
     generating telemetry every second (the simulator)
  2. Starts a background thread that reads that telemetry and detects
     anomalies using Z-score math (the detection engine, in Python here
     — in the real project this is the C++ binary)
  3. Starts a web server on port 8080 that serves the dashboard and
     all the API endpoints the dashboard needs

Press Ctrl+C to stop.
"""

import json
import math
import os
import random
import signal
import sys
import threading
import time
import collections
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


# ─────────────────────────────────────────────────────────────────────────────
# SHARED STATE  (in a real multi-process system this would be files or Kafka)
# ─────────────────────────────────────────────────────────────────────────────

# All telemetry frames generated so far (list of dicts)
telemetry_frames = []
telemetry_lock   = threading.Lock()

# All alerts generated so far (list of dicts)
alerts           = []
alerts_lock      = threading.Lock()

# Event that fires when a new alert is added (for SSE streaming)
new_alert_event  = threading.Event()

# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — TELEMETRY SIMULATOR
# Pretends to be 3 network nodes reporting health data every second.
# ─────────────────────────────────────────────────────────────────────────────

NODES = ["node-toronto-01", "node-ottawa-02", "node-montreal-03"]

# What a "healthy" network looks like (mean and standard deviation)
NORMAL_PROFILE = {
    "latency_ms":      {"mean": 12.0,  "std": 1.5},
    "packet_loss_pct": {"mean": 0.01,  "std": 0.005},
    "jitter_ms":       {"mean": 0.8,   "std": 0.2},
    "link_util_pct":   {"mean": 45.0,  "std": 8.0},
    "error_rate":      {"mean": 1e-9,  "std": 5e-10},
}

# What different types of network problems look like
FAULT_PROFILES = {
    "latency_spike": {
        # A congested or degraded optical path — latency shoots up
        "latency_ms":      {"mean": 180.0, "std": 30.0},
        "packet_loss_pct": {"mean": 0.5,   "std": 0.1},
        "jitter_ms":       {"mean": 25.0,  "std": 5.0},
        "link_util_pct":   {"mean": 90.0,  "std": 5.0},
        "error_rate":      {"mean": 1e-9,  "std": 5e-10},
    },
    "packet_storm": {
        # A broadcast storm — link is completely saturated, lots of drops
        "latency_ms":      {"mean": 45.0,  "std": 10.0},
        "packet_loss_pct": {"mean": 15.0,  "std": 3.0},
        "jitter_ms":       {"mean": 8.0,   "std": 2.0},
        "link_util_pct":   {"mean": 99.0,  "std": 0.5},
        "error_rate":      {"mean": 1e-8,  "std": 1e-9},
    },
    "link_degradation": {
        # Dirty fiber or physical layer issue — error rate spikes
        "latency_ms":      {"mean": 20.0,  "std": 3.0},
        "packet_loss_pct": {"mean": 2.5,   "std": 0.5},
        "jitter_ms":       {"mean": 3.0,   "std": 1.0},
        "link_util_pct":   {"mean": 44.0,  "std": 8.0},
        "error_rate":      {"mean": 1e-6,  "std": 1e-7},
    },
}


def gauss_positive(mean, std):
    """Draw from a Gaussian distribution, but never go below 0."""
    return max(0.0, random.gauss(mean, std))


def simulator_thread():
    """
    Runs forever, generating one telemetry reading per node per second.
    Every ~30 seconds it randomly injects a fault on one node.
    """
    active_fault  = None
    fault_ticks   = 0

    print("[simulator] Started — generating telemetry for 3 nodes at 1Hz")

    while True:
        # Decide whether to start or clear a fault
        if fault_ticks > 0:
            fault_ticks -= 1
            if fault_ticks == 0:
                print(f"[simulator] Fault '{active_fault}' cleared")
                active_fault = None
        elif random.random() < 0.03:   # ~3% chance per tick = fault every ~33s
            active_fault = random.choice(list(FAULT_PROFILES.keys()))
            fault_ticks  = random.randint(5, 20)
            print(f"[simulator] >>> Injecting '{active_fault}' for {fault_ticks}s")

        for node in NODES:
            profile = FAULT_PROFILES[active_fault] if active_fault else NORMAL_PROFILE
            frame = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "node_id":   node,
                "fault":     active_fault,   # None during normal operation
                "metrics": {
                    "latency_ms":      round(gauss_positive(profile["latency_ms"]["mean"],
                                                             profile["latency_ms"]["std"]), 3),
                    "packet_loss_pct": round(gauss_positive(profile["packet_loss_pct"]["mean"],
                                                             profile["packet_loss_pct"]["std"]), 5),
                    "jitter_ms":       round(gauss_positive(profile["jitter_ms"]["mean"],
                                                             profile["jitter_ms"]["std"]), 3),
                    "link_util_pct":   round(min(gauss_positive(profile["link_util_pct"]["mean"],
                                                                 profile["link_util_pct"]["std"]), 100.0), 2),
                    "error_rate":      gauss_positive(profile["error_rate"]["mean"],
                                                      profile["error_rate"]["std"]),
                }
            }
            with telemetry_lock:
                telemetry_frames.append(frame)
                # Keep only last 1000 frames in memory
                if len(telemetry_frames) > 1000:
                    telemetry_frames.pop(0)

        time.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — DETECTION ENGINE
# This is what the C++ binary does, implemented here in Python so you can run
# the demo without a compiler. The logic is identical.
# ─────────────────────────────────────────────────────────────────────────────

class RollingZScoreDetector:
    """
    Keeps a rolling window of the last `window_size` values.
    Flags a new value as anomalous if its Z-score exceeds `threshold`.

    Z-score = (value - mean) / stddev
    Tells you: "how many standard deviations is this from normal?"
    Anything above 3.0 happens less than 0.3% of the time by chance.
    """

    def __init__(self, window_size=128, threshold=3.0, min_samples=30):
        self.window      = collections.deque(maxlen=window_size)
        self.threshold   = threshold
        self.min_samples = min_samples

    def push(self, value):
        self.window.append(value)

    def check(self, value):
        """Returns (is_anomaly: bool, z_score: float)"""
        if len(self.window) < self.min_samples:
            return False, 0.0   # Not enough history yet to know what "normal" is

        mean   = sum(self.window) / len(self.window)
        stddev = math.sqrt(sum((x - mean)**2 for x in self.window) / len(self.window))

        if stddev < 1e-10:
            return False, 0.0   # Signal is perfectly constant — nothing to detect against

        z = (value - mean) / stddev
        return abs(z) > self.threshold, z


# One set of detectors per node (each node has its own baseline)
node_detectors = {
    node: {
        "latency_ms":      RollingZScoreDetector(threshold=3.0),
        "packet_loss_pct": RollingZScoreDetector(threshold=3.0),
        "jitter_ms":       RollingZScoreDetector(threshold=3.0),
        "link_util_pct":   RollingZScoreDetector(threshold=3.5),
        "error_rate":      RollingZScoreDetector(threshold=2.5),
    }
    for node in NODES
}


def z_to_severity(z, metric):
    """Map a Z-score to a human-readable severity level."""
    az = abs(z)
    if   az > 5.0: base = "CRITICAL"
    elif az > 4.0: base = "MAJOR"
    elif az > 3.0: base = "MINOR"
    else:          base = "WARNING"

    # Packet loss and error rate are more serious — bump up one level
    if metric in ("packet_loss_pct", "error_rate"):
        order = ["WARNING", "MINOR", "MAJOR", "CRITICAL"]
        idx = order.index(base)
        base = order[min(idx + 1, 3)]

    return base


def detection_engine_thread():
    """
    Runs forever, reading new telemetry frames and checking each metric.
    When a Z-score exceeds the threshold, creates an alert.
    This is exactly what the C++ engine.cpp does — same algorithm.
    """
    last_processed = 0
    print("[engine] Started — watching telemetry for anomalies")

    while True:
        with telemetry_lock:
            new_frames = telemetry_frames[last_processed:]
            last_processed = len(telemetry_frames)

        for frame in new_frames:
            node = frame["node_id"]
            dets = node_detectors[node]

            for metric_name, value in frame["metrics"].items():
                det = dets[metric_name]
                is_anomaly, z = det.check(value)
                det.push(value)

                if is_anomaly:
                    severity = z_to_severity(z, metric_name)
                    alert = {
                        "timestamp":      frame["timestamp"],
                        "node_id":        node,
                        "metric":         metric_name,
                        "observed_value": round(value, 6),
                        "z_score":        round(z, 2),
                        "severity":       severity,
                        "description":    f"{metric_name} = {round(value,4)} "
                                          f"(z={z:.2f}, {abs(z):.1f} sigma from baseline)",
                    }
                    with alerts_lock:
                        alerts.append(alert)
                    new_alert_event.set()
                    new_alert_event.clear()
                    print(f"[engine] {severity:8s} {node} | {metric_name} = {value:.4f} | z={z:.2f}")

        time.sleep(0.1)


# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — WEB SERVER
# Serves the dashboard HTML and all the API endpoints.
# Standard library only — no Flask needed for the demo.
# ─────────────────────────────────────────────────────────────────────────────

SEVERITY_ORDER = {"WARNING": 0, "MINOR": 1, "MAJOR": 2, "CRITICAL": 3}

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NetWatch — Network Anomaly Detection</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root { --bg:#0d1117; --surface:#161b22; --border:#30363d; --text:#e6edf3; --muted:#8b949e;
          --critical:#f85149; --major:#ff7b72; --minor:#e3b341; --warning:#3fb950; --blue:#58a6ff; }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',monospace;font-size:13px}
  header{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;border-bottom:1px solid var(--border);background:var(--surface)}
  header h1{font-size:16px;font-weight:600;color:var(--blue)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--warning);display:inline-block;margin-right:6px;animation:pulse 2s infinite}
  .dot.live{background:var(--warning)}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:16px 24px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 18px}
  .card .lbl{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
  .card .val{font-size:28px;font-weight:700}
  .card.cr .val{color:var(--critical)} .card.mj .val{color:var(--major)}
  .card.mn .val{color:var(--minor)}    .card.tt .val{color:var(--blue)}
  .main{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:0 24px 24px}
  .panel{background:var(--surface);border:1px solid var(--border);border-radius:8px}
  .ph{padding:10px 16px;border-bottom:1px solid var(--border);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);display:flex;justify-content:space-between;align-items:center}
  .pb{padding:12px}
  .chart-wrap{position:relative;height:180px}
  .alerts{max-height:360px;overflow-y:auto}
  .ah{display:grid;grid-template-columns:72px 115px 130px 1fr 60px;gap:8px;align-items:center;padding:6px 14px;background:rgba(255,255,255,.04);font-size:10px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px}
  .ar{display:grid;grid-template-columns:72px 115px 130px 1fr 60px;gap:8px;align-items:center;padding:7px 14px;border-bottom:1px solid var(--border);font-size:12px;animation:fadein .4s ease}
  @keyframes fadein{from{background:rgba(88,166,255,.08)}to{background:transparent}}
  .ar:hover{background:rgba(255,255,255,.03)}
  .badge{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:.5px}
  .CRITICAL{background:rgba(248,81,73,.2);color:var(--critical)}
  .MAJOR{background:rgba(255,123,114,.2);color:var(--major)}
  .MINOR{background:rgba(227,179,65,.2);color:var(--minor)}
  .WARNING{background:rgba(63,185,80,.2);color:var(--warning)}
  .nid{color:var(--blue);font-family:monospace;font-size:11px}
  .met{color:var(--muted)}
  .zs{font-family:monospace;text-align:right}
  .ts{color:var(--muted);font-size:10px}
  .node-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
  .nc{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px}
  .nn{color:var(--blue);font-family:monospace;font-size:11px;margin-bottom:8px}
  .mr{display:flex;justify-content:space-between;margin-bottom:3px}
  .mk{color:var(--muted);font-size:10px}
  .mv{font-family:monospace;font-size:11px}
  .ok{color:var(--warning)} .warn{color:var(--minor)} .crit{color:var(--critical)}
  .two{grid-column:span 2}
  select{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px}
  #warmup{position:fixed;top:0;left:0;right:0;background:rgba(88,166,255,.15);border-bottom:1px solid var(--blue);padding:8px 24px;font-size:12px;color:var(--blue);text-align:center;z-index:99}
</style>
</head>
<body>
<div id="warmup">⏳ Warming up — the detector needs 30 seconds of baseline data before it can fire alerts. Watch the node cards update in real time.</div>
<header>
  <div><span class="dot" id="dot"></span><strong>NetWatch</strong>
  <span style="color:var(--muted);margin-left:8px;font-size:11px">real-time network anomaly detection</span></div>
  <span style="color:var(--muted);font-size:11px" id="ts">connecting...</span>
</header>
<div class="grid">
  <div class="card tt"><div class="lbl">Total alerts</div><div class="val" id="s-total">0</div></div>
  <div class="card cr"><div class="lbl">Critical</div><div class="val" id="s-cr">0</div></div>
  <div class="card mj"><div class="lbl">Major</div><div class="val" id="s-mj">0</div></div>
  <div class="card mn"><div class="lbl">Minor / warning</div><div class="val" id="s-mn">0</div></div>
</div>
<div class="main">
  <div class="panel">
    <div class="ph"><span>Live alert feed</span>
      <select id="sev-f" onchange="render()">
        <option value="">All</option><option value="MINOR">Minor+</option>
        <option value="MAJOR">Major+</option><option value="CRITICAL">Critical</option>
      </select>
    </div>
    <div style="padding:0">
      <div class="ah"><span>Severity</span><span>Time</span><span>Node</span><span>Metric</span><span>Z-score</span></div>
      <div class="alerts" id="al"><div style="padding:20px;text-align:center;color:var(--muted)">Waiting for anomalies… (need ~30s of baseline first)</div></div>
    </div>
  </div>
  <div class="panel">
    <div class="ph">Alerts by metric</div>
    <div class="pb"><div class="chart-wrap"><canvas id="mc"></canvas></div></div>
  </div>
  <div class="panel two">
    <div class="ph">Node telemetry — latest snapshot</div>
    <div class="pb"><div class="node-grid" id="ng">Loading...</div></div>
  </div>
  <div class="panel">
    <div class="ph"><span>Latency over time (ms)</span>
      <select id="nf" onchange="render()"><option value="">All nodes</option></select>
    </div>
    <div class="pb"><div class="chart-wrap"><canvas id="lc"></canvas></div></div>
  </div>
  <div class="panel">
    <div class="ph">Severity breakdown</div>
    <div class="pb"><div class="chart-wrap"><canvas id="sc"></canvas></div></div>
  </div>
</div>
<script>
const CO={animation:false,responsive:true,maintainAspectRatio:false,
  plugins:{legend:{labels:{color:'#8b949e',font:{size:11}}}},
  scales:{x:{ticks:{color:'#8b949e',font:{size:10}},grid:{color:'#21262d'}},
          y:{ticks:{color:'#8b949e',font:{size:10}},grid:{color:'#21262d'}}}};
let lChart,sChart,mChart,state={alerts:[],telemetry:[],nodes:[]};

function initCharts(){
  lChart=new Chart(document.getElementById('lc'),{type:'line',data:{labels:[],datasets:[{label:'Latency (ms)',data:[],borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,.1)',fill:true,tension:.3,pointRadius:0,borderWidth:1.5}]},options:CO});
  sChart=new Chart(document.getElementById('sc'),{type:'doughnut',data:{labels:['Critical','Major','Minor','Warning'],datasets:[{data:[0,0,0,0],backgroundColor:['rgba(248,81,73,.8)','rgba(255,123,114,.8)','rgba(227,179,65,.8)','rgba(63,185,80,.8)'],borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,animation:false,plugins:{legend:{labels:{color:'#8b949e',font:{size:11}}}}}});
  mChart=new Chart(document.getElementById('mc'),{type:'bar',data:{labels:[],datasets:[{label:'Count',data:[],backgroundColor:'rgba(88,166,255,.6)',borderRadius:4}]},options:{...CO,plugins:{legend:{display:false}}}});
}

const SORD={WARNING:0,MINOR:1,MAJOR:2,CRITICAL:3};
function ft(ts){try{return new Date(ts).toLocaleTimeString();}catch{return ts;}}
function cv(v,warn,crit){return v>=crit?'crit':v>=warn?'warn':'ok';}

function render(){
  const sevF=document.getElementById('sev-f').value;
  const nodeF=document.getElementById('nf').value;

  // Update node select
  const ns=document.getElementById('nf');
  const cur=ns.value;
  ns.innerHTML='<option value="">All nodes</option>'+state.nodes.map(n=>`<option value="${n}"${n===cur?' selected':''}>${n}</option>`).join('');

  // Stats
  const al=state.alerts;
  document.getElementById('s-total').textContent=al.length;
  document.getElementById('s-cr').textContent=al.filter(a=>a.severity==='CRITICAL').length;
  document.getElementById('s-mj').textContent=al.filter(a=>a.severity==='MAJOR').length;
  document.getElementById('s-mn').textContent=al.filter(a=>a.severity==='MINOR'||a.severity==='WARNING').length;

  // Alert list
  let filtered=[...al].reverse();
  if(sevF) filtered=filtered.filter(a=>SORD[a.severity]>=SORD[sevF]);
  filtered=filtered.slice(0,80);
  document.getElementById('al').innerHTML=filtered.length
    ?filtered.map(a=>`<div class="ar"><span><span class="badge ${a.severity}">${a.severity}</span></span><span class="ts">${ft(a.timestamp)}</span><span class="nid">${a.node_id}</span><span class="met">${a.metric}</span><span class="zs">${parseFloat(a.z_score).toFixed(2)}σ</span></div>`).join('')
    :'<div style="padding:20px;text-align:center;color:var(--muted)">No alerts match filter</div>';

  // Severity donut
  sChart.data.datasets[0].data=[
    al.filter(a=>a.severity==='CRITICAL').length,al.filter(a=>a.severity==='MAJOR').length,
    al.filter(a=>a.severity==='MINOR').length,al.filter(a=>a.severity==='WARNING').length];
  sChart.update();

  // Metric bar
  const mc={};
  al.forEach(a=>{mc[a.metric]=(mc[a.metric]||0)+1;});
  const me=Object.entries(mc).sort((a,b)=>b[1]-a[1]);
  mChart.data.labels=me.map(m=>m[0].replace('_pct','').replace('_ms','').replace(/_/g,' '));
  mChart.data.datasets[0].data=me.map(m=>m[1]);
  mChart.update();

  // Latency chart — last 60 frames for selected node (or all)
  let frames=state.telemetry;
  if(nodeF) frames=frames.filter(f=>f.node_id===nodeF);
  frames=frames.slice(-60);
  lChart.data.labels=frames.map(f=>ft(f.timestamp));
  lChart.data.datasets[0].data=frames.map(f=>f.metrics.latency_ms);
  lChart.update();

  // Node cards — latest reading per node
  const latest={};
  state.telemetry.forEach(f=>{latest[f.node_id]=f;});
  document.getElementById('ng').innerHTML=Object.entries(latest).map(([nid,f])=>{
    const m=f.metrics;
    return `<div class="nc">
      <div class="nn">${nid}${f.fault?` <span style="color:var(--critical);font-size:10px">⚠ ${f.fault.replace('_',' ')}</span>`:''}
      </div>
      <div class="mr"><span class="mk">Latency</span><span class="mv ${cv(m.latency_ms,25,50)}">${m.latency_ms.toFixed(1)} ms</span></div>
      <div class="mr"><span class="mk">Pkt loss</span><span class="mv ${cv(m.packet_loss_pct,.1,1)}">${m.packet_loss_pct.toFixed(3)}%</span></div>
      <div class="mr"><span class="mk">Jitter</span><span class="mv ${cv(m.jitter_ms,2,8)}">${m.jitter_ms.toFixed(2)} ms</span></div>
      <div class="mr"><span class="mk">Utilization</span><span class="mv ${cv(m.link_util_pct,70,85)}">${m.link_util_pct.toFixed(1)}%</span></div>
    </div>`;
  }).join('')||'<span style="color:var(--muted)">Waiting for first telemetry tick...</span>';

  document.getElementById('ts').textContent='Updated '+new Date().toLocaleTimeString();
  document.getElementById('dot').className='dot live';

  // Hide warmup banner once alerts start arriving
  if(al.length>0) document.getElementById('warmup').style.display='none';
}

// Poll the API every 2 seconds
async function poll(){
  try{
    const [ar,tr]=await Promise.all([
      fetch('/api/alerts').then(r=>r.json()),
      fetch('/api/telemetry').then(r=>r.json()),
    ]);
    state.alerts=ar.alerts||[];
    state.telemetry=tr.frames||[];
    state.nodes=[...new Set(state.telemetry.map(f=>f.node_id))].sort();
    render();
  }catch(e){
    document.getElementById('ts').textContent='API error — retrying';
    document.getElementById('dot').style.background='var(--critical)';
  }
}

initCharts();
poll();
setInterval(poll,2000);
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    """Handles all HTTP requests — dashboard HTML + API endpoints."""

    def log_message(self, fmt, *args):
        pass  # silence default access log

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)

        # ── Dashboard HTML ────────────────────────────────────────────────
        if path in ("/", "/index.html"):
            body = DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /api/alerts ───────────────────────────────────────────────────
        if path == "/api/alerts":
            node     = qs.get("node",     [""])[0]
            severity = qs.get("severity", [""])[0]
            limit    = int(qs.get("limit", ["200"])[0])

            with alerts_lock:
                result = list(alerts)

            if node:
                result = [a for a in result if a["node_id"] == node]
            if severity:
                min_s = SEVERITY_ORDER.get(severity, 0)
                result = [a for a in result if SEVERITY_ORDER.get(a["severity"], 0) >= min_s]

            result = result[-limit:]
            self.send_json({"alerts": result, "total": len(result)})
            return

        # ── /api/telemetry ────────────────────────────────────────────────
        if path == "/api/telemetry":
            node  = qs.get("node",  [""])[0]
            limit = int(qs.get("limit", ["120"])[0])

            with telemetry_lock:
                frames = list(telemetry_frames)

            if node:
                frames = [f for f in frames if f["node_id"] == node]

            frames = frames[-limit:]
            self.send_json({"frames": frames, "total": len(frames)})
            return

        # ── /api/stats ────────────────────────────────────────────────────
        if path == "/api/stats":
            with alerts_lock:
                al = list(alerts)
            with telemetry_lock:
                tf = list(telemetry_frames)

            by_sev    = {}
            by_node   = {}
            by_metric = {}
            for a in al:
                by_sev[a["severity"]]   = by_sev.get(a["severity"], 0) + 1
                by_node[a["node_id"]]   = by_node.get(a["node_id"], 0) + 1
                by_metric[a["metric"]]  = by_metric.get(a["metric"], 0) + 1

            latest = {}
            for f in reversed(tf):
                if f["node_id"] not in latest:
                    latest[f["node_id"]] = f.get("metrics", {})

            self.send_json({
                "total_alerts": len(al),
                "by_severity":  by_sev,
                "by_node":      by_node,
                "by_metric":    by_metric,
                "node_latest":  latest,
                "nodes":        sorted(latest.keys()),
            })
            return

        # ── /api/health ───────────────────────────────────────────────────
        if path == "/api/health":
            with alerts_lock:    na = len(alerts)
            with telemetry_lock: nt = len(telemetry_frames)
            self.send_json({"ok": True, "alerts": na, "telemetry_frames": nt})
            return

        self.send_response(404)
        self.end_headers()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — wire it all together
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  NetWatch — Real-Time Network Anomaly Detection Engine")
    print("=" * 60)
    print()
    print("  Starting 3 background threads:")
    print("  1. Telemetry simulator  (fake network nodes)")
    print("  2. Detection engine     (Z-score anomaly detection)")
    print("  3. Web server           (dashboard + API)")
    print()

    # Start the simulator
    t1 = threading.Thread(target=simulator_thread, daemon=True)
    t1.start()

    # Start the detection engine
    t2 = threading.Thread(target=detection_engine_thread, daemon=True)
    t2.start()

    # Start the HTTP server (blocking — runs on the main thread)
    server = HTTPServer(("0.0.0.0", 8080), Handler)

    def on_exit(sig, frame):
        print("\n\n[main] Shutting down...")
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT,  on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    print("  ✓ Open your browser:  http://localhost:8080")
    print()
    print("  What to watch:")
    print("  - Node cards update every 2s with live telemetry")
    print("  - After ~30s the detector has enough baseline data to fire alerts")
    print("  - Faults are injected randomly — watch the terminal for '>>> Injecting'")
    print("  - Alerts appear in the dashboard with severity and Z-score")
    print()
    print("  Press Ctrl+C to stop.")
    print()

    server.serve_forever()


if __name__ == "__main__":
    main()
