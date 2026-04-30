/**
 * engine.cpp
 * ----------
 * Main detection engine daemon.
 *
 * WHAT THIS PROCESS DOES:
 *   1. Opens the telemetry JSONL file written by the Python simulator
 *   2. Tail-follows it (like `tail -f`) using inotify on Linux
 *   3. For each new line, parses the JSON and extracts metrics
 *   4. Feeds each metric into its own RollingZScoreDetector instance
 *   5. If any metric crosses the z-score threshold, emits an Alert
 *   6. Writes alerts to a separate JSONL file that the API server reads
 *
 * WHY INOTIFY INSTEAD OF POLLING?
 *   Polling (checking the file every N ms) wastes CPU. inotify is a Linux
 *   kernel facility that wakes your process ONLY when the file changes.
 *   This is how production log shippers (Filebeat, Fluent Bit) work.
 *   On macOS you'd use kqueue; on Windows, ReadDirectoryChangesW.
 *   This is a great interview talking point — shows you understand OS-level
 *   I/O efficiency, not just "read a file in a loop."
 *
 * Z-SCORE TO SEVERITY MAPPING:
 *   3.0 - 4.0 sigma  → MINOR
 *   4.0 - 5.0 sigma  → MAJOR
 *   > 5.0 sigma      → CRITICAL
 *   We also escalate packet_loss and error_rate alerts by one level because
 *   those metrics are more operationally impactful than latency.
 *
 * JSON PARSING:
 *   We use nlohmann/json (header-only, MIT license) — the most widely used
 *   C++ JSON library. No external build dependencies needed.
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/inotify.h>

#include "detector.hpp"
#include "telemetry.hpp"
#include "json.hpp"   // nlohmann/json single-header

using json = nlohmann::json;
using namespace netwatch;

// ── Global shutdown flag ─────────────────────────────────────────────────────
static std::atomic<bool> g_running{true};

void handle_signal(int) { g_running = false; }

// ── Alert output ─────────────────────────────────────────────────────────────
static std::ofstream g_alert_file;
static std::string   g_alert_path;

void emit_alert(const Alert& alert) {
    json j = {
        {"timestamp",      alert.timestamp},
        {"node_id",        alert.node_id},
        {"metric",         alert.metric_name},
        {"observed_value", alert.observed_value},
        {"z_score",        alert.z_score},
        {"severity",       severity_to_string(alert.severity)},
        {"description",    alert.description},
    };

    std::string line = j.dump();
    g_alert_file << line << "\n";
    g_alert_file.flush();

    // Also print to stdout so docker logs shows something useful
    std::cout << "[ALERT] " << line << "\n";
}

// ── Severity calculation ─────────────────────────────────────────────────────
Severity z_to_severity(double z, const std::string& metric) {
    Severity base;
    double az = std::abs(z);
    if      (az > 5.0) base = Severity::CRITICAL;
    else if (az > 4.0) base = Severity::MAJOR;
    else if (az > 3.0) base = Severity::MINOR;
    else               base = Severity::WARNING;

    // Escalate for high-impact metrics
    if (metric == "packet_loss_pct" || metric == "error_rate") {
        if (base == Severity::MINOR)   base = Severity::MAJOR;
        else if (base == Severity::MAJOR) base = Severity::CRITICAL;
    }
    return base;
}

// ── Per-node detector state ───────────────────────────────────────────────────
struct NodeDetectors {
    RollingZScoreDetector latency;
    RollingZScoreDetector packet_loss;
    RollingZScoreDetector jitter;
    RollingZScoreDetector link_util;
    RollingZScoreDetector error_rate;

    NodeDetectors()
        : latency     (128, 3.0, 30)
        , packet_loss (128, 3.0, 30)
        , jitter      (128, 3.0, 30)
        , link_util   (128, 3.5, 30)   // higher threshold for utilization
        , error_rate  (128, 2.5, 30)   // more sensitive for BER
    {}
};

// ── Frame processing ─────────────────────────────────────────────────────────
void process_frame(const TelemetryFrame& frame,
                   std::map<std::string, NodeDetectors>& node_map)
{
    auto& nd = node_map[frame.node_id];  // creates entry if missing
    const auto& m = frame.metrics;

    // Helper: check one metric, emit alert if anomalous
    auto check = [&](const std::string& name,
                     double value,
                     RollingZScoreDetector& det)
    {
        bool anom = det.is_anomaly(value);
        double z  = det.z_score(value);
        det.push(value);

        if (anom) {
            std::ostringstream desc;
            desc << name << " = " << value
                 << " (z=" << std::fixed << std::setprecision(2) << z
                 << ", threshold=" << det.threshold() << ")";

            Alert alert{
                .timestamp      = frame.timestamp,
                .node_id        = frame.node_id,
                .metric_name    = name,
                .observed_value = value,
                .z_score        = z,
                .severity       = z_to_severity(z, name),
                .description    = desc.str(),
            };
            emit_alert(alert);
        }
    };

    check("latency_ms",      m.latency_ms,      nd.latency);
    check("packet_loss_pct", m.packet_loss_pct,  nd.packet_loss);
    check("jitter_ms",       m.jitter_ms,        nd.jitter);
    check("link_util_pct",   m.link_util_pct,    nd.link_util);
    check("error_rate",      m.error_rate,        nd.error_rate);
}

// ── JSON parsing ─────────────────────────────────────────────────────────────
std::optional<TelemetryFrame> parse_line(const std::string& line) {
    try {
        auto j = json::parse(line);
        TelemetryFrame f;
        f.timestamp = j.at("timestamp").get<std::string>();
        f.node_id   = j.at("node_id").get<std::string>();

        if (!j.at("anomaly_injected").is_null())
            f.anomaly_injected = j.at("anomaly_injected").get<std::string>();

        auto& mj = j.at("metrics");
        f.metrics.latency_ms      = mj.at("latency_ms").get<double>();
        f.metrics.packet_loss_pct = mj.at("packet_loss_pct").get<double>();
        f.metrics.jitter_ms       = mj.at("jitter_ms").get<double>();
        f.metrics.link_util_pct   = mj.at("link_util_pct").get<double>();
        f.metrics.error_rate      = mj.at("error_rate").get<double>();

        return f;
    } catch (const std::exception& e) {
        std::cerr << "[engine] JSON parse error: " << e.what()
                  << " | line=" << line << "\n";
        return std::nullopt;
    }
}

// ── Tail-follow loop ─────────────────────────────────────────────────────────
void tail_follow(const std::string& telemetry_path) {
    std::map<std::string, NodeDetectors> node_map;

    std::ifstream infile(telemetry_path);
    if (!infile.is_open()) {
        std::cerr << "[engine] Waiting for telemetry file: " << telemetry_path << "\n";
        while (!infile.is_open() && g_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            infile.open(telemetry_path);
        }
    }

    std::cout << "[engine] Opened telemetry file: " << telemetry_path << "\n";

    // Set up inotify to watch for file modifications
    int inotify_fd = inotify_init1(IN_NONBLOCK);
    int watch_fd   = inotify_add_watch(inotify_fd, telemetry_path.c_str(), IN_MODIFY);

    char inotify_buf[4096];
    std::string line;

    while (g_running) {
        // Try to read all pending lines first
        bool read_any = false;
        while (std::getline(infile, line)) {
            if (line.empty()) continue;
            auto frame = parse_line(line);
            if (frame) process_frame(*frame, node_map);
            read_any = true;
        }
        infile.clear();  // reset EOF so next getline works

        if (!read_any) {
            // No new data — block on inotify until file is modified
            read(inotify_fd, inotify_buf, sizeof(inotify_buf));
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    inotify_rm_watch(inotify_fd, watch_fd);
    close(inotify_fd);
}

// ── Entry point ──────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::string telemetry_path = "/tmp/netwatch/telemetry.jsonl";
    g_alert_path               = "/tmp/netwatch/alerts.jsonl";

    if (argc > 1) telemetry_path = argv[1];
    if (argc > 2) g_alert_path   = argv[2];

    // Ensure output directory exists
    std::system(("mkdir -p $(dirname " + g_alert_path + ")").c_str());

    g_alert_file.open(g_alert_path, std::ios::app);
    if (!g_alert_file.is_open()) {
        std::cerr << "[engine] Cannot open alert output: " << g_alert_path << "\n";
        return 1;
    }

    std::cout << "[engine] NetWatch detection engine starting\n";
    std::cout << "[engine] Telemetry: " << telemetry_path << "\n";
    std::cout << "[engine] Alerts:    " << g_alert_path << "\n";

    tail_follow(telemetry_path);

    std::cout << "[engine] Shutdown complete.\n";
    return 0;
}
