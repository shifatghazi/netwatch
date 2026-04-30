#pragma once
/**
 * telemetry.hpp
 * -------------
 * Plain data structures for network telemetry. Kept as simple POD-like
 * structs so they're trivially serializable. In a production system these
 * would be generated from a Protobuf schema — we define them manually here
 * to keep the build simple.
 *
 * WHY SEPARATE DATA FROM LOGIC?
 *   This is the "data model" layer. Keeping it separate from the detector
 *   logic means you can swap out the data source (file → gNMI stream →
 *   Kafka) without touching the detection algorithm. This is the
 *   Single Responsibility Principle in practice.
 */

#include <string>
#include <optional>

namespace netwatch {

struct Metrics {
    double latency_ms       = 0.0;
    double packet_loss_pct  = 0.0;
    double jitter_ms        = 0.0;
    double link_util_pct    = 0.0;
    double error_rate       = 0.0;
};

struct TelemetryFrame {
    std::string              timestamp;
    std::string              node_id;
    std::optional<std::string> anomaly_injected;  // null in normal traffic
    Metrics                  metrics;
};

/**
 * Severity levels for alerts. Maps to standard NOC severity conventions
 * used by Nokia NSP, Ciena MCP, and most OSS systems.
 *
 *   CRITICAL  = service-affecting, page the on-call engineer now
 *   MAJOR     = degraded service, open a trouble ticket
 *   MINOR     = heads-up, investigate during business hours
 *   WARNING   = informational, trending in bad direction
 */
enum class Severity { WARNING, MINOR, MAJOR, CRITICAL };

inline std::string severity_to_string(Severity s) {
    switch (s) {
        case Severity::WARNING:  return "WARNING";
        case Severity::MINOR:    return "MINOR";
        case Severity::MAJOR:    return "MAJOR";
        case Severity::CRITICAL: return "CRITICAL";
    }
    return "UNKNOWN";
}

struct Alert {
    std::string  timestamp;
    std::string  node_id;
    std::string  metric_name;   // Which metric triggered the alert
    double       observed_value;
    double       z_score;       // How many sigmas from baseline
    Severity     severity;
    std::string  description;   // Human-readable explanation for operators
};

}  // namespace netwatch
