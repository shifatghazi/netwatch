/**
 * test_detector.cpp
 * -----------------
 * Unit tests for RollingZScoreDetector.
 *
 * We use a minimal test framework (no external deps) — just macros that
 * print PASS/FAIL and exit with a non-zero code on failure. In a production
 * project you'd use Google Test or Catch2, but this keeps the build simple.
 *
 * WHY WRITE TESTS? (important for interviews)
 *   In a safety-critical system (network equipment, automotive), a false
 *   negative (missed anomaly) causes an outage. A false positive (spurious
 *   alert) erodes operator trust until they start ignoring the system.
 *   Tests give you confidence that your thresholds and window logic are
 *   correct before you deploy to production hardware.
 */

#include <cassert>
#include <iostream>
#include <cmath>
#include "../src/detector.hpp"

using namespace netwatch;

int tests_run = 0;
int tests_passed = 0;

#define TEST(name, expr) do { \
    tests_run++; \
    if (expr) { \
        std::cout << "  PASS  " << name << "\n"; \
        tests_passed++; \
    } else { \
        std::cout << "  FAIL  " << name << " (line " << __LINE__ << ")\n"; \
    } \
} while(0)

// ── Test: no alerts before min_samples ───────────────────────────────────
void test_warmup_period() {
    std::cout << "\n[test_warmup_period]\n";
    RollingZScoreDetector det(128, 3.0, 30);

    // Feed 29 samples — should never alert yet
    for (int i = 0; i < 29; i++)
        det.push(10.0);

    TEST("no alert before min_samples", !det.is_anomaly(999.0));

    // Feed one more to reach min_samples
    det.push(10.0);
    TEST("alerts after min_samples with obvious outlier", det.is_anomaly(999.0));
}

// ── Test: normal values don't trigger ────────────────────────────────────
void test_normal_values() {
    std::cout << "\n[test_normal_values]\n";
    RollingZScoreDetector det(128, 3.0, 30);

    // Feed 60 samples with mean=12, std~1.5 (like our latency baseline)
    for (int i = 0; i < 60; i++)
        det.push(12.0 + (i % 3 == 0 ? 0.5 : -0.5));

    TEST("value at mean not an anomaly",   !det.is_anomaly(12.0));
    TEST("value 1-sigma away not anomaly", !det.is_anomaly(13.5));
    TEST("value 2-sigma away not anomaly", !det.is_anomaly(15.0));
}

// ── Test: clear anomalies are detected ───────────────────────────────────
void test_anomaly_detection() {
    std::cout << "\n[test_anomaly_detection]\n";
    RollingZScoreDetector det(128, 3.0, 30);

    // Stable baseline: mean=12, very low std
    for (int i = 0; i < 60; i++)
        det.push(12.0);

    // A latency spike to 180ms should be many sigmas above baseline
    TEST("obvious spike is anomaly",     det.is_anomaly(180.0));
    TEST("z_score high for spike",       std::abs(det.z_score(180.0)) > 3.0);
    TEST("z_score near-zero for normal", std::abs(det.z_score(12.5)) < 1.0);
}

// ── Test: window eviction ─────────────────────────────────────────────────
void test_window_eviction() {
    std::cout << "\n[test_window_eviction]\n";
    RollingZScoreDetector det(10, 3.0, 5);  // tiny window for testing

    // Fill with 10 stable samples
    for (int i = 0; i < 10; i++) det.push(100.0);

    TEST("spike detected on stable window", det.is_anomaly(200.0));

    // Now flood with the new higher baseline — window should adapt
    for (int i = 0; i < 10; i++) det.push(200.0);

    // 200 is now the baseline — 201 should not be an anomaly
    TEST("adapted baseline, 201 not anomaly", !det.is_anomaly(201.0));
    // But 999 still is
    TEST("obvious spike still anomaly after adaptation", det.is_anomaly(999.0));
}

// ── Test: flat signal ─────────────────────────────────────────────────────
void test_zero_variance() {
    std::cout << "\n[test_zero_variance]\n";
    RollingZScoreDetector det(128, 3.0, 30);

    // Perfectly constant signal
    for (int i = 0; i < 60; i++) det.push(42.0);

    // With zero stddev, we can't compute a meaningful z-score.
    // The detector should NOT alert (it returns false for zero-variance signals)
    // because any deviation would produce infinite z-score. Better to be quiet.
    TEST("constant signal: no false positive for slightly different value",
         !det.is_anomaly(42.1));
}

// ── Test: sample count tracking ──────────────────────────────────────────
void test_sample_count() {
    std::cout << "\n[test_sample_count]\n";
    RollingZScoreDetector det(5, 3.0, 3);  // window of 5

    TEST("empty window", det.sample_count() == 0);
    det.push(1.0); det.push(2.0); det.push(3.0);
    TEST("3 samples", det.sample_count() == 3);
    det.push(4.0); det.push(5.0); det.push(6.0);
    TEST("window capped at 5", det.sample_count() == 5);
}

int main() {
    std::cout << "=== NetWatch Detector Unit Tests ===\n";

    test_warmup_period();
    test_normal_values();
    test_anomaly_detection();
    test_window_eviction();
    test_zero_variance();
    test_sample_count();

    std::cout << "\n=== Results: " << tests_passed << "/" << tests_run << " passed ===\n";
    return (tests_passed == tests_run) ? 0 : 1;
}
