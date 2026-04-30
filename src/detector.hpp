#pragma once
/**
 * detector.hpp
 * ------------
 * Rolling-window Z-score anomaly detector.
 *
 * WHAT IS A Z-SCORE?
 *   Z = (x - mean) / stddev
 *   It tells you how many standard deviations a value is from the mean of
 *   recent history. A Z-score of 3 means the value is 3 standard deviations
 *   away — statistically, that happens less than 0.3% of the time in a normal
 *   distribution, so we treat it as an anomaly.
 *
 * WHY A ROLLING WINDOW?
 *   Network baselines drift over time (traffic patterns change by hour, day,
 *   season). A rolling window of the last N samples lets our baseline adapt
 *   automatically without manual recalibration. This is called "adaptive
 *   thresholding" — a key phrase for interviews.
 *
 * WHY NOT ML?
 *   For a first-pass anomaly detector, statistical methods are preferred
 *   because they are: deterministic (same input = same output), explainable
 *   (you can tell an operator exactly why an alert fired), and require no
 *   training data. ML is added on top for pattern classification AFTER
 *   the statistical layer catches candidates. This two-stage design is
 *   standard in production NOC (Network Operations Center) systems.
 *
 * THREAD SAFETY:
 *   This class is NOT thread-safe. The caller must hold a mutex when calling
 *   push() or is_anomaly() from multiple threads. The main engine uses a
 *   single reader thread per metric, so this is fine.
 */

#include <cmath>
#include <deque>
#include <numeric>
#include <stdexcept>
#include <string>

namespace netwatch {

class RollingZScoreDetector {
public:
    /**
     * @param window_size  Number of recent samples to compute baseline from.
     *                     128 samples at 1s intervals = ~2 min of history.
     * @param z_threshold  Anomaly threshold. 3.0 is industry standard for
     *                     "three-sigma" alerting. Lower = more sensitive.
     * @param min_samples  Don't alert until we have enough data to compute
     *                     a meaningful baseline. Avoids false positives on
     *                     startup.
     */
    explicit RollingZScoreDetector(
        std::size_t window_size = 128,
        double      z_threshold  = 3.0,
        std::size_t min_samples  = 30
    )
        : window_size_(window_size)
        , z_threshold_(z_threshold)
        , min_samples_(min_samples)
    {
        if (window_size < 2)
            throw std::invalid_argument("window_size must be >= 2");
    }

    /** Ingest one new sample. Evicts oldest if window is full. O(1) amortized. */
    void push(double value) {
        window_.push_back(value);
        if (window_.size() > window_size_)
            window_.pop_front();
    }

    /**
     * Returns true if `value` is anomalous relative to current window.
     * Does NOT push the value — call push() separately so you can decide
     * whether to include anomalies in the baseline or not.
     *
     * Design choice: we DO include anomalies in the window. This makes the
     * detector self-correcting: if high latency becomes the new normal
     * (e.g. after a network change), the detector adapts within ~window_size
     * ticks rather than alerting forever. Trade-off: it can "absorb" a slowly
     * growing degradation. For production you'd add a secondary trend detector.
     */
    bool is_anomaly(double value) const {
        if (window_.size() < min_samples_)
            return false;  // Not enough history yet

        double mean   = compute_mean();
        double stddev = compute_stddev(mean);

        if (stddev < 1e-10)
            return false;  // Perfectly constant signal — no variance to detect against

        double z = std::abs((value - mean) / stddev);
        return z > z_threshold_;
    }

    double z_score(double value) const {
        if (window_.size() < min_samples_) return 0.0;
        double mean   = compute_mean();
        double stddev = compute_stddev(mean);
        if (stddev < 1e-10) return 0.0;
        return (value - mean) / stddev;
    }

    std::size_t sample_count() const { return window_.size(); }
    double      threshold()    const { return z_threshold_; }

private:
    std::deque<double> window_;
    std::size_t        window_size_;
    double             z_threshold_;
    std::size_t        min_samples_;

    double compute_mean() const {
        double sum = std::accumulate(window_.begin(), window_.end(), 0.0);
        return sum / static_cast<double>(window_.size());
    }

    double compute_stddev(double mean) const {
        double sq_sum = 0.0;
        for (double v : window_)
            sq_sum += (v - mean) * (v - mean);
        // Population stddev (not sample stddev) — we have the full window,
        // not a sample from a larger population.
        return std::sqrt(sq_sum / static_cast<double>(window_.size()));
    }
};

}  // namespace netwatch
