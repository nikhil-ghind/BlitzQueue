/**
 * BlitzQueue Benchmark
 *
 * Measures raw MPMC throughput with N producers and N consumers running
 * concurrently.  Reports:
 *   - Total messages / second
 *   - p50 / p95 / p99 enqueue latency in nanoseconds
 *
 * Target: >= 2 000 000 msgs/sec with 4 producers + 4 consumers.
 */

#include "blitz_queue/mpmc_queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

using Clock = std::chrono::high_resolution_clock;
using ns    = std::chrono::nanoseconds;

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark parameters
// ─────────────────────────────────────────────────────────────────────────────

static constexpr std::size_t kQueueCapacity = 65536;
static constexpr uint64_t    kMessagesPerProducer = 2'000'000ULL;
static constexpr int         kDefaultThreadPairs  = 4;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<ns>(Clock::now().time_since_epoch()).count());
}

static double percentile(std::vector<uint64_t>& v, double pct) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    std::size_t idx = static_cast<std::size_t>(pct / 100.0 * (v.size() - 1));
    return static_cast<double>(v[idx]);
}

// ─────────────────────────────────────────────────────────────────────────────
// MPMC benchmark
// ─────────────────────────────────────────────────────────────────────────────

void run_mpmc_benchmark(int n_pairs) {
    using Queue = blitz::MpmcQueue<uint64_t, kQueueCapacity>;
    Queue queue;

    const uint64_t total_messages = static_cast<uint64_t>(n_pairs) * kMessagesPerProducer;

    std::atomic<bool> start_flag{false};
    std::atomic<uint64_t> consumed{0};

    // Per-producer latency samples (1 in 100 sampled to keep memory sane)
    std::vector<std::vector<uint64_t>> lat_samples(n_pairs);
    std::vector<std::thread> producers, consumers;

    // ── Producers ────────────────────────────────────────────────────────────
    for (int i = 0; i < n_pairs; ++i) {
        producers.emplace_back([&, i]() {
            auto& samples = lat_samples[i];
            samples.reserve(kMessagesPerProducer / 100);
            while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }

            for (uint64_t j = 0; j < kMessagesPerProducer; ++j) {
                uint64_t t0 = now_ns();
                while (!queue.enqueue(j)) {
                    // Back-pressure: queue is full; busy-wait
                    std::this_thread::yield();
                }
                uint64_t lat = now_ns() - t0;
                if (j % 100 == 0) samples.push_back(lat);
            }
        });
    }

    // ── Consumers ────────────────────────────────────────────────────────────
    for (int i = 0; i < n_pairs; ++i) {
        consumers.emplace_back([&]() {
            while (!start_flag.load(std::memory_order_acquire)) { /* spin */ }
            uint64_t item;
            for (;;) {
                if (queue.dequeue(item)) {
                    uint64_t c = consumed.fetch_add(1, std::memory_order_relaxed) + 1;
                    if (c >= total_messages) return;
                } else {
                    if (consumed.load(std::memory_order_relaxed) >= total_messages) return;
                    std::this_thread::yield();
                }
            }
        });
    }

    // ── Run ──────────────────────────────────────────────────────────────────
    auto wall_start = Clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto wall_end = Clock::now();
    double elapsed_s = std::chrono::duration<double>(wall_end - wall_start).count();

    // ── Merge latency samples ─────────────────────────────────────────────────
    std::vector<uint64_t> all_samples;
    for (auto& v : lat_samples)
        all_samples.insert(all_samples.end(), v.begin(), v.end());

    double msgs_per_sec = static_cast<double>(total_messages) / elapsed_s;

    std::cout << "\n====================================================\n"
              << "  BlitzQueue MPMC Benchmark\n"
              << "====================================================\n"
              << "  Producers:        " << n_pairs << "\n"
              << "  Consumers:        " << n_pairs << "\n"
              << "  Queue capacity:   " << kQueueCapacity << "\n"
              << "  Total messages:   " << total_messages << "\n"
              << "  Elapsed time:     " << std::fixed << std::setprecision(3)
                                        << elapsed_s << " s\n"
              << "  Throughput:       " << std::fixed << std::setprecision(0)
                                        << msgs_per_sec << " msgs/sec\n"
              << "\n  Enqueue latency (nanoseconds):\n"
              << "    p50:  " << std::setprecision(0) << percentile(all_samples, 50.0) << " ns\n"
              << "    p95:  " << percentile(all_samples, 95.0) << " ns\n"
              << "    p99:  " << percentile(all_samples, 99.0) << " ns\n"
              << "====================================================\n\n";

    if (msgs_per_sec >= 2'000'000.0) {
        std::cout << "  [PASS] Throughput target met (>= 2M msgs/sec)\n\n";
    } else {
        std::cout << "  [NOTE] Below 2M msgs/sec target; check CPU affinity "
                     "or reduce thread count.\n\n";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    int n_pairs = kDefaultThreadPairs;
    if (argc > 1) {
        n_pairs = std::stoi(argv[1]);
        if (n_pairs < 1 || n_pairs > 64) {
            std::cerr << "Thread pairs must be 1..64\n";
            return 1;
        }
    }

    std::cout << "CPU hardware concurrency: "
              << std::thread::hardware_concurrency() << " threads\n";

    run_mpmc_benchmark(n_pairs);
    return 0;
}
