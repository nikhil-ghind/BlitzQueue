/**
 * async_worker_pool.cpp
 *
 * Demonstrates a Boost.Asio-backed thread pool that drains a BlitzQueue
 * and dispatches each item to a worker coroutine / handler.
 *
 * Build note: this file requires Boost.Asio (header-only, boost >= 1.66).
 * Link with: -lboost_thread -lpthread  (or use Boost.Asio header-only mode)
 */

#include "blitz_queue/mpmc_queue.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

static constexpr std::size_t kQueueCapacity = 65536;
static constexpr int         kWorkerThreads = 4;
static constexpr uint64_t    kTotalItems    = 1'000'000ULL;
static constexpr int         kDrainerPollUs = 50; // microseconds between drain polls

using WorkQueue = blitz::MpmcQueue<std::string, kQueueCapacity>;

// ─────────────────────────────────────────────────────────────────────────────
// AsyncWorkerPool
// ─────────────────────────────────────────────────────────────────────────────

class AsyncWorkerPool {
public:
    using Handler = std::function<void(std::string&&)>;

    explicit AsyncWorkerPool(int nthreads, Handler handler)
        : ioc_()
        , work_guard_(asio::make_work_guard(ioc_))
        , handler_(std::move(handler))
        , stopped_(false) {
        workers_.reserve(nthreads);
        for (int i = 0; i < nthreads; ++i) {
            workers_.emplace_back([this]() { ioc_.run(); });
        }
    }

    ~AsyncWorkerPool() { stop(); }

    // Start a background drainer that polls the queue and posts work to Asio.
    void start_drainer(WorkQueue& queue) {
        drainer_ = std::thread([this, &queue]() {
            while (!stopped_.load(std::memory_order_relaxed)) {
                std::string item;
                if (queue.dequeue(item)) {
                    // Move item into a lambda captured by value and post it.
                    auto h = handler_;
                    asio::post(ioc_, [h, item = std::move(item)]() mutable {
                        h(std::move(item));
                    });
                } else {
                    // Nothing to drain; yield briefly to avoid burning CPU.
                    struct timespec ts{0, kDrainerPollUs * 1000L};
                    nanosleep(&ts, nullptr);
                }
            }
        });
    }

    void stop() {
        if (!stopped_.exchange(true)) {
            if (drainer_.joinable()) drainer_.join();
            work_guard_.reset();
            ioc_.stop();
            for (auto& t : workers_) t.join();
        }
    }

    asio::io_context& io_context() { return ioc_; }

private:
    asio::io_context        ioc_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    Handler                 handler_;
    std::vector<std::thread> workers_;
    std::thread             drainer_;
    std::atomic<bool>       stopped_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Demo / standalone main
// ─────────────────────────────────────────────────────────────────────────────

int main() {
    WorkQueue queue;
    std::atomic<uint64_t> processed{0};

    // Worker handler: simulate processing (just count).
    auto handler = [&processed](std::string&& /*item*/) noexcept {
        processed.fetch_add(1, std::memory_order_relaxed);
    };

    AsyncWorkerPool pool(kWorkerThreads, handler);
    pool.start_drainer(queue);

    // Producer thread: push kTotalItems messages.
    auto t_start = std::chrono::high_resolution_clock::now();

    std::thread producer([&queue]() {
        for (uint64_t i = 0; i < kTotalItems; ++i) {
            std::string msg = "item-" + std::to_string(i);
            while (!queue.enqueue(std::move(msg))) {
                std::this_thread::yield();
                msg = "item-" + std::to_string(i); // rebuild after failed move
            }
        }
    });

    producer.join();

    // Wait until all items are processed.
    while (processed.load(std::memory_order_relaxed) < kTotalItems) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    pool.stop();

    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    std::cout << "AsyncWorkerPool demo:\n"
              << "  Worker threads:  " << kWorkerThreads << "\n"
              << "  Items processed: " << processed.load() << "\n"
              << "  Elapsed:         " << elapsed << " s\n"
              << "  Throughput:      "
              << static_cast<double>(kTotalItems) / elapsed << " items/sec\n";
    return 0;
}
