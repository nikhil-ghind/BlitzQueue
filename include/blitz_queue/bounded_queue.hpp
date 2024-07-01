#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <new>
#include <type_traits>

namespace blitz {

/**
 * BoundedSpscQueue<T, Capacity>
 *
 * Single-producer / single-consumer lock-free bounded queue.
 *
 * Because only one thread writes and one thread reads, we can use simple
 * load/store pairs on head and tail without CAS.  This yields lower latency
 * and higher throughput than the MPMC variant for the SPSC case.
 *
 * Capacity MUST be a power of two.
 */
template <typename T, std::size_t Capacity>
class BoundedSpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "BoundedSpscQueue Capacity must be a power of two");
    static_assert(Capacity >= 2, "BoundedSpscQueue Capacity must be at least 2");

public:
    BoundedSpscQueue() noexcept = default;

    BoundedSpscQueue(const BoundedSpscQueue&) = delete;
    BoundedSpscQueue& operator=(const BoundedSpscQueue&) = delete;

    /**
     * Producer-side: enqueue an item.
     * Must only be called from the single producer thread.
     * Returns false if queue is full.
     */
    bool enqueue(T&& item) noexcept {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t next = (h + 1) & kMask;
        if (next == tail_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[h] = std::move(item);
        head_.store((h + 1), std::memory_order_release);
        return true;
    }

    bool enqueue(const T& item) noexcept {
        T copy = item;
        return enqueue(std::move(copy));
    }

    /**
     * Consumer-side: dequeue an item.
     * Must only be called from the single consumer thread.
     * Returns false if queue is empty.
     */
    bool dequeue(T& item) noexcept {
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire)) {
            return false; // empty
        }
        item = std::move(buffer_[t]);
        tail_.store((t + 1) & (Capacity), std::memory_order_release);
        return true;
    }

    std::size_t size() const noexcept {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        return (h >= t) ? (h - t) : (Capacity + 1 - t + h);
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) ==
               tail_.load(std::memory_order_relaxed);
    }

    static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity; // intentionally Capacity, not Capacity-1
    // We use Capacity+1 logical slots but Capacity data slots by keeping
    // head always in [0, Capacity).

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
    T buffer_[Capacity];
};

} // namespace blitz
