#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>

namespace blitz {

// Compile-time power-of-2 check
template <std::size_t N>
struct is_power_of_two : std::integral_constant<bool, N != 0 && (N & (N - 1)) == 0> {};

/**
 * MpmcQueue<T, Capacity>
 *
 * Lock-free multi-producer multi-consumer bounded queue.
 *
 * Algorithm: Each slot carries a sequence number.  Producers CAS the head
 * index forward and write into slots[head % Capacity]; consumers CAS the
 * tail index forward and read from slots[tail % Capacity].  The sequence
 * number in each slot signals whether the slot is "ready to write" or
 * "ready to read", eliminating ABA issues without a generation counter on
 * the index.
 *
 * Capacity MUST be a power of two (enforced at compile time).
 */
template <typename T, std::size_t Capacity>
class MpmcQueue {
    static_assert(is_power_of_two<Capacity>::value,
                  "MpmcQueue Capacity must be a power of two");
    static_assert(Capacity >= 2, "MpmcQueue Capacity must be at least 2");

public:
    // Each slot is padded to a full cache line to prevent false sharing.
    struct alignas(64) Slot {
        std::atomic<std::size_t> sequence{0};
        T data;
    };

    MpmcQueue() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i) {
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    MpmcQueue(const MpmcQueue&) = delete;
    MpmcQueue& operator=(const MpmcQueue&) = delete;

    /**
     * Try to enqueue an item.
     * Returns true on success, false if the queue is full.
     */
    bool enqueue(T&& item) noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[head & kMask];
            std::size_t seq = slot.sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) -
                                 static_cast<std::intptr_t>(head);
            if (diff == 0) {
                // Slot is ready to be written; attempt to claim it.
                if (head_.compare_exchange_weak(head, head + 1,
                                                std::memory_order_relaxed)) {
                    slot.data = std::move(item);
                    // Publish: sequence = head + 1 signals "ready to read".
                    slot.sequence.store(head + 1, std::memory_order_release);
                    return true;
                }
                // CAS failed — another producer took this slot; retry.
            } else if (diff < 0) {
                // Queue is full.
                return false;
            } else {
                // Another producer is ahead; reload head and retry.
                head = head_.load(std::memory_order_relaxed);
            }
        }
    }

    /**
     * Try to enqueue a copy.
     */
    bool enqueue(const T& item) noexcept {
        T copy = item;
        return enqueue(std::move(copy));
    }

    /**
     * Try to dequeue an item.
     * Returns true on success, false if the queue is empty.
     */
    bool dequeue(T& item) noexcept {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[tail & kMask];
            std::size_t seq = slot.sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) -
                                 static_cast<std::intptr_t>(tail + 1);
            if (diff == 0) {
                // Slot is ready to be read; attempt to claim it.
                if (tail_.compare_exchange_weak(tail, tail + 1,
                                                std::memory_order_relaxed)) {
                    item = std::move(slot.data);
                    // Release slot back: sequence = tail + Capacity.
                    slot.sequence.store(tail + Capacity,
                                        std::memory_order_release);
                    return true;
                }
                // CAS failed — another consumer took this slot; retry.
            } else if (diff < 0) {
                // Queue is empty.
                return false;
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    /**
     * Approximate number of items currently in the queue.
     * May be slightly stale due to concurrent modifications.
     */
    std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        return (head >= tail) ? (head - tail) : 0;
    }

    static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // Pad head_ and tail_ to separate cache lines to avoid false sharing
    // between producers and consumers.
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};

    Slot slots_[Capacity];
};

} // namespace blitz
