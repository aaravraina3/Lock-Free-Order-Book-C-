#pragma once

#include "Types.hpp"
#include <atomic>
#include <memory>
#include <cstddef>
#include <cstdint>

/**
 * An order submission message. Price is carried as an integer number of ticks,
 * so turning it into a book index is a subtract, not a floating-point divide.
 */
struct OrderMsg {
    int32_t  price_ticks;
    Quantity quantity;
    Side     side;
};

/**
 * Wait-free single-producer / single-consumer ring buffer for order submission.
 *
 * push() is a relaxed load, a plain store, and one release store. There is no
 * CAS and no lock, so on the producer's hot path it clears in a few ns. This is
 * the client-facing "insert": producers drop orders here and a single matcher
 * thread drains the ring into the book (the LMAX Disruptor pattern).
 *
 * Capacity must be a power of two.
 */
class OrderRing {
public:
    explicit OrderRing(size_t capacity_pow2)
        : m_mask(capacity_pow2 - 1), m_buf(new OrderMsg[capacity_pow2]) {
        m_head.store(0, std::memory_order_relaxed);
        m_tail.store(0, std::memory_order_relaxed);
    }

    // Producer side. Wait-free: no CAS, no retry loop.
    bool push(const OrderMsg& msg) {
        const size_t tail = m_tail.load(std::memory_order_relaxed);
        const size_t next = (tail + 1) & m_mask;
        if (next == m_head.load(std::memory_order_acquire)) return false;  // full
        m_buf[tail] = msg;
        m_tail.store(next, std::memory_order_release);
        return true;
    }

    // Consumer side (the matcher thread).
    bool pop(OrderMsg& out) {
        const size_t head = m_head.load(std::memory_order_relaxed);
        if (head == m_tail.load(std::memory_order_acquire)) return false;  // empty
        out = m_buf[head];
        m_head.store((head + 1) & m_mask, std::memory_order_release);
        return true;
    }

    // Single-threaded only: rewind for a fresh measurement pass.
    void reset() {
        m_head.store(0, std::memory_order_relaxed);
        m_tail.store(0, std::memory_order_relaxed);
    }

private:
    const size_t m_mask;
    std::unique_ptr<OrderMsg[]> m_buf;
    alignas(64) std::atomic<size_t> m_head;
    alignas(64) std::atomic<size_t> m_tail;
};
