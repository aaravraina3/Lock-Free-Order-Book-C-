#pragma once

#include <atomic>
#include <vector>
#include <memory>

/**
 * A simple Single-Producer Single-Consumer (SPSC) lock-free queue.
 * Ideal for passing trade results from the matching engine to a logger/subscriber.
 */
template<typename T>
class SPSCQueue {
public:
    SPSCQueue(size_t capacity) : m_capacity(capacity + 1), m_buffer(new T[capacity + 1]) {
        m_head.store(0, std::memory_order_relaxed);
        m_tail.store(0, std::memory_order_relaxed);
    }

    bool push(const T& item) {
        size_t tail = m_tail.load(std::memory_order_relaxed);
        size_t next_tail = (tail + 1) % m_capacity;
        if (next_tail == m_head.load(std::memory_order_acquire)) {
            return false; // Full
        }
        m_buffer[tail] = item;
        m_tail.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t head = m_head.load(std::memory_order_relaxed);
        if (head == m_tail.load(std::memory_order_acquire)) {
            return false; // Empty
        }
        item = m_buffer[head];
        m_head.store((head + 1) % m_capacity, std::memory_order_release);
        return true;
    }

private:
    const size_t m_capacity;
    std::unique_ptr<T[]> m_buffer;
    alignas(64) std::atomic<size_t> m_head;
    alignas(64) std::atomic<size_t> m_tail;
};

