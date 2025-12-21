#pragma once

#include "Types.hpp"
#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include <array>

/**
 * A simple lock-free object pool (freelist) for Order nodes.
 * Updated to handle TaggedPtr to prevent ABA.
 */
class OrderPool {
public:
    static constexpr size_t NUM_SHARDS = 8;

    OrderPool(size_t capacity) {
        size_t shard_capacity = capacity / NUM_SHARDS;
        m_pool = std::make_unique<Order[]>(capacity);
        
        for (size_t s = 0; s < NUM_SHARDS; ++s) {
            size_t start = s * shard_capacity;
            size_t end = (s == NUM_SHARDS - 1) ? capacity : (s + 1) * shard_capacity;
            
            for (size_t i = start; i < end - 1; ++i) {
                m_pool[i].tag = 0;
                m_pool[i].next.store(TaggedPtr::make(&m_pool[i + 1], 0), std::memory_order_relaxed);
            }
            m_pool[end - 1].tag = 0;
            m_pool[end - 1].next.store(TaggedPtr(0), std::memory_order_relaxed);
            m_heads[s].head.store(TaggedPtr::make(&m_pool[start], 0), std::memory_order_relaxed);
        }
    }

    Order* acquire(Price p, Quantity q, Side s) {
        static thread_local size_t shard_hint = std::hash<std::thread::id>{}(std::this_thread::get_id()) % NUM_SHARDS;
        
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            size_t shard_idx = (shard_hint + i) % NUM_SHARDS;
            auto& shard = m_heads[shard_idx];
            
            TaggedPtr old_head = shard.head.load(std::memory_order_acquire);
            while (old_head.ptr<Order>() && !shard.head.compare_exchange_weak(
                old_head, 
                old_head.ptr<Order>()->next.load(std::memory_order_relaxed),
                std::memory_order_release,
                std::memory_order_acquire)) {
                // CAS loop
            }
            
            Order* node = old_head.ptr<Order>();
            if (node) {
                node->price = p;
                node->quantity.store(q, std::memory_order_relaxed);
                node->side = s;
                node->next.store(TaggedPtr(0), std::memory_order_relaxed);
                // The tag in TaggedPtr remains the one it had in the pool
                return node;
            }
        }
        return nullptr;
    }

    void release(Order* order) {
        if (!order) return;
        
        // Increment the node's internal tag so the next time it's used,
        // it will have a different TaggedPtr value.
        order->tag++;
        
        static thread_local size_t shard_hint = std::hash<std::thread::id>{}(std::this_thread::get_id()) % NUM_SHARDS;
        auto& shard = m_heads[shard_hint];

        TaggedPtr old_head = shard.head.load(std::memory_order_acquire);
        do {
            order->next.store(old_head, std::memory_order_relaxed);
        } while (!shard.head.compare_exchange_weak(
            old_head, 
            TaggedPtr::make(order, order->tag), 
            std::memory_order_release, 
            std::memory_order_acquire));
    }

private:
    struct alignas(64) ShardHead {
        std::atomic<TaggedPtr> head;
    };

    std::unique_ptr<Order[]> m_pool;
    std::array<ShardHead, NUM_SHARDS> m_heads;
};
