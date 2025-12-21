#pragma once

#include "Types.hpp"
#include "OrderPool.hpp"
#include "Trade.hpp"
#include "SPSCQueue.hpp"
#include <array>
#include <atomic>
#include <cmath>
#include <algorithm>

/**
 * A lock-free Order Book with matching capabilities and memory reclamation.
 * Uses TaggedPtr to solve the ABA problem when removing nodes.
 */
class OrderBook {
public:
    static constexpr int MAX_PRICE_LEVELS = 10000;
    static constexpr double TICK_SIZE = 0.01;
    static constexpr double MIN_PRICE = 50.00;

    OrderBook(size_t pool_size) : m_pool(pool_size), m_trades(10000) {
        for (auto& level : m_buys) level.head.store(TaggedPtr(0), std::memory_order_relaxed);
        for (auto& level : m_sells) level.head.store(TaggedPtr(0), std::memory_order_relaxed);
        m_max_buy_idx.store(0, std::memory_order_relaxed);
        m_min_sell_idx.store(MAX_PRICE_LEVELS - 1, std::memory_order_relaxed);
    }

    bool add_order(Side side, Price price, Quantity quantity) {
        int index = price_to_index(price);
        if (index < 0 || index >= MAX_PRICE_LEVELS) return false;

        Order* new_order = m_pool.acquire(price, quantity, side);
        if (!new_order) return false;

        if (side == Side::Buy) {
            int current_max = m_max_buy_idx.load(std::memory_order_relaxed);
            while (index > current_max && !m_max_buy_idx.compare_exchange_weak(current_max, index));
        } else {
            int current_min = m_min_sell_idx.load(std::memory_order_relaxed);
            while (index < current_min && !m_min_sell_idx.compare_exchange_weak(current_min, index));
        }

        auto& levels = (side == Side::Buy) ? m_buys : m_sells;
        PriceLevel& level = levels[index];

        TaggedPtr old_head = level.head.load(std::memory_order_acquire);
        do {
            new_order->next.store(old_head, std::memory_order_relaxed);
        } while (!level.head.compare_exchange_weak(old_head, 
                                                   TaggedPtr::make(new_order, new_order->tag), 
                                                   std::memory_order_release, 
                                                   std::memory_order_acquire));
        
        return true;
    }

    void match() {
        while (true) {
            int buy_idx = m_max_buy_idx.load(std::memory_order_acquire);
            int sell_idx = m_min_sell_idx.load(std::memory_order_acquire);

            if (buy_idx < sell_idx) return;

            Order* buy_order = find_and_clean_dead(m_buys[buy_idx]);
            Order* sell_order = find_and_clean_dead(m_sells[sell_idx]);

            if (!buy_order) {
                int next_idx = buy_idx - 1;
                if (next_idx < 0) return;
                m_max_buy_idx.compare_exchange_strong(buy_idx, next_idx);
                continue;
            }

            if (!sell_order) {
                int next_idx = sell_idx + 1;
                if (next_idx >= MAX_PRICE_LEVELS) return;
                m_min_sell_idx.compare_exchange_strong(sell_idx, next_idx);
                continue;
            }

            Quantity buy_qty = buy_order->quantity.load(std::memory_order_acquire);
            Quantity sell_qty = sell_order->quantity.load(std::memory_order_acquire);

            if (buy_qty == 0 || sell_qty == 0) continue;

            Quantity match_qty = std::min(buy_qty, sell_qty);
            
            if (buy_order->quantity.compare_exchange_strong(buy_qty, buy_qty - match_qty)) {
                if (sell_order->quantity.compare_exchange_strong(sell_qty, sell_qty - match_qty)) {
                    m_trades.push({index_to_price(buy_idx), match_qty, Side::Buy, 0, 0});
                } else {
                    buy_order->quantity.fetch_add(match_qty);
                }
            }
        }
    }

    bool get_trade(Trade& t) {
        return m_trades.pop(t);
    }

    Quantity get_volume_at_price(Side side, Price price) const {
        int index = price_to_index(price);
        if (index < 0 || index >= MAX_PRICE_LEVELS) return 0;

        auto& levels = (side == Side::Buy) ? m_buys : m_sells;
        const PriceLevel& level = levels[index];

        Quantity total = 0;
        TaggedPtr current = level.head.load(std::memory_order_acquire);
        while (current.ptr<Order>()) {
            total += current.ptr<Order>()->quantity.load(std::memory_order_relaxed);
            current = current.ptr<Order>()->next.load(std::memory_order_acquire);
        }
        return total;
    }

private:
    struct alignas(64) PriceLevel {
        std::atomic<TaggedPtr> head;
    };

    /**
     * Traverses the list and removes nodes that have 0 quantity.
     * This is the "Garbage Collection" phase.
     */
    Order* find_and_clean_dead(PriceLevel& level) {
        TaggedPtr head = level.head.load(std::memory_order_acquire);
        
        while (head.ptr<Order>()) {
            Order* head_node = head.ptr<Order>();
            if (head_node->quantity.load(std::memory_order_acquire) > 0) {
                return head_node;
            }
            
            // Head is dead. Try to pop it.
            TaggedPtr next = head_node->next.load(std::memory_order_acquire);
            if (level.head.compare_exchange_weak(head, next, 
                                               std::memory_order_release, 
                                               std::memory_order_acquire)) {
                // Successfully popped! Return to pool.
                m_pool.release(head_node);
                head = next;
            } else {
                // CAS failed, head was changed by someone else. Retry.
                head = level.head.load(std::memory_order_acquire);
            }
        }
        return nullptr;
    }

    int price_to_index(Price p) const {
        return static_cast<int>(std::round((p - MIN_PRICE) / TICK_SIZE));
    }

    Price index_to_price(int idx) const {
        return MIN_PRICE + idx * TICK_SIZE;
    }

    std::array<PriceLevel, MAX_PRICE_LEVELS> m_buys;
    std::array<PriceLevel, MAX_PRICE_LEVELS> m_sells;
    
    alignas(64) std::atomic<int> m_max_buy_idx;
    alignas(64) std::atomic<int> m_min_sell_idx;
    
    OrderPool m_pool;
    SPSCQueue<Trade> m_trades;
};
