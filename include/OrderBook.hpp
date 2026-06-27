#pragma once

#include "Types.hpp"
#include "OrderPool.hpp"
#include "Trade.hpp"
#include "SPSCQueue.hpp"
#include <array>
#include <atomic>
#include <cmath>
#include <algorithm>
#include <vector>

/**
 * A lock-free order book with price-time priority.
 *
 * Price priority comes from the best-bid / best-ask index hints. Time priority
 * comes from making each price level a lock-free FIFO queue (Michael & Scott):
 * orders are enqueued at the tail and matched from the head, so the oldest
 * resting order at a price fills first. Each level keeps a sentinel node so
 * head and tail are never null. TaggedPtr carries a per-node version to defeat
 * ABA on reuse. Model is multi-producer (order submitters) / single-consumer
 * (one matching thread), which is the configuration the benchmark exercises.
 */
class OrderBook {
public:
    static constexpr int MAX_PRICE_LEVELS = 10000;
    static constexpr double TICK_SIZE = 0.01;
    static constexpr double MIN_PRICE = 50.00;

    OrderBook(size_t pool_size) : m_pool(pool_size), m_trades(10000) {
        // Each level starts with a sentinel so head/tail are never null.
        for (auto& level : m_buys) init_level(level, Side::Buy);
        for (auto& level : m_sells) init_level(level, Side::Sell);
        m_max_buy_idx.store(0, std::memory_order_relaxed);
        m_min_sell_idx.store(MAX_PRICE_LEVELS - 1, std::memory_order_relaxed);
    }

    // Enqueue at the tail (newest), preserving arrival order. Lock-free, MP-safe.
    bool add_order(Side side, Price price, Quantity quantity) {
        int index = price_to_index(price);
        if (index < 0 || index >= MAX_PRICE_LEVELS) return false;

        Order* node = m_pool.acquire(price, quantity, side);
        if (!node) return false;
        node->next.store(TaggedPtr(0), std::memory_order_relaxed);

        if (side == Side::Buy) {
            int current_max = m_max_buy_idx.load(std::memory_order_relaxed);
            while (index > current_max && !m_max_buy_idx.compare_exchange_weak(current_max, index));
        } else {
            int current_min = m_min_sell_idx.load(std::memory_order_relaxed);
            while (index < current_min && !m_min_sell_idx.compare_exchange_weak(current_min, index));
        }

        PriceLevel& level = (side == Side::Buy ? m_buys : m_sells)[index];
        TaggedPtr new_node = TaggedPtr::make(node, node->tag);

        while (true) {
            TaggedPtr tail = level.tail.load(std::memory_order_acquire);
            Order* tail_node = tail.ptr<Order>();
            TaggedPtr next = tail_node->next.load(std::memory_order_acquire);

            if (tail != level.tail.load(std::memory_order_acquire)) continue; // re-read, inconsistent

            if (next.ptr<Order>() == nullptr) {
                // Tail is the real last node; try to link the new node after it.
                if (tail_node->next.compare_exchange_weak(next, new_node,
                        std::memory_order_release, std::memory_order_acquire)) {
                    // Linked. Swing tail forward (best effort; helpers may beat us).
                    level.tail.compare_exchange_strong(tail, new_node,
                        std::memory_order_release, std::memory_order_relaxed);
                    return true;
                }
            } else {
                // Tail is lagging behind a linked node; help advance it.
                level.tail.compare_exchange_strong(tail, next,
                    std::memory_order_release, std::memory_order_relaxed);
            }
        }
    }

    void match() {
        while (true) {
            int buy_idx = m_max_buy_idx.load(std::memory_order_acquire);
            int sell_idx = m_min_sell_idx.load(std::memory_order_acquire);

            if (buy_idx < sell_idx) return;

            Order* buy_order = peek_oldest(m_buys[buy_idx]);
            if (!buy_order) {
                int next_idx = buy_idx - 1;
                if (next_idx < 0) return;
                m_max_buy_idx.compare_exchange_strong(buy_idx, next_idx);
                continue;
            }

            Order* sell_order = peek_oldest(m_sells[sell_idx]);
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

        const PriceLevel& level = (side == Side::Buy ? m_buys : m_sells)[index];

        Quantity total = 0;
        // Skip the leading sentinel; sum the live orders.
        TaggedPtr current = level.head.load(std::memory_order_acquire);
        current = current.ptr<Order>()->next.load(std::memory_order_acquire);
        while (current.ptr<Order>()) {
            total += current.ptr<Order>()->quantity.load(std::memory_order_relaxed);
            current = current.ptr<Order>()->next.load(std::memory_order_acquire);
        }
        return total;
    }

    // Resting orders at a price, oldest -> newest. Test/inspection helper.
    std::vector<Quantity> snapshot(Side side, Price price) const {
        std::vector<Quantity> out;
        int index = price_to_index(price);
        if (index < 0 || index >= MAX_PRICE_LEVELS) return out;

        const PriceLevel& level = (side == Side::Buy ? m_buys : m_sells)[index];
        TaggedPtr current = level.head.load(std::memory_order_acquire);
        current = current.ptr<Order>()->next.load(std::memory_order_acquire); // skip sentinel
        while (current.ptr<Order>()) {
            out.push_back(current.ptr<Order>()->quantity.load(std::memory_order_relaxed));
            current = current.ptr<Order>()->next.load(std::memory_order_acquire);
        }
        return out;
    }

private:
    struct alignas(64) PriceLevel {
        std::atomic<TaggedPtr> head; // dequeue end: sentinel, oldest live order is head->next
        std::atomic<TaggedPtr> tail; // enqueue end: newest order
    };

    void init_level(PriceLevel& level, Side side) {
        Order* sentinel = m_pool.acquire(0.0, 0, side);
        sentinel->next.store(TaggedPtr(0), std::memory_order_relaxed);
        TaggedPtr sp = TaggedPtr::make(sentinel, sentinel->tag);
        level.head.store(sp, std::memory_order_relaxed);
        level.tail.store(sp, std::memory_order_relaxed);
    }

    /**
     * Return the oldest live (quantity > 0) order at this level, or nullptr if
     * none. Fully-filled orders at the head are dequeued and reclaimed lazily:
     * the spent order becomes the new sentinel and the old sentinel is freed.
     * Only the single matching thread calls this.
     */
    Order* peek_oldest(PriceLevel& level) {
        while (true) {
            TaggedPtr head = level.head.load(std::memory_order_acquire);
            TaggedPtr tail = level.tail.load(std::memory_order_acquire);
            Order* head_node = head.ptr<Order>();
            TaggedPtr next = head_node->next.load(std::memory_order_acquire);
            Order* next_node = next.ptr<Order>();

            if (head != level.head.load(std::memory_order_acquire)) continue; // inconsistent

            if (next_node == nullptr) {
                return nullptr; // only the sentinel: no live orders
            }

            if (head == tail) {
                // Tail is lagging behind a freshly linked node; help it, then retry.
                level.tail.compare_exchange_strong(tail, next,
                    std::memory_order_release, std::memory_order_relaxed);
                continue;
            }

            if (next_node->quantity.load(std::memory_order_acquire) > 0) {
                return next_node; // oldest live order
            }

            // next is spent: advance head past the sentinel and reclaim it.
            // head != tail here, so head_node is safe to free.
            if (level.head.compare_exchange_weak(head, next,
                    std::memory_order_release, std::memory_order_acquire)) {
                m_pool.release(head_node);
            }
        }
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
