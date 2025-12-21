#pragma once

#include "Types.hpp"
#include <array>
#include <mutex>
#include <vector>

/**
 * A standard mutex-based Order Book for comparison.
 */
class LockedOrderBook {
public:
    static constexpr int MAX_PRICE_LEVELS = 10000;
    static constexpr double TICK_SIZE = 0.01;
    static constexpr double MIN_PRICE = 50.00;

    void add_order(Side side, Price price, Quantity quantity) {
        int index = price_to_index(price);
        if (index < 0 || index >= MAX_PRICE_LEVELS) return;

        std::lock_guard<std::mutex> lock(m_mutexes[index]);
        auto& levels = (side == Side::Buy) ? m_buys : m_sells;
        levels[index].push_back({price, quantity, side});
    }

    void match() {
        // Simple stub for benchmark compatibility
    }

    bool get_trade(struct Trade& t) {
        return false;
    }

private:
    struct SimpleOrder {
        Price price;
        Quantity quantity;
        Side side;
    };

    int price_to_index(Price p) const {
        return static_cast<int>((p - MIN_PRICE) / TICK_SIZE);
    }

    std::array<std::vector<SimpleOrder>, MAX_PRICE_LEVELS> m_buys;
    std::array<std::vector<SimpleOrder>, MAX_PRICE_LEVELS> m_sells;
    std::array<std::mutex, MAX_PRICE_LEVELS> m_mutexes;
};
