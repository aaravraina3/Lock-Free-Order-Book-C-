#include "OrderBook.hpp"
#include "LockedOrderBook.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <atomic>

void run_lock_free_benchmark(int num_orders) {
    OrderBook lf_book(num_orders * 3);
    std::cout << "Starting Lock-Free Order Book with Matching..." << std::endl;

    std::atomic<bool> running{true};
    std::atomic<uint64_t> trades_executed{0};
    std::atomic<uint64_t> total_volume_traded{0};

    // 1. Matching Thread
    std::thread matcher([&]() {
        while (running) {
            lf_book.match();
            std::this_thread::yield();
        }
        // Final drain
        lf_book.match();
    });

    // 2. Trade Consumer Thread
    std::thread consumer([&]() {
        Trade t;
        while (true) {
            if (lf_book.get_trade(t)) {
                trades_executed.fetch_add(1, std::memory_order_relaxed);
                total_volume_traded.fetch_add(t.quantity, std::memory_order_relaxed);
            } else if (!running) {
                break;
            } else {
                std::this_thread::yield();
            }
        }
    });

    auto start = std::chrono::high_resolution_clock::now();

    // 3. Producer Threads
    std::thread buyer([&]() {
        for (int i = 0; i < num_orders; i++) {
            lf_book.add_order(Side::Buy, 100.05, 10);
        }
    });

    std::thread seller([&]() {
        for (int i = 0; i < num_orders; i++) {
            lf_book.add_order(Side::Sell, 100.05, 10);
        }
    });

    buyer.join();
    seller.join();
    
    // Give time for matching to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    running = false;
    matcher.join();
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "Lock-Free Results:" << std::endl;
    std::cout << "Total Orders:     " << num_orders * 2 << std::endl;
    std::cout << "Trades Executed:  " << trades_executed.load() << std::endl;
    std::cout << "Volume Traded:    " << total_volume_traded.load() << std::endl;
    std::cout << "Time Taken:       " << std::fixed << std::setprecision(6) << diff.count() << " seconds" << std::endl;
    std::cout << "Throughput:       " << std::fixed << std::setprecision(2) << (num_orders * 2 / diff.count() / 1e6) << " million orders/sec" << std::endl;
    std::cout << "------------------------------------------" << std::endl;
}

int main() {
    const int num_orders = 1000000;
    run_lock_free_benchmark(num_orders);
    return 0;
}
