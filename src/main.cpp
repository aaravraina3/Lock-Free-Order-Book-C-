#include "OrderBook.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <atomic>
#include <cstdint>

using Clock = std::chrono::high_resolution_clock;

// ---------------------------------------------------------------------------
// Throughput: 2 producer threads insert orders while a matcher + consumer run.
// The clock STOPS right after the producers finish, BEFORE the drain sleep,
// so we measure the actual insertion work and not a hardcoded 200ms wait.
// ---------------------------------------------------------------------------
void run_throughput_benchmark(int num_orders) {
    OrderBook lf_book(num_orders * 3);
    std::cout << "[throughput] 2 producers + matcher + consumer (concurrent)\n";

    std::atomic<bool> running{true};
    std::atomic<uint64_t> trades_executed{0};

    std::thread matcher([&]() {
        while (running) { lf_book.match(); std::this_thread::yield(); }
        lf_book.match();
    });

    std::thread consumer([&]() {
        Trade t;
        while (true) {
            if (lf_book.get_trade(t)) {
                trades_executed.fetch_add(1, std::memory_order_relaxed);
            } else if (!running) {
                break;
            } else {
                std::this_thread::yield();
            }
        }
    });

    auto start = Clock::now();
    std::thread buyer([&]() {
        for (int i = 0; i < num_orders; i++) lf_book.add_order(Side::Buy, 100.05, 10);
    });
    std::thread seller([&]() {
        for (int i = 0; i < num_orders; i++) lf_book.add_order(Side::Sell, 100.05, 10);
    });
    buyer.join();
    seller.join();
    auto end = Clock::now();  // stop timing the insertion work HERE

    std::this_thread::sleep_for(std::chrono::milliseconds(200));  // untimed drain
    running = false;
    matcher.join();
    consumer.join();

    double secs = std::chrono::duration<double>(end - start).count();
    uint64_t total = static_cast<uint64_t>(num_orders) * 2;
    std::cout << "  orders inserted:    " << total << "\n";
    std::cout << "  trades executed:    " << trades_executed.load() << "\n";
    std::cout << "  time (insert only): " << std::fixed << std::setprecision(6) << secs << " s\n";
    std::cout << "  throughput:         " << std::setprecision(2)
              << (total / secs / 1e6) << " M orders/sec\n";
    std::cout << "------------------------------------------\n";
}

// ---------------------------------------------------------------------------
// Latency: single thread, time each add_order individually. No sleep, no
// contention. Reports a real per-operation distribution.
// ---------------------------------------------------------------------------
void run_latency_benchmark(int num_orders) {
    OrderBook book(static_cast<size_t>(num_orders) + 100000);
    std::cout << "[latency] single thread, per-op add_order timing\n";

    // Measure the stopwatch's own cost so the numbers below are honest.
    {
        auto a = Clock::now();
        volatile uint64_t sink = 0;
        for (int i = 0; i < 100000; i++) { sink += Clock::now().time_since_epoch().count(); }
        auto b = Clock::now();
        double ov = std::chrono::duration<double, std::nano>(b - a).count() / 100000.0;
        std::cout << "  (stopwatch self-cost ~" << std::setprecision(1) << ov << " ns/read)\n";
    }

    // Warmup so the pool / branch predictors are hot.
    for (int i = 0; i < 10000; i++) book.add_order(Side::Buy, 100.05, 10);

    std::vector<double> samples;
    samples.reserve(num_orders);

    auto t0 = Clock::now();
    for (int i = 0; i < num_orders; i++) {
        auto a = Clock::now();
        book.add_order(Side::Buy, 100.05, 10);
        auto b = Clock::now();
        samples.push_back(std::chrono::duration<double, std::nano>(b - a).count());
    }
    auto t1 = Clock::now();

    double total_s = std::chrono::duration<double>(t1 - t0).count();
    std::sort(samples.begin(), samples.end());
    auto pct = [&](double q) { return samples[static_cast<size_t>(q * (samples.size() - 1))]; };

    std::cout << "  ops:                " << num_orders << "\n";
    std::cout << "  mean (total/n):     " << std::setprecision(1)
              << (total_s * 1e9 / num_orders) << " ns\n";
    std::cout << "  p50: " << pct(0.50) << " ns   p99: " << pct(0.99)
              << " ns   p99.9: " << pct(0.999) << " ns\n";
    std::cout << "  min: " << samples.front() << " ns   max: " << samples.back() << " ns\n";
    std::cout << "  single-thread tput: " << std::setprecision(2)
              << (num_orders / total_s / 1e6) << " M orders/sec\n";
    std::cout << "------------------------------------------\n";
}

int main() {
    const int num_orders = 1000000;
    run_throughput_benchmark(num_orders);
    run_latency_benchmark(num_orders);
    return 0;
}
