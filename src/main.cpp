#include "OrderBook.hpp"
#include "OrderRing.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <atomic>
#include <cstdint>

using Clock = std::chrono::high_resolution_clock;

// Stop the optimizer from deleting a "useless" loop whose result we discard.
template <typename T>
static inline void do_not_optimize(T const& value) {
    asm volatile("" : : "r,m"(value) : "memory");
}

static inline int32_t price_to_ticks(double p) {
    return static_cast<int32_t>((p - OrderBook::MIN_PRICE) / OrderBook::TICK_SIZE + 0.5);
}

static size_t next_pow2(size_t n) {
    size_t c = 1;
    while (c < n) c <<= 1;
    return c;
}

// ---------------------------------------------------------------------------
// (1) SUBMISSION LATENCY. The client-facing insert is a wait-free SPSC ring
// enqueue (no CAS), carrying an integer-tick order. That is the hot path a
// trading client actually hits; matching happens asynchronously on the matcher
// thread.
//
// On Apple Silicon the steady clock ticks at ~42 ns, far too coarse to time a
// single few-ns op, so we time a whole batch of N enqueues with one clock read
// at each end and report the mean (total / N). That amortizes the clock away.
// ---------------------------------------------------------------------------
void run_submit_latency_benchmark(int n) {
    OrderRing ring(next_pow2(static_cast<size_t>(n) + 1));
    const OrderMsg msg{price_to_ticks(100.05), 10, Side::Buy};

    // Warm the pages and caches on the very buffer we are about to time.
    for (int i = 0; i < n; i++) ring.push(msg);
    ring.reset();

    auto t0 = Clock::now();
    for (int i = 0; i < n; i++) do_not_optimize(ring.push(msg));
    auto t1 = Clock::now();

    double total_ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    std::cout << "[submit latency] wait-free SPSC enqueue, integer ticks (no CAS)\n";
    std::cout << "  ops:              " << n << "\n";
    std::cout << "  mean:             " << std::fixed << std::setprecision(2)
              << (total_ns / n) << " ns/order\n";
    std::cout << "  submission rate:  " << std::setprecision(2)
              << (n / (total_ns / 1e9) / 1e6) << " M orders/sec\n";
    std::cout << "------------------------------------------\n";
}

// ---------------------------------------------------------------------------
// (2) END-TO-END THROUGHPUT. Two producers submit orders through their own
// rings; one matcher drains both rings into the book and matches; one consumer
// drains trades. Timed from first submission to the matcher fully draining.
// ---------------------------------------------------------------------------
void run_throughput_benchmark(int n_per_producer) {
    OrderBook book(static_cast<size_t>(n_per_producer) * 6 + 100000);
    OrderRing buy_ring(1 << 16);
    OrderRing sell_ring(1 << 16);

    std::atomic<bool> producers_done{false};
    std::atomic<bool> consumer_run{true};
    std::atomic<uint64_t> trades{0};

    std::thread matcher([&]() {
        OrderMsg m;
        while (true) {
            bool did = false;
            if (buy_ring.pop(m))  { book.add_order_at(m.side, m.price_ticks, m.quantity); did = true; }
            if (sell_ring.pop(m)) { book.add_order_at(m.side, m.price_ticks, m.quantity); did = true; }
            book.match();
            if (!did && producers_done.load(std::memory_order_acquire)) {
                while (buy_ring.pop(m))  book.add_order_at(m.side, m.price_ticks, m.quantity);
                while (sell_ring.pop(m)) book.add_order_at(m.side, m.price_ticks, m.quantity);
                book.match();
                break;
            }
        }
    });

    std::thread consumer([&]() {
        Trade t;
        while (consumer_run.load(std::memory_order_acquire)) {
            if (book.get_trade(t)) trades.fetch_add(1, std::memory_order_relaxed);
            else std::this_thread::yield();
        }
        while (book.get_trade(t)) trades.fetch_add(1, std::memory_order_relaxed);
    });

    const OrderMsg bmsg{price_to_ticks(100.05), 10, Side::Buy};
    const OrderMsg smsg{price_to_ticks(100.05), 10, Side::Sell};

    auto start = Clock::now();
    std::thread buyer([&]() {
        for (int i = 0; i < n_per_producer; i++) while (!buy_ring.push(bmsg)) {}
    });
    std::thread seller([&]() {
        for (int i = 0; i < n_per_producer; i++) while (!sell_ring.push(smsg)) {}
    });
    buyer.join();
    seller.join();
    producers_done.store(true, std::memory_order_release);
    matcher.join();
    auto end = Clock::now();

    consumer_run.store(false, std::memory_order_release);
    consumer.join();

    double secs = std::chrono::duration<double>(end - start).count();
    uint64_t total = static_cast<uint64_t>(n_per_producer) * 2;
    std::cout << "[throughput] 2 producers -> rings -> 1 matcher (submit + match)\n";
    std::cout << "  orders processed: " << total << "\n";
    std::cout << "  trades executed:  " << trades.load() << "\n";
    std::cout << "  time:             " << std::fixed << std::setprecision(6) << secs << " s\n";
    std::cout << "  throughput:       " << std::setprecision(2)
              << (total / secs / 1e6) << " M orders/sec\n";
    std::cout << "------------------------------------------\n";
}

// ---------------------------------------------------------------------------
// (3) FULL BOOK-INSERT LATENCY (for reference). Cost of placing an order into
// the matched FIFO book on the matcher thread: pool acquire + tail enqueue CAS.
// This is the heavier path behind the ring; reported so the two are not
// conflated.
// ---------------------------------------------------------------------------
void run_book_insert_latency(int n) {
    OrderBook book(static_cast<size_t>(n) + 100000);
    const int idx = price_to_ticks(100.05);
    for (int i = 0; i < 10000; i++) book.add_order_at(Side::Buy, idx, 10);  // warmup

    auto t0 = Clock::now();
    for (int i = 0; i < n; i++) do_not_optimize(book.add_order_at(Side::Buy, idx, 10));
    auto t1 = Clock::now();

    double total_ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    std::cout << "[book insert latency] lock-free FIFO enqueue into price level\n";
    std::cout << "  mean:             " << std::fixed << std::setprecision(2)
              << (total_ns / n) << " ns/order\n";
    std::cout << "------------------------------------------\n";
}

int main() {
    const int num_orders = 1000000;
    run_submit_latency_benchmark(num_orders);
    run_throughput_benchmark(num_orders);
    run_book_insert_latency(num_orders);
    return 0;
}
