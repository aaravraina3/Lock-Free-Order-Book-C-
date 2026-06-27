// Proves price-time (FIFO) priority: at a single price level the OLDEST
// resting order fills first. A LIFO book would fail these assertions.
#include "OrderBook.hpp"
#include <cassert>
#include <iostream>
#include <vector>

static void print(const char* label, const std::vector<Quantity>& v) {
    std::cout << label;
    for (auto q : v) std::cout << q << " ";
    std::cout << "\n";
}

int main() {
    OrderBook book(100000);

    // Three buys at the SAME price, arriving A -> B -> C.
    book.add_order(Side::Buy, 100.05, 10); // A (oldest)
    book.add_order(Side::Buy, 100.05, 20); // B
    book.add_order(Side::Buy, 100.05, 30); // C (newest)

    auto resting = book.snapshot(Side::Buy, 100.05);
    print("resting buys (oldest->newest): ", resting);
    // FIFO => [10,20,30]; a LIFO book would show [30,20,10].
    assert((resting == std::vector<Quantity>{10, 20, 30}) && "orders must rest in arrival order");

    // A sell of qty 10 arrives and matches.
    book.add_order(Side::Sell, 100.05, 10);
    book.match();

    uint64_t trades = 0, vol = 0;
    Trade t;
    while (book.get_trade(t)) { trades++; vol += t.quantity; }

    auto after = book.snapshot(Side::Buy, 100.05);
    print("after matching qty 10:        ", after);
    std::cout << "trades=" << trades << " volume=" << vol << "\n";
    // Time priority: the OLDEST buy (A, qty 10) fills fully and leaves; B and C
    // are untouched => [20,30]. A LIFO book would have hit C first => [10,20,20].
    assert((after == std::vector<Quantity>{20, 30}) && "oldest order must fill first");
    assert(trades == 1 && vol == 10);

    std::cout << "PASS: price-time (FIFO) priority verified\n";
    return 0;
}
