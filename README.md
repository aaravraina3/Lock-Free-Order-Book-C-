# Lock-Free Order Book with Matching Engine

A high-performance order book that handles millions of orders per second without locks. Built to understand the concurrency primitives that power real trading systems.

## The Problem

Trading systems need to:
1. Accept orders from multiple sources simultaneously
2. Match crossing orders (buy ≥ sell price)
3. Never block, never crash, never corrupt data

Mutexes are too slow. A lock acquisition can take 20-50ns - an eternity when you're competing on microseconds. This project implements a fully lock-free solution.

## Performance

```
Lock-Free Throughput: ~114 million orders/sec (insertion only)
Lock-Free Throughput: ~9 million orders/sec (with matching + GC)
Mutex Throughput:     ~76 million orders/sec (insertion only)

Average latency: 8-10 nanoseconds per order
```

The matching throughput is lower because we're doing more work - actually executing trades and reclaiming memory, not just inserting.

## How It Works

### The Core Data Structure

```
Price Level 100.05:  [Order A] → [Order B] → [Order C] → nullptr
Price Level 100.04:  [Order D] → nullptr
Price Level 100.03:  (empty)
```

Each price level is a lock-free linked list. Orders are inserted at the head using compare-and-swap (CAS).

### Lock-Free Insertion

```cpp
Order* old_head = level.head.load();
do {
    new_order->next = old_head;
} while (!level.head.compare_exchange_weak(old_head, new_order));
```

CAS says: "If head is still what I think it is, swap it to my new order. Otherwise, tell me what it actually is and I'll retry."

No locks. No blocking. Multiple threads can insert simultaneously.

### The Matching Engine

A dedicated thread spins looking for crosses:

```cpp
void match() {
    while (true) {
        int buy_idx = m_max_buy_idx.load();   // Best bid
        int sell_idx = m_min_sell_idx.load(); // Best ask
        
        if (buy_idx < sell_idx) return;  // No cross
        
        // Find active orders, match them atomically
        Quantity match_qty = std::min(buy_qty, sell_qty);
        buy_order->quantity.compare_exchange_strong(buy_qty, buy_qty - match_qty);
        sell_order->quantity.compare_exchange_strong(sell_qty, sell_qty - match_qty);
    }
}
```

Quantities are atomic, so the matcher can "shave" fills off orders while producers are still inserting.

### The ABA Problem

Here's a subtle bug that took me a while to understand:

```
Thread 1: Reads head = 0x1234 (Order A)
Thread 1: Gets preempted...

Thread 2: Removes Order A, returns it to pool
Thread 2: Allocates Order A again (same address!)
Thread 2: Inserts Order A back at head

Thread 1: Wakes up, CAS succeeds (head == 0x1234? yes!)
Thread 1: But A->next is now pointing somewhere completely different
```

The pointer value is identical, but the object changed. This corrupts the list.

### Tagged Pointers

The fix: pack a version counter into the pointer itself.

```cpp
struct TaggedPtr {
    uintptr_t bits;  // [16-bit tag][48-bit pointer]
    
    static TaggedPtr make(void* p, uint16_t tag) {
        return reinterpret_cast<uintptr_t>(p) | (static_cast<uintptr_t>(tag) << 48);
    }
};
```

x86-64 only uses 48 bits for addresses. We steal the upper 16 bits for a version tag. When we release an order back to the pool, we increment its tag:

```cpp
void release(Order* order) {
    order->tag++;  // Next use will have different TaggedPtr
    // ... return to pool
}
```

Now CAS compares the full 64 bits. Same pointer + different tag = CAS fails. ABA solved.

### Lock-Free Garbage Collection

Dead orders (quantity = 0) need to be removed, otherwise the matcher wastes time scanning past them.

```cpp
Order* find_and_clean_dead(PriceLevel& level) {
    TaggedPtr head = level.head.load();
    
    while (head.ptr<Order>()) {
        if (head.ptr<Order>()->quantity > 0) {
            return head.ptr<Order>();  // Found live order
        }
        
        // Pop dead order using CAS
        TaggedPtr next = head.ptr<Order>()->next.load();
        if (level.head.compare_exchange_weak(head, next)) {
            m_pool.release(head.ptr<Order>());  // Recycle with new tag
        }
    }
    return nullptr;
}
```

No locks. Dead orders get popped and recycled. The pool reuses the memory with an incremented tag.

### SPSC Trade Queue

Trades flow from the matcher to a consumer thread via a lock-free single-producer single-consumer queue:

```cpp
// Matcher (producer)
m_trades.push({price, quantity, ...});

// Consumer
Trade t;
while (m_trades.pop(t)) {
    log_trade(t);
}
```

This decouples trade logging from the matching hot path.

### Cache Line Alignment

```cpp
struct alignas(64) PriceLevel {
    std::atomic<TaggedPtr> head;
};
```

CPUs load memory in 64-byte cache lines. Without alignment, two adjacent PriceLevels might share a cache line. When Thread 1 modifies one and Thread 2 modifies the other, they'd fight over the same cache line - "false sharing."

`alignas(64)` forces each level onto its own cache line. No false sharing, no wasted cycles.

### Sharded Memory Pool

Allocating memory (`new`) is slow and unpredictable. We pre-allocate a pool:

```cpp
OrderPool pool(1000000);  // Pre-allocate 1M orders

Order* order = pool.acquire(price, qty, side);  // O(1), no syscall
pool.release(order);  // Return for reuse
```

But one pool head = contention. So we shard it:

```cpp
static constexpr size_t NUM_SHARDS = 8;
std::array<ShardHead, NUM_SHARDS> m_heads;

// Each thread hashes to a shard
size_t shard = hash(thread_id) % NUM_SHARDS;
```

8 shards = 8x less contention. This alone took throughput from 34M to 114M orders/sec.

## Thread Architecture

```
┌─────────────┐  ┌─────────────┐
│   Buyer     │  │   Seller    │   Producer threads
│   Thread    │  │   Thread    │   (add orders)
└──────┬──────┘  └──────┬──────┘
       │                │
       ▼                ▼
┌─────────────────────────────────┐
│         Order Book              │   Lock-free data structure
│   (atomic linked lists)         │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       Matcher Thread            │   Finds crosses, executes trades
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│      SPSC Trade Queue           │   Lock-free handoff
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│      Consumer Thread            │   Logs/broadcasts trades
└─────────────────────────────────┘
```

## Building

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
./OrderBook
```

Requires C++17 and a 64-bit system (x86-64 or ARM64).

## What I Learned

1. **CAS loops are the primitive** - Everything lock-free is built on compare-and-swap
2. **Memory ordering matters** - acquire/release semantics prevent subtle reordering bugs
3. **Cache lines are real** - False sharing can tank performance
4. **ABA is sneaky** - Tagged pointers are the standard fix
5. **Measure everything** - The sharding optimization came from benchmarking, not guessing

## Limitations

- Single matcher thread (could shard by price range)
- No order cancellation by ID
- No persistence
- Tagged pointers assume 48-bit address space (true on current x86-64/ARM64)

## References

- [Herlihy & Shavit - The Art of Multiprocessor Programming](https://www.amazon.com/Art-Multiprocessor-Programming-Maurice-Herlihy/dp/0124159508)
- [Preshing on Programming - Lock-Free Articles](https://preshing.com/archives/)
- [CppCon - Lock-Free Programming](https://www.youtube.com/results?search_query=cppcon+lock+free)