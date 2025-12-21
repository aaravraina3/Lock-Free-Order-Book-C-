#pragma once

#include <atomic>
#include <cstdint>

/**
 * A Tagged Pointer to solve the ABA problem.
 * Uses the upper 16 bits of a 64-bit pointer for a version tag.
 */
struct TaggedPtr {
    uintptr_t bits;

    TaggedPtr() : bits(0) {}
    TaggedPtr(uintptr_t b) : bits(b) {}

    static TaggedPtr make(void* p, uint16_t tag) {
        return TaggedPtr(reinterpret_cast<uintptr_t>(p) | (static_cast<uintptr_t>(tag) << 48));
    }

    template<typename T>
    T* ptr() const {
        // Mask out the upper 16 bits to get the original address
        return reinterpret_cast<T*>(bits & 0x0000FFFFFFFFFFFF);
    }

    uint16_t tag() const {
        return static_cast<uint16_t>(bits >> 48);
    }

    bool operator==(const TaggedPtr& other) const { return bits == other.bits; }
    bool operator!=(const TaggedPtr& other) const { return bits != other.bits; }
};

enum class Side : uint8_t {
    Buy,
    Sell
};

using Price = double;
using Quantity = uint32_t;

struct Order {
    Price price;
    std::atomic<Quantity> quantity;
    Side side;
    std::atomic<TaggedPtr> next{TaggedPtr()};
    uint16_t tag{0}; // The current tag for THIS node instance

    Order() : price(0.0), quantity(0), side(Side::Buy) {}
    Order(Price p, Quantity q, Side s) : price(p), quantity(q), side(s) {}
    
    Order(const Order&) = delete;
    Order& operator=(const Order&) = delete;
};
