#pragma once

#include "Types.hpp"

struct Trade {
    Price price;
    Quantity quantity;
    Side buyer_side; // Side that initiated the trade
    uint64_t buyer_order_id; // For future expansion
    uint64_t seller_order_id;
};

