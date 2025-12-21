cmake_minimum_required(VERSION 3.10)
project(LockFreeOrderBook)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optimization flags for HFT
if(MSVC)
    add_compile_options(/O2 /Oi /Ot /GL)
else()
    add_compile_options(-O3 -march=native -Wall -Wextra -pthread)
endif()

include_directories(include)

add_executable(OrderBook
    src/main.cpp
)

