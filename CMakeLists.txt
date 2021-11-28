cmake_minimum_required(VERSION 3.0)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(db_concurrency_control VERSION 0.1.0 LANGUAGES C CXX)

set(DB_SOURCE_DIR   ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(DB_BINARY_DIR   ${CMAKE_CURRENT_BINARY_DIR})

find_package(Threads REQUIRED)

enable_testing()

include(systems)
include(compile)

include_directories(${DB_SOURCE_DIR})

add_subdirectory(src)
