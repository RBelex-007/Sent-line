cmake_minimum_required(VERSION 3.15)
project(SentLineCPP)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages installed via vcpkg
find_package(RdKafka CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

add_executable(ingest ingest.cpp)
target_link_libraries(ingest PRIVATE RdKafka::rdkafka++ nlohmann_json::nlohmann_json)

add_executable(engine engine.cpp)
target_link_libraries(engine PRIVATE RdKafka::rdkafka++ nlohmann_json::nlohmann_json libpqxx::pqxx)

add_executable(monitor monitor.cpp)
target_link_libraries(monitor PRIVATE RdKafka::rdkafka++ nlohmann_json::nlohmann_json)