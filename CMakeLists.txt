cmake_minimum_required(VERSION 3.16)
project(socketIoServerCpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(socketIoServerCore
  src/utils/http_util.cpp
  src/utils/url_util.cpp
  src/utils/engine_io_util.cpp
  src/server.cpp
  src/server_http.cpp
)

target_include_directories(socketIoServerCore
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(socketIoServerCpp src/main.cpp)
target_link_libraries(socketIoServerCpp PRIVATE socketIoServerCore)
