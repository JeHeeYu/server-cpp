cmake_minimum_required(VERSION 3.16)
project(socketIoServerCpp LANGUAGES CXX)

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Boost 1.75 CONFIG REQUIRED COMPONENTS json)

add_library(socketIoServerCore
  src/protocol/engine_io.cpp
  src/protocol/namespace.cpp
  src/protocol/socket_io.cpp
  src/utils/http_util.cpp
  src/utils/json_util.cpp
  src/utils/url_util.cpp
  src/utils/engine_io_util.cpp
  src/utils/websocket_util.cpp
  src/server_api.cpp
  src/server_dispatch.cpp
  src/server_lifecycle.cpp
  src/session.cpp
  src/room.cpp
  src/server_http.cpp
  src/server_ws.cpp
)

target_include_directories(socketIoServerCore
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(socketIoServerCore
  PUBLIC
    Boost::json
)

add_executable(socketIoServerCpp
  src/main.cpp
  src/server_handlers.cpp
)
target_link_libraries(socketIoServerCpp PRIVATE socketIoServerCore)
