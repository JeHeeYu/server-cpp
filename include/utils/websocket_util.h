#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace socketIoServer::utils {

enum class WebSocketOpcode {
  invalid = -1,
  continuation = 0x0,
  text = 0x1,
  binary = 0x2,
  close = 0x8,
  ping = 0x9,
  pong = 0xA
};

std::string getHttpHeader(const std::string& request, const std::string& headerName);
std::string computeWebSocketAccept(const std::string& secWebSocketKey);
bool sendWebSocketTextFrame(int fd, const std::string& payload);
bool sendWebSocketControlFrame(int fd, WebSocketOpcode opcode, const std::string& payload = "");
bool readWebSocketFrame(int fd, std::string& payloadOut, WebSocketOpcode& opcodeOut);
bool readWebSocketTextFrame(int fd, std::string& payloadOut);

}  // namespace socketIoServer::utils
