#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace socketIoServer::utils {

std::string getHttpHeader(const std::string& request, const std::string& headerName);
std::string computeWebSocketAccept(const std::string& secWebSocketKey);
bool sendWebSocketTextFrame(int fd, const std::string& payload);
bool readWebSocketTextFrame(int fd, std::string& payloadOut);

}  // namespace socketIoServer::utils
