#pragma once

#include <deque>
#include <string>
#include <vector>

namespace socketIoServer::utils {

std::vector<std::string> splitEngineIoPayload(const std::string& payload);
std::string joinEngineIoPayload(const std::deque<std::string>& packets);

}  // namespace socketIoServer::utils
