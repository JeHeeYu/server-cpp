#pragma once

#include <string>
#include <unordered_map>

namespace socketIoServer::utils {

void splitTarget(const std::string& target, std::string& path, std::string& query);
std::unordered_map<std::string, std::string> parseQuery(const std::string& query);

}  // namespace socketIoServer::utils
