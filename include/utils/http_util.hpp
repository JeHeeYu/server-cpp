#pragma once

#include <string>

namespace socketIoServer::utils {

std::string makeHttpResponse(const std::string& status, const std::string& body);
bool readHttpRequestFromSocket(int clientFd, std::string& request);
std::string parseRequestMethod(const std::string& request);
std::string parseRequestTarget(const std::string& request);
std::string parseRequestBody(const std::string& request);

}  // namespace socketIoServer::utils
