#include "utils/http_util.h"

#include <sys/socket.h>

#include <cstdlib>
#include <sstream>

namespace socketIoServer::utils {

std::string makeHttpResponse(const std::string& status, const std::string& body)
{
  std::ostringstream oss;
  oss << "HTTP/1.1 " << status << "\r\n";
  oss << "Content-Type: text/plain; charset=UTF-8\r\n";
  oss << "Content-Length: " << body.size() << "\r\n";
  oss << "Connection: close\r\n";
  oss << "\r\n";
  oss << body;
  return oss.str();
}

bool readHttpRequestFromSocket(int clientFd, std::string& request)
{
  request.clear();
  char buffer[4096];

  while (true) {
    const ssize_t readBytes = ::recv(clientFd, buffer, sizeof(buffer), 0);
    if (readBytes <= 0) {
      return false;
    }
    request.append(buffer, static_cast<std::size_t>(readBytes));
    if (request.find("\r\n\r\n") != std::string::npos) {
      break;
    }
  }

  std::size_t contentLength = 0;
  const std::size_t headerEnd = request.find("\r\n\r\n");
  const std::string headers = request.substr(0, headerEnd + 4);
  const std::string key = "Content-Length:";
  const std::size_t keyPos = headers.find(key);
  if (keyPos != std::string::npos) {
    std::size_t valueStart = keyPos + key.size();
    while (valueStart < headers.size() && headers[valueStart] == ' ') {
      ++valueStart;
    }
    std::size_t valueEnd = valueStart;
    while (valueEnd < headers.size() && headers[valueEnd] >= '0' && headers[valueEnd] <= '9') {
      ++valueEnd;
    }
    if (valueEnd > valueStart) {
      contentLength = static_cast<std::size_t>(
          std::strtoull(headers.substr(valueStart, valueEnd - valueStart).c_str(), nullptr, 10));
    }
  }

  const std::size_t bodyStart = headerEnd + 4;
  while (request.size() < bodyStart + contentLength) {
    const ssize_t readBytes = ::recv(clientFd, buffer, sizeof(buffer), 0);
    if (readBytes <= 0) {
      return false;
    }
    request.append(buffer, static_cast<std::size_t>(readBytes));
  }

  return true;
}

std::string parseRequestMethod(const std::string& request)
{
  const std::size_t firstSpace = request.find(' ');
  if (firstSpace == std::string::npos) {
    return "";
  }
  return request.substr(0, firstSpace);
}

std::string parseRequestTarget(const std::string& request)
{
  const std::size_t firstSpace = request.find(' ');
  if (firstSpace == std::string::npos) {
    return "";
  }
  const std::size_t secondSpace = request.find(' ', firstSpace + 1);
  if (secondSpace == std::string::npos) {
    return "";
  }
  return request.substr(firstSpace + 1, secondSpace - firstSpace - 1);
}

std::string parseRequestBody(const std::string& request)
{
  const std::size_t headerEnd = request.find("\r\n\r\n");
  if (headerEnd == std::string::npos) {
    return "";
  }
  return request.substr(headerEnd + 4);
}

}  // namespace socketIoServer::utils
