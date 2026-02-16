#include "server.hpp"
#include "utils/http_util.hpp"
#include "utils/url_util.hpp"
#include "utils/engine_io_util.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <string>
#include <utility>

namespace socketIoServer {

constexpr const char* kSocketIoPath = "/socket.io/";

Server::Server(ServerConfig config) : config(std::move(config))
{
}

bool Server::start()
{
  if (running.load()) {
    return false;
  }

  if (config.port == 0) {
    return false;
  }

  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }

  int reuse = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(config.port);

  if (::inet_pton(AF_INET, config.host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return false;
  }

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return false;
  }

  if (::listen(fd, static_cast<int>(config.maxConnections)) < 0) {
    ::close(fd);
    return false;
  }

  listenFd = fd;
  running.store(true);
  acceptThread = std::thread(&Server::acceptLoop, this);
  return true;
}

void Server::stop()
{
  if (!running.exchange(false)) {
    return;
  }

  if (listenFd >= 0) {
    ::shutdown(listenFd, SHUT_RDWR);
    ::close(listenFd);
    listenFd = -1;
  }

  if (acceptThread.joinable()) {
    acceptThread.join();
  }
}

bool Server::isRunning() const
{
  return running.load();
}

void Server::acceptLoop()
{
  while (running.load()) {
    int clientFd = ::accept(listenFd, nullptr, nullptr);
    if (clientFd < 0) {
      if (!running.load()) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      break;
    }

    std::string request;
    if (utils::readHttpRequestFromSocket(clientFd, request)) {
      std::string response;
      const bool handled = handleHttpRequest(request, response);
      if (!handled) {
        response = utils::makeHttpResponse("404 Not Found", "not found");
      }
      (void)::send(clientFd, response.c_str(), response.size(), 0);
    }

    ::shutdown(clientFd, SHUT_RDWR);
    ::close(clientFd);
  }
}

std::string Server::createSession()
{
  const std::uint64_t id = nextSid.fetch_add(1);
  return "sid" + std::to_string(id);
}

void Server::enqueuePacket(const std::string& sid, const std::string& packet)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return;
  }
  it->second.outgoingPackets.push_back(packet);
}

bool Server::handleHttpRequest(const std::string& request, std::string& response)
{
  const std::string method = utils::parseRequestMethod(request);
  const std::string target = utils::parseRequestTarget(request);
  std::string path;
  std::string queryString;
  utils::splitTarget(target, path, queryString);

  if (path == "/") {
    response = utils::makeHttpResponse("200 OK", "socketIoServerCpp alive\n");
    return true;
  }

  if (path != kSocketIoPath && path != "/socket.io") {
    return false;
  }

  const auto query = utils::parseQuery(queryString);
  const auto eioIt = query.find("EIO");
  const auto transportIt = query.find("transport");
  if (eioIt == query.end() || transportIt == query.end()) {
    response = utils::makeHttpResponse("400 Bad Request", "missing query");
    return true;
  }
  if (eioIt->second != "4" || transportIt->second != "polling") {
    response = utils::makeHttpResponse("400 Bad Request", "unsupported transport");
    return true;
  }

  if (method == "GET") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      const std::string sid = createSession();
      SessionState session;
      session.sid = sid;
      {
        std::lock_guard<std::mutex> lock(sessionsMutex);
        sessions.emplace(sid, std::move(session));
      }
      const std::string openPacket =
          "0{\"sid\":\"" + sid +
          "\",\"upgrades\":[],\"pingInterval\":25000,\"pingTimeout\":20000,\"maxPayload\":1000000}";
      response = utils::makeHttpResponse("200 OK", openPacket);
      return true;
    }

    std::deque<std::string> pendingPackets;
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto it = sessions.find(sidIt->second);
      if (it == sessions.end()) {
        response = utils::makeHttpResponse("400 Bad Request", "unknown sid");
        response = utils::makeHttpResponse("400 Bad Request", "unknown sid");
        return true;
      }
      pendingPackets.swap(it->second.outgoingPackets);
    }

    if (pendingPackets.empty()) {
      response = utils::makeHttpResponse("200 OK", "6");
      return true;
    }

    response = utils::makeHttpResponse("200 OK", utils::joinEngineIoPayload(pendingPackets));
    return true;
  }

  if (method == "POST") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      response = utils::makeHttpResponse("400 Bad Request", "missing sid");
      return true;
    }

    std::string sid = sidIt->second;
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      if (sessions.find(sid) == sessions.end()) {
        response = utils::makeHttpResponse("400 Bad Request", "unknown sid");
        return true;
      }
    }

    const std::string body = utils::parseRequestBody(request);
    const auto packets = utils::splitEngineIoPayload(body);
    for (const std::string& packet : packets) {
      if (packet.empty()) {
        continue;
      }

      if (packet.rfind("40", 0) == 0) {
        {
          std::lock_guard<std::mutex> lock(sessionsMutex);
          auto it = sessions.find(sid);
          if (it != sessions.end()) {
            it->second.namespaceConnected = true;
          }
        }
        enqueuePacket(sid, "40{\"sid\":\"" + sid + "\"}");
        continue;
      }

      if (packet.rfind("42", 0) == 0) {
        std::size_t pos = 2;
        std::string ackId;
        while (pos < packet.size() && packet[pos] >= '0' && packet[pos] <= '9') {
          ackId.push_back(packet[pos]);
          ++pos;
        }
        const std::string payload = packet.substr(pos);
        if (payload.find("\"ping\"") != std::string::npos && !ackId.empty()) {
          enqueuePacket(sid, "43" + ackId + "[{\"ok\":true}]");
        }
        continue;
      }

      if (packet == "3") {
        continue;
      }
    }

    response = utils::makeHttpResponse("200 OK", "ok");
    return true;
  }

  response = utils::makeHttpResponse("405 Method Not Allowed", "method not allowed");
  return true;
}

}  // namespace socketIoServer
