#include "server.h"
#include "utils/http_util.h"
#include "utils/url_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

namespace socketIoServer {

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
  sessionThread = std::thread(&Server::sessionLoop, this);
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

  if (sessionThread.joinable()) {
    sessionThread.join();
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

    std::thread(&Server::handleClient, this, clientFd).detach();
  }
}

void Server::sessionLoop()
{
  while (running.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto now = std::chrono::steady_clock::now();
    const auto ttl = sessionTtl();

    std::lock_guard<std::mutex> lock(sessionsMutex);
    for (auto it = sessions.begin(); it != sessions.end();) {
      const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.lastSeenAt);
      if (idle > ttl) {
        it = sessions.erase(it);
      } else {
        ++it;
      }
    }
  }
}

std::string Server::createSession()
{
  const std::uint64_t id = nextSid.fetch_add(1);
  return "sid" + std::to_string(id);
}

bool Server::hasSession(const std::string& sid)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  return sessions.find(sid) != sessions.end();
}

void Server::touchSession(const std::string& sid)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it != sessions.end()) {
    it->second.lastSeenAt = std::chrono::steady_clock::now();
  }
}

void Server::removeSession(const std::string& sid)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  sessions.erase(sid);
}

std::chrono::milliseconds Server::sessionTtl() const
{
  return std::chrono::milliseconds(
      static_cast<std::uint64_t>(config.pingIntervalMs) + static_cast<std::uint64_t>(config.pingTimeoutMs));
}

void Server::enqueuePacket(const std::string& sid, const std::string& packet)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return;
  }
  it->second.lastSeenAt = std::chrono::steady_clock::now();
  it->second.outgoingPackets.push_back(packet);
}

void Server::handleClient(int clientFd)
{
  std::string request;
  if (utils::readHttpRequestFromSocket(clientFd, request)) {
    const std::string target = utils::parseRequestTarget(request);
    std::string path;
    std::string queryString;
    utils::splitTarget(target, path, queryString);
    const auto query = utils::parseQuery(queryString);
    if (handleWebSocketHandshake(clientFd, request, query)) {
      ::shutdown(clientFd, SHUT_RDWR);
      ::close(clientFd);
      return;
    }

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

}  // namespace socketIoServer
