#include "server.h"
#include "constants.h"
#include "utils/http_util.h"
#include "utils/url_util.h"
#include "utils/websocket_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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

  std::vector<int> webSocketFds;
  {
    std::lock_guard<std::mutex> lock(webSocketClientsMutex);
    webSocketFds.reserve(webSocketClients.size());
    for (const auto& entry : webSocketClients) {
      webSocketFds.push_back(entry.first);
    }
  }

  for (const int clientFd : webSocketFds) {
    (void)utils::sendWebSocketControlFrame(clientFd, utils::WebSocketOpcode::close);
    ::shutdown(clientFd, SHUT_RDWR);
    ::close(clientFd);
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
    const auto onlineTtl = sessionTtl();
    const auto recoveryTtl = std::chrono::milliseconds(config.sessionRecoveryMs);
    std::vector<std::string> expiredSids;
    std::vector<PendingAckState> expiredAcks;

    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      for (const auto& entry : sessions) {
        const SessionState& session = entry.second;
        if (session.online) {
          const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(now - session.lastSeenAt);
          if (idle > onlineTtl) {
            expiredSids.push_back(entry.first);
          }
          continue;
        }

        if (session.disconnectedAt != std::chrono::steady_clock::time_point::min()) {
          const auto offline = std::chrono::duration_cast<std::chrono::milliseconds>(now - session.disconnectedAt);
          if (offline > recoveryTtl) {
            expiredSids.push_back(entry.first);
          }
        }
      }
    }

    {
      std::lock_guard<std::mutex> lock(pendingAcksMutex);
      for (auto it = pendingAcks.begin(); it != pendingAcks.end();) {
        if (it->second.expiresAt <= now) {
          expiredAcks.push_back(std::move(it->second));
          it = pendingAcks.erase(it);
          continue;
        }
        ++it;
      }
    }

    for (const std::string& sid : expiredSids) {
      removeSession(sid);
    }

    for (const PendingAckState& ackState : expiredAcks) {
      if (ackState.callback) {
        ackState.callback(false, "");
      }
    }
  }
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
      response = utils::makeHttpResponse(constants::kHttpStatusNotFound, constants::kBodyNotFound);
    }
    (void)::send(clientFd, response.c_str(), response.size(), 0);
  }

  ::shutdown(clientFd, SHUT_RDWR);
  ::close(clientFd);
}

}  // namespace socketIoServer
