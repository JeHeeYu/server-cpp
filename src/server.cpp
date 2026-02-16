#include "server.h"
#include "constants.h"
#include "protocol/types.h"
#include "utils/http_util.h"
#include "utils/url_util.h"
#include "utils/websocket_util.h"

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

void Server::setEventHandler(EventHandler handler)
{
  std::lock_guard<std::mutex> lock(eventHandlerMutex);
  eventHandler = std::move(handler);
}

void Server::setEventGuardHandler(EventGuardHandler handler)
{
  std::lock_guard<std::mutex> lock(eventGuardMutex);
  eventGuardHandler = std::move(handler);
}

void Server::setNamespaceConnectHandler(NamespaceConnectHandler handler)
{
  std::lock_guard<std::mutex> lock(connectHandlerMutex);
  namespaceConnectHandler = std::move(handler);
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

    for (const std::string& sid : expiredSids) {
      removeSession(sid);
    }
  }
}
Server::ConnectDecision Server::evaluateNamespaceConnect(
    const std::string& sid, const std::string& nsp, const std::string& authJson)
{
  NamespaceConnectHandler handlerCopy;
  {
    std::lock_guard<std::mutex> lock(connectHandlerMutex);
    handlerCopy = namespaceConnectHandler;
  }
  if (!handlerCopy) {
    return ConnectDecision{};
  }
  return handlerCopy(*this, sid, protocol::normalizeNamespace(nsp), authJson);
}

Server::ConnectDecision Server::evaluateEventGuard(
    const std::string& sid, const std::string& nsp, const std::string& eventName, const std::string& eventData)
{
  EventGuardHandler handlerCopy;
  {
    std::lock_guard<std::mutex> lock(eventGuardMutex);
    handlerCopy = eventGuardHandler;
  }
  if (!handlerCopy) {
    return ConnectDecision{};
  }
  return handlerCopy(*this, sid, protocol::normalizeNamespace(nsp), eventName, eventData);
}

std::chrono::milliseconds Server::sessionTtl() const
{
  return std::chrono::milliseconds(
      static_cast<std::uint64_t>(config.pingIntervalMs) + static_cast<std::uint64_t>(config.pingTimeoutMs));
}

void Server::dispatchSocketIoEvent(
    const std::string& sid, const std::string& packet, const std::function<void(const std::string&)>& sendPacket)
{
  protocol::SocketIoEventPacket eventPacket;
  if (!protocol::parseSocketIoEventPacket(packet, eventPacket)) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(
        "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket));
    return;
  }

  if (!isNamespaceConnected(sid, eventPacket.nsp)) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(
        eventPacket.nsp, constants::kCodeNamespaceNotConnected, constants::kMessageNamespaceNotConnected));
    return;
  }

  const std::string eventName = protocol::parseSocketIoEventName(eventPacket.eventPayload);
  const std::string eventData = protocol::parseSocketIoEventData(eventPacket.eventPayload);
  dispatchSocketIoEventData(sid, eventPacket.nsp, eventPacket.ackId, eventName, eventData, sendPacket);
}

void Server::dispatchSocketIoEventData(
    const std::string& sid, const std::string& nsp, const std::string& ackId, const std::string& eventName,
    const std::string& eventData, const std::function<void(const std::string&)>& sendPacket)
{
  if (nsp.size() > config.maxNamespaceLength) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(
        "/", constants::kCodeNamespaceTooLong, constants::kMessageNamespaceTooLong));
    return;
  }
  if (eventName.empty()) {
    sendPacket(
        protocol::makeSocketIoErrorEventPacket(nsp, constants::kCodeEmptyEventName, constants::kMessageEmptyEventName));
    return;
  }
  if (eventName.size() > config.maxEventNameLength) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(
        nsp, constants::kCodeEventNameTooLong, constants::kMessageEventNameTooLong));
    return;
  }

  const ConnectDecision guardDecision = evaluateEventGuard(sid, nsp, eventName, eventData);
  if (!guardDecision.allowed) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(nsp, guardDecision.code, guardDecision.message));
    return;
  }

  EventHandler handlerCopy;
  {
    std::lock_guard<std::mutex> lock(eventHandlerMutex);
    handlerCopy = eventHandler;
  }
  if (!handlerCopy) {
    return;
  }

  const AckCallback ack = [ackId, nsp, sendPacket](const std::string& ackJsonArrayPayload) {
    if (ackId.empty()) {
      return;
    }
    sendPacket(protocol::makeSocketIoAckPacket(ackId, ackJsonArrayPayload, nsp));
  };

  handlerCopy(*this, sid, nsp, eventName, eventData, ack);
}

void Server::registerWebSocketClient(int clientFd, const std::string& sid)
{
  std::lock_guard<std::mutex> lock(webSocketClientsMutex);
  webSocketClients[clientFd] = sid;
}

void Server::unregisterWebSocketClient(int clientFd)
{
  std::lock_guard<std::mutex> lock(webSocketClientsMutex);
  webSocketClients.erase(clientFd);
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
