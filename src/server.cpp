#include "server.h"
#include "protocol/types.h"
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
    std::vector<std::string> expiredSids;

    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      for (const auto& entry : sessions) {
        const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(now - entry.second.lastSeenAt);
        if (idle > ttl) {
          expiredSids.push_back(entry.first);
        }
      }
    }

    for (const std::string& sid : expiredSids) {
      removeSession(sid);
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
  const auto roomsIt = sessionRooms.find(sid);
  if (roomsIt != sessionRooms.end()) {
    for (const std::string& room : roomsIt->second) {
      const auto membersIt = roomMembers.find(room);
      if (membersIt != roomMembers.end()) {
        membersIt->second.erase(sid);
        if (membersIt->second.empty()) {
          roomMembers.erase(membersIt);
        }
      }
    }
    sessionRooms.erase(roomsIt);
  }
  sessions.erase(sid);
}

void Server::connectNamespace(const std::string& sid, const std::string& nsp)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return;
  }
  it->second.connectedNamespaces.insert(protocol::normalizeNamespace(nsp));
  it->second.lastSeenAt = std::chrono::steady_clock::now();
}

void Server::disconnectNamespace(const std::string& sid, const std::string& nsp)
{
  const std::string normalizedNsp = protocol::normalizeNamespace(nsp);
  std::lock_guard<std::mutex> lock(sessionsMutex);

  const auto sessionIt = sessions.find(sid);
  if (sessionIt == sessions.end()) {
    return;
  }
  sessionIt->second.connectedNamespaces.erase(normalizedNsp);

  const auto roomsIt = sessionRooms.find(sid);
  if (roomsIt != sessionRooms.end()) {
    std::vector<std::string> toErase;
    for (const std::string& roomKey : roomsIt->second) {
      if (roomKey.rfind(normalizedNsp + "|", 0) == 0) {
        toErase.push_back(roomKey);
      }
    }
    for (const std::string& roomKey : toErase) {
      const auto membersIt = roomMembers.find(roomKey);
      if (membersIt != roomMembers.end()) {
        membersIt->second.erase(sid);
        if (membersIt->second.empty()) {
          roomMembers.erase(membersIt);
        }
      }
      roomsIt->second.erase(roomKey);
    }
    if (roomsIt->second.empty()) {
      sessionRooms.erase(roomsIt);
    }
  }
}

std::chrono::milliseconds Server::sessionTtl() const
{
  return std::chrono::milliseconds(
      static_cast<std::uint64_t>(config.pingIntervalMs) + static_cast<std::uint64_t>(config.pingTimeoutMs));
}

std::string Server::makeRoomKey(const std::string& nsp, const std::string& room) const
{
  return protocol::normalizeNamespace(nsp) + "|" + room;
}

void Server::joinRoom(const std::string& sid, const std::string& nsp, const std::string& room)
{
  if (room.empty()) {
    return;
  }

  const std::string roomKey = makeRoomKey(nsp, room);
  const std::string normalizedNsp = protocol::normalizeNamespace(nsp);
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto sessionIt = sessions.find(sid);
  if (sessionIt == sessions.end()) {
    return;
  }
  if (sessionIt->second.connectedNamespaces.find(normalizedNsp) ==
      sessionIt->second.connectedNamespaces.end()) {
    return;
  }
  roomMembers[roomKey].insert(sid);
  sessionRooms[sid].insert(roomKey);
}

void Server::leaveRoom(const std::string& sid, const std::string& nsp, const std::string& room)
{
  if (room.empty()) {
    return;
  }
  const std::string roomKey = makeRoomKey(nsp, room);
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto membersIt = roomMembers.find(roomKey);
  if (membersIt != roomMembers.end()) {
    membersIt->second.erase(sid);
    if (membersIt->second.empty()) {
      roomMembers.erase(membersIt);
    }
  }
  const auto sessionRoomsIt = sessionRooms.find(sid);
  if (sessionRoomsIt != sessionRooms.end()) {
    sessionRoomsIt->second.erase(roomKey);
    if (sessionRoomsIt->second.empty()) {
      sessionRooms.erase(sessionRoomsIt);
    }
  }
}

void Server::broadcastToRoom(
    const std::string& nsp, const std::string& room, const std::string& packet, const std::string& excludeSid)
{
  if (nsp.empty() || room.empty() || packet.empty()) {
    return;
  }

  const std::string roomKey = makeRoomKey(nsp, room);
  std::vector<std::string> targets;
  {
    std::lock_guard<std::mutex> lock(sessionsMutex);
    const auto membersIt = roomMembers.find(roomKey);
    if (membersIt == roomMembers.end()) {
      return;
    }
    targets.assign(membersIt->second.begin(), membersIt->second.end());
  }

  for (const std::string& sid : targets) {
    if (!excludeSid.empty() && sid == excludeSid) {
      continue;
    }
    enqueuePacket(sid, packet);
  }
}

void Server::emitToRoomEvent(
    const std::string& nsp, const std::string& room, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::string& excludeSid)
{
  broadcastToRoom(nsp, room, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp), excludeSid);
}

void Server::dispatchSocketIoEvent(
    const std::string& sid, const std::string& packet, const std::function<void(const std::string&)>& sendPacket)
{
  std::string ackId;
  protocol::SocketIoEventPacket eventPacket;
  if (!protocol::parseSocketIoEventPacket(packet, eventPacket)) {
    return;
  }

  const std::string eventName = protocol::parseSocketIoEventName(eventPacket.eventPayload);
  const std::string eventData = protocol::parseSocketIoEventData(eventPacket.eventPayload);
  if (eventName.empty()) {
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

  const AckCallback ack = [eventPacket, sendPacket](const std::string& ackJsonArrayPayload) {
    if (eventPacket.ackId.empty()) {
      return;
    }
    sendPacket(protocol::makeSocketIoAckPacket(eventPacket.ackId, ackJsonArrayPayload, eventPacket.nsp));
  };

  handlerCopy(*this, sid, eventPacket.nsp, eventName, eventData, ack);
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
