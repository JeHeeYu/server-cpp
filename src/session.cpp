#include "session.h"

#include "protocol/types.h"
#include "utils/websocket_util.h"

#include <chrono>
#include <vector>

namespace socketIoServer {

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
    it->second.online = true;
    it->second.lastSeenAt = std::chrono::steady_clock::now();
    it->second.disconnectedAt = std::chrono::steady_clock::time_point::min();
  }
}

void Server::markSessionDisconnected(const std::string& sid)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return;
  }
  it->second.online = false;
  it->second.disconnectedAt = std::chrono::steady_clock::now();
  resetPendingBinaryState(it->second);
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

  std::lock_guard<std::mutex> ackLock(pendingAcksMutex);
  for (auto it = pendingAcks.begin(); it != pendingAcks.end();) {
    if (it->second.sid == sid) {
      it = pendingAcks.erase(it);
      continue;
    }
    ++it;
  }
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

bool Server::isNamespaceConnected(const std::string& sid, const std::string& nsp)
{
  const std::string normalized = protocol::normalizeNamespace(nsp);
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return false;
  }
  return it->second.connectedNamespaces.find(normalized) != it->second.connectedNamespaces.end();
}

void Server::resetPendingBinaryState(SessionState& session)
{
  session.pendingBinaryNsp.clear();
  session.pendingBinaryAckId.clear();
  session.pendingBinaryEventPayload.clear();
  session.pendingBinaryIsAck = false;
  session.pendingBinaryExpectedAttachmentCount = 0;
  session.pendingBinaryTotalBytes = 0;
  session.pendingBinaryAttachments.clear();
}

bool Server::consumeInboundPacketBudget(const std::string& sid)
{
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return false;
  }

  SessionState& session = it->second;
  const auto now = std::chrono::steady_clock::now();
  if (session.inboundWindowStart == std::chrono::steady_clock::time_point::min() ||
      now - session.inboundWindowStart >= std::chrono::seconds(1)) {
    session.inboundWindowStart = now;
    session.inboundPacketCountInWindow = 0;
  }

  if (config.maxInboundPacketsPerSecond > 0 &&
      session.inboundPacketCountInWindow >= config.maxInboundPacketsPerSecond) {
    return false;
  }

  ++session.inboundPacketCountInWindow;
  session.lastSeenAt = now;
  session.online = true;
  session.disconnectedAt = std::chrono::steady_clock::time_point::min();
  return true;
}

void Server::enqueuePacket(const std::string& sid, const std::string& packet)
{
  int targetWebSocketFd = -1;
  {
    std::lock_guard<std::mutex> lock(sessionsMutex);
    const auto it = sessions.find(sid);
    if (it == sessions.end()) {
      return;
    }

    if (!it->second.online) {
      enqueuePacketUnlocked(it->second, packet);
      return;
    }

    {
      std::lock_guard<std::mutex> wsLock(webSocketClientsMutex);
      for (const auto& entry : webSocketClients) {
        if (entry.second == sid) {
          targetWebSocketFd = entry.first;
          break;
        }
      }
    }

    if (targetWebSocketFd < 0) {
      enqueuePacketUnlocked(it->second, packet);
      return;
    }
  }

  if (utils::sendWebSocketTextFrame(targetWebSocketFd, packet)) {
    return;
  }

  markSessionDisconnected(sid);
  std::lock_guard<std::mutex> lock(sessionsMutex);
  const auto it = sessions.find(sid);
  if (it == sessions.end()) {
    return;
  }
  enqueuePacketUnlocked(it->second, packet);
}

void Server::enqueuePacketWithPolicy(const std::string& sid, const std::string& packet, bool isVolatile)
{
  if (!isVolatile) {
    enqueuePacket(sid, packet);
    return;
  }

  int targetWebSocketFd = -1;
  {
    std::lock_guard<std::mutex> lock(sessionsMutex);
    const auto it = sessions.find(sid);
    if (it == sessions.end() || !it->second.online) {
      return;
    }

    std::lock_guard<std::mutex> wsLock(webSocketClientsMutex);
    for (const auto& entry : webSocketClients) {
      if (entry.second == sid) {
        targetWebSocketFd = entry.first;
        break;
      }
    }
  }

  if (targetWebSocketFd < 0) {
    return;
  }

  if (!utils::sendWebSocketTextFrame(targetWebSocketFd, packet)) {
    markSessionDisconnected(sid);
  }
}

void Server::enqueuePacketUnlocked(SessionState& session, const std::string& packet)
{
  session.lastSeenAt = std::chrono::steady_clock::now();
  session.online = true;
  session.disconnectedAt = std::chrono::steady_clock::time_point::min();
  if (config.maxOutgoingPacketsPerSession > 0 &&
      session.outgoingPackets.size() >= config.maxOutgoingPacketsPerSession) {
    if (config.outgoingOverflowPolicy == ServerConfig::OutgoingOverflowPolicy::dropNewest) {
      return;
    }
    if (!session.outgoingPackets.empty()) {
      session.outgoingPackets.pop_front();
    }
  }
  session.outgoingPackets.push_back(packet);
}

}  // namespace socketIoServer
