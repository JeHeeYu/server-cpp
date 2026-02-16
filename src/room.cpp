#include "room.h"

#include "constants.h"
#include "protocol/types.h"

#include <vector>
#include <unordered_set>

namespace socketIoServer {

std::string Server::makeRoomKey(const std::string& nsp, const std::string& room) const
{
  return protocol::normalizeNamespace(nsp) + "|" + room;
}

void Server::joinRoom(const std::string& sid, const std::string& nsp, const std::string& room)
{
  if (room.empty()) {
    return;
  }
  if (room.size() > config.maxRoomNameLength) {
    enqueuePacket(
        sid, protocol::makeSocketIoErrorEventPacket(
                 protocol::normalizeNamespace(nsp), constants::kCodeRoomNameTooLong, constants::kMessageRoomNameTooLong));
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
  if (room.size() > config.maxRoomNameLength) {
    enqueuePacket(
        sid, protocol::makeSocketIoErrorEventPacket(
                 protocol::normalizeNamespace(nsp), constants::kCodeRoomNameTooLong, constants::kMessageRoomNameTooLong));
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
    enqueuePacketWithPolicy(sid, packet, false);
  }
}

void Server::broadcastToRoom(
    const std::string& nsp, const std::string& room, const std::string& packet,
    const std::vector<std::string>& excludedSids)
{
  if (nsp.empty() || room.empty() || packet.empty()) {
    return;
  }

  const std::unordered_set<std::string> excluded(excludedSids.begin(), excludedSids.end());
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
    if (excluded.find(sid) != excluded.end()) {
      continue;
    }
    enqueuePacketWithPolicy(sid, packet, false);
  }
}

void Server::broadcastToRoom(
    const std::string& nsp, const std::string& room, const std::string& packet, bool isVolatile,
    const std::string& excludeSid)
{
  if (isVolatile) {
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
      enqueuePacketWithPolicy(sid, packet, true);
    }
    return;
  }

  broadcastToRoom(nsp, room, packet, excludeSid);
}

void Server::broadcastToRoom(
    const std::string& nsp, const std::string& room, const std::string& packet, bool isVolatile,
    const std::vector<std::string>& excludedSids)
{
  if (isVolatile) {
    if (nsp.empty() || room.empty() || packet.empty()) {
      return;
    }

    const std::unordered_set<std::string> excluded(excludedSids.begin(), excludedSids.end());
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
      if (excluded.find(sid) != excluded.end()) {
        continue;
      }
      enqueuePacketWithPolicy(sid, packet, true);
    }
    return;
  }

  broadcastToRoom(nsp, room, packet, excludedSids);
}

void Server::emitToRoomEvent(
    const std::string& nsp, const std::string& room, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::string& excludeSid)
{
  broadcastToRoom(nsp, room, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp), excludeSid);
}

void Server::emitToRoomEvent(
    const std::string& nsp, const std::string& room, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids)
{
  broadcastToRoom(
      nsp, room, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp), excludedSids);
}

void Server::emitToRoomEventVolatile(
    const std::string& nsp, const std::string& room, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::string& excludeSid)
{
  broadcastToRoom(
      nsp, room, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp), true, excludeSid);
}

void Server::emitToRoomEventVolatile(
    const std::string& nsp, const std::string& room, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids)
{
  broadcastToRoom(
      nsp, room, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp), true, excludedSids);
}

void Server::emitToRoomsEvent(
    const std::string& nsp, const std::vector<std::string>& rooms, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids)
{
  if (rooms.empty()) {
    return;
  }

  const std::string packet = protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp);
  for (const std::string& room : rooms) {
    if (room.empty()) {
      continue;
    }
    broadcastToRoom(nsp, room, packet, false, excludedSids);
  }
}

void Server::emitToRoomsEventVolatile(
    const std::string& nsp, const std::vector<std::string>& rooms, const std::string& eventName,
    const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids)
{
  if (rooms.empty()) {
    return;
  }

  const std::string packet = protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, nsp);
  for (const std::string& room : rooms) {
    if (room.empty()) {
      continue;
    }
    broadcastToRoom(nsp, room, packet, true, excludedSids);
  }
}

}  // namespace socketIoServer
