#include "server.h"
#include "protocol/types.h"
#include "utils/websocket_util.h"

#include <sys/socket.h>

#include <chrono>

namespace socketIoServer {

bool Server::handleWebSocketHandshake(
    int clientFd, const std::string& request, const std::unordered_map<std::string, std::string>& query)
{
  const auto eioIt = query.find("EIO");
  const auto transportIt = query.find("transport");
  if (eioIt == query.end() || transportIt == query.end()) {
    return false;
  }
  if (!protocol::isEngineIoVersion4(eioIt->second) ||
      !protocol::isWebSocketTransport(transportIt->second)) {
    return false;
  }

  const std::string wsKey = utils::getHttpHeader(request, "Sec-WebSocket-Key");
  if (wsKey.empty()) {
    return false;
  }

  const std::string acceptValue = utils::computeWebSocketAccept(wsKey);
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Accept: " +
      acceptValue + "\r\n"
                    "\r\n";
  if (::send(clientFd, response.c_str(), response.size(), 0) <= 0) {
    return false;
  }

  auto sidIt = query.find("sid");
  std::string sid;
  bool directWebSocketConnection = false;
  if (sidIt == query.end()) {
    sid = createSession();
    SessionState session;
    session.sid = sid;
    session.lastSeenAt = std::chrono::steady_clock::now();
    session.connectedNamespaces.insert("/");
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      sessions.emplace(sid, std::move(session));
    }
    directWebSocketConnection = true;
  } else {
    sid = sidIt->second;
    if (!hasSession(sid)) {
      return false;
    }
    touchSession(sid);
  }

  if (directWebSocketConnection) {
    const std::string openPacket = protocol::makeEngineIoOpenPacket(sid);
    if (!utils::sendWebSocketTextFrame(clientFd, openPacket)) {
      return false;
    }
  }

  serveWebSocket(clientFd, sid);
  return true;
}

void Server::serveWebSocket(int clientFd, const std::string& sid)
{
  while (running.load()) {
    std::string packet;
    if (!utils::readWebSocketTextFrame(clientFd, packet)) {
      break;
    }

    if (packet.empty()) {
      continue;
    }

    touchSession(sid);

    if (protocol::isEngineIoProbePingPacket(packet)) {
      if (!utils::sendWebSocketTextFrame(clientFd, protocol::makeEngineIoProbePongPacket())) {
        break;
      }
      continue;
    }

    if (protocol::isEngineIoUpgradePacket(packet)) {
      continue;
    }

    const protocol::EngineIoControlPacket controlType = protocol::parseEngineIoControlPacket(packet);
    if (controlType == protocol::EngineIoControlPacket::close) {
      removeSession(sid);
      break;
    }
    if (controlType == protocol::EngineIoControlPacket::ping) {
      if (!utils::sendWebSocketTextFrame(clientFd, protocol::toWirePacket(protocol::EngineIoControlPacket::pong))) {
        break;
      }
      continue;
    }

    const protocol::SocketIoPacketType packetType = protocol::parseSocketIoPacketType(packet);
    if (packetType == protocol::SocketIoPacketType::connect) {
      const std::string nsp = protocol::parseSocketIoNamespace(packet);
      connectNamespace(sid, nsp);
      if (!utils::sendWebSocketTextFrame(clientFd, protocol::makeSocketIoConnectPacket(sid, nsp))) {
        break;
      }
      continue;
    }

    if (packetType == protocol::SocketIoPacketType::event ||
        packetType == protocol::SocketIoPacketType::binaryEvent) {
      bool sendFailed = false;
      dispatchSocketIoEvent(sid, packet, [clientFd, &sendFailed](const std::string& packetToSend) {
        if (!utils::sendWebSocketTextFrame(clientFd, packetToSend)) {
          sendFailed = true;
        }
      });
      if (sendFailed) {
        break;
      }
      continue;
    }

    if (packetType == protocol::SocketIoPacketType::disconnect) {
      disconnectNamespace(sid, protocol::parseSocketIoNamespace(packet));
      continue;
    }
  }

  removeSession(sid);
}

}  // namespace socketIoServer
