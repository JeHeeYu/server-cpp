#include "server.h"
#include "constants.h"
#include "protocol/types.h"
#include "utils/json_util.h"
#include "utils/websocket_util.h"

#include <sys/socket.h>

#include <chrono>
#include <deque>
#include <utility>
#include <vector>

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
  std::deque<std::string> pendingPackets;
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
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto sessionIt = sessions.find(sid);
      if (sessionIt == sessions.end()) {
        return false;
      }
      sessionIt->second.online = true;
      sessionIt->second.lastSeenAt = std::chrono::steady_clock::now();
      sessionIt->second.disconnectedAt = std::chrono::steady_clock::time_point::min();
      pendingPackets.swap(sessionIt->second.outgoingPackets);
    }
  }

  if (directWebSocketConnection) {
    const std::string openPacket = protocol::makeEngineIoOpenPacket(
        sid, config.pingIntervalMs, config.pingTimeoutMs, static_cast<std::uint32_t>(config.maxIncomingPacketBytes));
    if (!utils::sendWebSocketTextFrame(clientFd, openPacket)) {
      return false;
    }
  }
  for (const std::string& pendingPacket : pendingPackets) {
    if (!utils::sendWebSocketTextFrame(clientFd, pendingPacket)) {
      return false;
    }
  }

  registerWebSocketClient(clientFd, sid);
  serveWebSocket(clientFd, sid);
  unregisterWebSocketClient(clientFd);
  return true;
}

void Server::serveWebSocket(int clientFd, const std::string& sid)
{
  protocol::SocketIoEventPacket pendingBinaryEvent;
  std::vector<std::string> pendingBinaryAttachments;
  std::size_t pendingBinaryTotalBytes = 0;
  bool hasPendingBinaryEvent = false;
  bool pendingBinaryIsAck = false;

  while (running.load()) {
    std::string packet;
    utils::WebSocketOpcode opcode = utils::WebSocketOpcode::invalid;
    if (!utils::readWebSocketFrame(clientFd, packet, opcode)) {
      break;
    }

    if (opcode == utils::WebSocketOpcode::close) {
      (void)utils::sendWebSocketControlFrame(clientFd, utils::WebSocketOpcode::close, packet);
      break;
    }
    if (opcode == utils::WebSocketOpcode::ping) {
      if (!utils::sendWebSocketControlFrame(clientFd, utils::WebSocketOpcode::pong, packet)) {
        break;
      }
      continue;
    }
    if (opcode == utils::WebSocketOpcode::binary) {
      if (!consumeInboundPacketBudget(sid)) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    "/", constants::kCodeRateLimitExceeded, constants::kMessageRateLimitExceeded))) {
          break;
        }
        continue;
      }
      if (!hasPendingBinaryEvent) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    "/", constants::kCodeBinaryAttachmentOutOfOrder, constants::kMessageBinaryAttachmentOutOfOrder))) {
          break;
        }
        continue;
      }

      if (packet.size() > config.maxBinaryAttachmentBytes) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }
      pendingBinaryAttachments.push_back(utils::encodeBase64(packet));
      pendingBinaryTotalBytes += packet.size();
      if (pendingBinaryAttachments.size() > config.maxBinaryAttachmentsPerEvent) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodeTooManyBinaryAttachments,
                    constants::kMessageTooManyBinaryAttachments))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }
      if (pendingBinaryTotalBytes > config.maxBinaryTotalBytesPerEvent) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }
      if (pendingBinaryAttachments.size() > static_cast<std::size_t>(pendingBinaryEvent.attachmentCount)) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodeBinaryAttachmentCountMismatch,
                    constants::kMessageBinaryAttachmentCountMismatch))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }

      if (pendingBinaryAttachments.size() == static_cast<std::size_t>(pendingBinaryEvent.attachmentCount)) {
        bool sendFailed = false;
        if (!pendingBinaryIsAck) {
          const std::string eventName = protocol::parseSocketIoEventName(pendingBinaryEvent.eventPayload);
          const std::string eventData = protocol::parseSocketIoEventData(pendingBinaryEvent.eventPayload);
          const std::string mergedEventData =
              protocol::mergeSocketIoBinaryEventData(eventData, pendingBinaryAttachments);

          dispatchSocketIoEventData(
              sid, pendingBinaryEvent.nsp, pendingBinaryEvent.ackId, eventName, mergedEventData,
              [clientFd, &sendFailed](const std::string& packetToSend) {
                if (!utils::sendWebSocketTextFrame(clientFd, packetToSend)) {
                  sendFailed = true;
                }
              });
        }
        hasPendingBinaryEvent = false;
        pendingBinaryIsAck = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        if (sendFailed) {
          break;
        }
      }
      continue;
    }

    if (opcode == utils::WebSocketOpcode::pong || opcode != utils::WebSocketOpcode::text) {
      continue;
    }
    if (!consumeInboundPacketBudget(sid)) {
      if (!utils::sendWebSocketTextFrame(
              clientFd,
              protocol::makeSocketIoErrorEventPacket(
                  "/", constants::kCodeRateLimitExceeded, constants::kMessageRateLimitExceeded))) {
        break;
      }
      continue;
    }

    if (packet.empty()) {
      continue;
    }
    if (packet.size() > config.maxIncomingPacketBytes) {
      if (!utils::sendWebSocketTextFrame(
              clientFd,
              protocol::makeSocketIoErrorEventPacket(
                  "/", constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge))) {
        break;
      }
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
      markSessionDisconnected(sid);
      break;
    }
    if (controlType == protocol::EngineIoControlPacket::ping) {
      if (!utils::sendWebSocketTextFrame(clientFd, protocol::toWirePacket(protocol::EngineIoControlPacket::pong))) {
        break;
      }
      continue;
    }

    const protocol::SocketIoPacketType packetType = protocol::parseSocketIoPacketType(packet);
    if (hasPendingBinaryEvent) {
      const bool expectedBinaryPacketArrived =
          pendingBinaryIsAck ? (packetType == protocol::SocketIoPacketType::binaryAck)
                             : (packetType == protocol::SocketIoPacketType::binaryEvent);
      if (!expectedBinaryPacketArrived) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodeBinaryAttachmentCountMismatch,
                    constants::kMessageBinaryAttachmentCountMismatch))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryIsAck = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }
    }

    if (packetType == protocol::SocketIoPacketType::connect) {
      protocol::SocketIoConnectPacket connectPacket;
      if (!protocol::parseSocketIoConnectPacket(packet, connectPacket)) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoConnectErrorPacket(
                    "/", constants::kCodeMalformedConnectPacket, constants::kMessageMalformedConnectPacket))) {
          break;
        }
        continue;
      }
      if (connectPacket.nsp.size() > config.maxNamespaceLength) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoConnectErrorPacket(
                    "/", constants::kCodeNamespaceTooLong, constants::kMessageNamespaceTooLong))) {
          break;
        }
        continue;
      }

      const ConnectDecision decision = evaluateNamespaceConnect(sid, connectPacket.nsp, connectPacket.authJson);
      if (!decision.allowed) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoConnectErrorPacket(connectPacket.nsp, decision.code, decision.message))) {
          break;
        }
        continue;
      }

      connectNamespace(sid, connectPacket.nsp);
      if (!utils::sendWebSocketTextFrame(
              clientFd, protocol::makeSocketIoConnectPacket(sid, connectPacket.nsp))) {
        break;
      }
      continue;
    }

    if (packetType == protocol::SocketIoPacketType::event ||
        packetType == protocol::SocketIoPacketType::binaryEvent) {
      if (hasPendingBinaryEvent && packetType == protocol::SocketIoPacketType::binaryEvent) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    pendingBinaryEvent.nsp, constants::kCodeBinaryAttachmentCountMismatch,
                    constants::kMessageBinaryAttachmentCountMismatch))) {
          break;
        }
        hasPendingBinaryEvent = false;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
      }

      if (packetType == protocol::SocketIoPacketType::binaryEvent) {
        protocol::SocketIoEventPacket parsedBinaryEvent;
        if (!protocol::parseSocketIoEventPacket(packet, parsedBinaryEvent)) {
          if (!utils::sendWebSocketTextFrame(
                  clientFd,
                  protocol::makeSocketIoErrorEventPacket(
                      "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket))) {
            break;
          }
          continue;
        }
        if (parsedBinaryEvent.attachmentCount <= 0) {
          if (!utils::sendWebSocketTextFrame(
                  clientFd,
                  protocol::makeSocketIoErrorEventPacket(
                      parsedBinaryEvent.nsp, constants::kCodeMalformedEventPacket,
                      constants::kMessageMalformedEventPacket))) {
            break;
          }
          continue;
        }
        if (static_cast<std::size_t>(parsedBinaryEvent.attachmentCount) > config.maxBinaryAttachmentsPerEvent) {
          if (!utils::sendWebSocketTextFrame(
                  clientFd,
                  protocol::makeSocketIoErrorEventPacket(
                      parsedBinaryEvent.nsp, constants::kCodeTooManyBinaryAttachments,
                      constants::kMessageTooManyBinaryAttachments))) {
            break;
          }
          continue;
        }

        hasPendingBinaryEvent = true;
        pendingBinaryIsAck = false;
        pendingBinaryEvent = std::move(parsedBinaryEvent);
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
        continue;
      }

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

    if (packetType == protocol::SocketIoPacketType::ack ||
        packetType == protocol::SocketIoPacketType::binaryAck) {
      protocol::SocketIoAckPacket ackPacket;
      if (!protocol::parseSocketIoAckPacket(packet, ackPacket)) {
        if (!utils::sendWebSocketTextFrame(
                clientFd,
                protocol::makeSocketIoErrorEventPacket(
                    "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket))) {
          break;
        }
      }
      if (ackPacket.isBinary) {
        if (static_cast<std::size_t>(ackPacket.attachmentCount) > config.maxBinaryAttachmentsPerEvent) {
          if (!utils::sendWebSocketTextFrame(
                  clientFd,
                  protocol::makeSocketIoErrorEventPacket(
                      ackPacket.nsp, constants::kCodeTooManyBinaryAttachments,
                      constants::kMessageTooManyBinaryAttachments))) {
            break;
          }
          continue;
        }
        hasPendingBinaryEvent = true;
        pendingBinaryIsAck = true;
        pendingBinaryEvent.nsp = ackPacket.nsp;
        pendingBinaryEvent.ackId = ackPacket.ackId;
        pendingBinaryEvent.eventPayload = ackPacket.ackPayload;
        pendingBinaryEvent.attachmentCount = ackPacket.attachmentCount;
        pendingBinaryAttachments.clear();
        pendingBinaryTotalBytes = 0;
      }
      continue;
    }

    if (packetType == protocol::SocketIoPacketType::unknown) {
      if (!utils::sendWebSocketTextFrame(
              clientFd,
              protocol::makeSocketIoErrorEventPacket(
                  "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket))) {
        break;
      }
      continue;
    }
  }

  markSessionDisconnected(sid);
}

}  // namespace socketIoServer
