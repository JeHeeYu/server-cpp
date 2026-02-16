#include "server.h"
#include "constants.h"
#include "protocol/types.h"
#include "utils/engine_io_util.h"
#include "utils/http_util.h"
#include "utils/json_util.h"
#include "utils/url_util.h"

#include <chrono>
#include <cstdlib>
#include <vector>

namespace socketIoServer {

namespace {

constexpr const char* kSocketIoPath = "/socket.io/";

bool parseOffsetValue(const std::string& raw, std::uint64_t& valueOut)
{
  if (raw.empty()) {
    return false;
  }

  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(raw.c_str(), &end, 10);
  if (end == nullptr || *end != '\0') {
    return false;
  }
  valueOut = static_cast<std::uint64_t>(parsed);
  return true;
}

}

void Server::processEngineIoPacket(const std::string& sid, const std::string& packet)
{
  if (packet.empty()) {
    return;
  }
  if (!consumeInboundPacketBudget(sid)) {
    enqueuePacket(
        sid, protocol::makeSocketIoErrorEventPacket(
                 "/", constants::kCodeRateLimitExceeded, constants::kMessageRateLimitExceeded));
    return;
  }
  if (packet.size() > config.maxIncomingPacketBytes) {
    enqueuePacket(sid, protocol::makeSocketIoErrorEventPacket(
                           "/", constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge));
    return;
  }

  touchSession(sid);

  if (packet.rfind(protocol::kEngineIoPacketBinaryPrefix, 0) == 0) {
    protocol::SocketIoEventPacket pendingEvent;
    protocol::SocketIoAckPacket pendingAck;
    std::vector<std::string> attachments;
    bool readyToDispatch = false;
    bool readyToFinalizeAck = false;
    std::size_t decodedBinarySize = 0;
    const std::string encodedBinary = packet.substr(1);
    if (!utils::getDecodedBase64Size(encodedBinary, decodedBinarySize)) {
      enqueuePacket(
          sid, protocol::makeSocketIoErrorEventPacket(
                   "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket));
      return;
    }

    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto sessionIt = sessions.find(sid);
      if (sessionIt == sessions.end()) {
        return;
      }

      SessionState& session = sessionIt->second;
      if (session.pendingBinaryExpectedAttachmentCount == 0) {
        enqueuePacketUnlocked(
            session, protocol::makeSocketIoErrorEventPacket(
                         "/", constants::kCodeBinaryAttachmentOutOfOrder,
                         constants::kMessageBinaryAttachmentOutOfOrder));
        return;
      }

      if (decodedBinarySize > config.maxBinaryAttachmentBytes) {
        enqueuePacketUnlocked(
            session, protocol::makeSocketIoErrorEventPacket(
                         session.pendingBinaryNsp, constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge));
        resetPendingBinaryState(session);
        return;
      }
      session.pendingBinaryAttachments.push_back(encodedBinary);
      session.pendingBinaryTotalBytes += decodedBinarySize;
      if (session.pendingBinaryTotalBytes > config.maxBinaryTotalBytesPerEvent) {
        enqueuePacketUnlocked(
            session, protocol::makeSocketIoErrorEventPacket(
                         session.pendingBinaryNsp, constants::kCodePayloadTooLarge, constants::kMessagePayloadTooLarge));
        resetPendingBinaryState(session);
        return;
      }
      if (session.pendingBinaryAttachments.size() > config.maxBinaryAttachmentsPerEvent) {
        enqueuePacketUnlocked(
            session, protocol::makeSocketIoErrorEventPacket(
                         session.pendingBinaryNsp, constants::kCodeTooManyBinaryAttachments,
                         constants::kMessageTooManyBinaryAttachments));
        resetPendingBinaryState(session);
        return;
      }
      if (session.pendingBinaryAttachments.size() > session.pendingBinaryExpectedAttachmentCount) {
        enqueuePacketUnlocked(
            session, protocol::makeSocketIoErrorEventPacket(
                         session.pendingBinaryNsp, constants::kCodeBinaryAttachmentCountMismatch,
                         constants::kMessageBinaryAttachmentCountMismatch));
        resetPendingBinaryState(session);
        return;
      }

      if (session.pendingBinaryAttachments.size() == session.pendingBinaryExpectedAttachmentCount) {
        if (session.pendingBinaryIsAck) {
          pendingAck.nsp = session.pendingBinaryNsp;
          pendingAck.ackId = session.pendingBinaryAckId;
          pendingAck.ackPayload = session.pendingBinaryEventPayload;
          readyToFinalizeAck = true;
        } else {
          pendingEvent.nsp = session.pendingBinaryNsp;
          pendingEvent.ackId = session.pendingBinaryAckId;
          pendingEvent.eventPayload = session.pendingBinaryEventPayload;
          readyToDispatch = true;
        }
        attachments = session.pendingBinaryAttachments;
        resetPendingBinaryState(session);
      }
    }

    if (readyToFinalizeAck) {
      const std::string mergedAckPayload =
          protocol::mergeSocketIoBinaryEventData(pendingAck.ackPayload, attachments);
      dispatchSocketIoAckData(sid, pendingAck.nsp, pendingAck.ackId, mergedAckPayload);
      return;
    }

    if (!readyToDispatch) {
      return;
    }

    const std::string eventName = protocol::parseSocketIoEventName(pendingEvent.eventPayload);
    const std::string eventData = protocol::parseSocketIoEventData(pendingEvent.eventPayload);
    const std::string mergedEventData = protocol::mergeSocketIoBinaryEventData(eventData, attachments);
    dispatchSocketIoEventData(
        sid, pendingEvent.nsp, pendingEvent.ackId, eventName, mergedEventData,
        [this, sid](const std::string& packetToSend) {
          enqueuePacket(sid, packetToSend);
        });
    return;
  }

  const protocol::SocketIoPacketType packetType = protocol::parseSocketIoPacketType(packet);
  const protocol::EngineIoControlPacket controlType = protocol::parseEngineIoControlPacket(packet);
  if (controlType == protocol::EngineIoControlPacket::close) {
    removeSession(sid);
    return;
  }
  if (controlType == protocol::EngineIoControlPacket::ping) {
    enqueuePacket(sid, protocol::toWirePacket(protocol::EngineIoControlPacket::pong));
    return;
  }

  {
    std::lock_guard<std::mutex> lock(sessionsMutex);
    const auto sessionIt = sessions.find(sid);
    if (sessionIt != sessions.end() && sessionIt->second.pendingBinaryExpectedAttachmentCount > 0) {
      enqueuePacketUnlocked(
          sessionIt->second, protocol::makeSocketIoErrorEventPacket(
                                 sessionIt->second.pendingBinaryNsp, constants::kCodeBinaryAttachmentCountMismatch,
                                 constants::kMessageBinaryAttachmentCountMismatch));
      resetPendingBinaryState(sessionIt->second);
      return;
    }
  }

  if (packetType == protocol::SocketIoPacketType::connect) {
    protocol::SocketIoConnectPacket connectPacket;
    if (!protocol::parseSocketIoConnectPacket(packet, connectPacket)) {
      enqueuePacket(
          sid, protocol::makeSocketIoConnectErrorPacket(
                   "/", constants::kCodeMalformedConnectPacket, constants::kMessageMalformedConnectPacket));
      return;
    }
    if (connectPacket.nsp.size() > config.maxNamespaceLength) {
      enqueuePacket(
          sid, protocol::makeSocketIoConnectErrorPacket(
                   "/", constants::kCodeNamespaceTooLong, constants::kMessageNamespaceTooLong));
      return;
    }

    const ConnectDecision decision = evaluateNamespaceConnect(sid, connectPacket.nsp, connectPacket.authJson);
    if (!decision.allowed) {
      enqueuePacket(
          sid, protocol::makeSocketIoConnectErrorPacket(connectPacket.nsp, decision.code, decision.message));
      return;
    }

    connectNamespace(sid, connectPacket.nsp);
    enqueuePacket(sid, protocol::makeSocketIoConnectPacket(sid, connectPacket.nsp));
    return;
  }

  if (packetType == protocol::SocketIoPacketType::disconnect) {
    disconnectNamespace(sid, protocol::parseSocketIoNamespace(packet));
    return;
  }

  if (packetType == protocol::SocketIoPacketType::ack || packetType == protocol::SocketIoPacketType::binaryAck) {
    protocol::SocketIoAckPacket ackPacket;
    if (!protocol::parseSocketIoAckPacket(packet, ackPacket)) {
      enqueuePacket(sid, protocol::makeSocketIoErrorEventPacket(
                             "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket));
      return;
    }

    if (ackPacket.isBinary) {
      if (static_cast<std::size_t>(ackPacket.attachmentCount) > config.maxBinaryAttachmentsPerEvent) {
        enqueuePacket(sid, protocol::makeSocketIoErrorEventPacket(
                               ackPacket.nsp, constants::kCodeTooManyBinaryAttachments,
                               constants::kMessageTooManyBinaryAttachments));
        return;
      }

      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto sessionIt = sessions.find(sid);
      if (sessionIt == sessions.end()) {
        return;
      }
      if (sessionIt->second.pendingBinaryExpectedAttachmentCount > 0) {
        enqueuePacketUnlocked(
            sessionIt->second, protocol::makeSocketIoErrorEventPacket(
                                   sessionIt->second.pendingBinaryNsp, constants::kCodeBinaryAttachmentCountMismatch,
                                   constants::kMessageBinaryAttachmentCountMismatch));
      }
      sessionIt->second.pendingBinaryNsp = ackPacket.nsp;
      sessionIt->second.pendingBinaryAckId = ackPacket.ackId;
      sessionIt->second.pendingBinaryEventPayload = ackPacket.ackPayload;
      sessionIt->second.pendingBinaryIsAck = true;
      sessionIt->second.pendingBinaryExpectedAttachmentCount = static_cast<std::size_t>(ackPacket.attachmentCount);
      sessionIt->second.pendingBinaryTotalBytes = 0;
      sessionIt->second.pendingBinaryAttachments.clear();
      return;
    }
    dispatchSocketIoAckData(sid, ackPacket.nsp, ackPacket.ackId, ackPacket.ackPayload);
    return;
  }

  if (packetType == protocol::SocketIoPacketType::event ||
      packetType == protocol::SocketIoPacketType::binaryEvent) {
    if (packetType == protocol::SocketIoPacketType::binaryEvent) {
      protocol::SocketIoEventPacket parsedBinaryEvent;
      if (!protocol::parseSocketIoEventPacket(packet, parsedBinaryEvent) || parsedBinaryEvent.attachmentCount <= 0) {
        enqueuePacket(sid, protocol::makeSocketIoErrorEventPacket(
                             "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket));
        return;
      }

      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto sessionIt = sessions.find(sid);
      if (sessionIt == sessions.end()) {
        return;
      }
      if (sessionIt->second.pendingBinaryExpectedAttachmentCount > 0) {
        enqueuePacketUnlocked(
            sessionIt->second, protocol::makeSocketIoErrorEventPacket(
                                   sessionIt->second.pendingBinaryNsp, constants::kCodeBinaryAttachmentCountMismatch,
                                   constants::kMessageBinaryAttachmentCountMismatch));
      }
      sessionIt->second.pendingBinaryNsp = parsedBinaryEvent.nsp;
      sessionIt->second.pendingBinaryAckId = parsedBinaryEvent.ackId;
      sessionIt->second.pendingBinaryEventPayload = parsedBinaryEvent.eventPayload;
      sessionIt->second.pendingBinaryIsAck = false;
      sessionIt->second.pendingBinaryExpectedAttachmentCount =
          static_cast<std::size_t>(parsedBinaryEvent.attachmentCount);
      sessionIt->second.pendingBinaryTotalBytes = 0;
      sessionIt->second.pendingBinaryAttachments.clear();
      return;
    }

    dispatchSocketIoEvent(sid, packet, [this, sid](const std::string& packetToSend) {
      enqueuePacket(sid, packetToSend);
    });
    return;
  }

  if (packetType == protocol::SocketIoPacketType::unknown) {
    enqueuePacket(sid, protocol::makeSocketIoErrorEventPacket(
                           "/", constants::kCodeMalformedEventPacket, constants::kMessageMalformedEventPacket));
  }
}

bool Server::handleHttpRequest(const std::string& request, std::string& response)
{
  const std::string method = utils::parseRequestMethod(request);
  const std::string target = utils::parseRequestTarget(request);
  std::string path;
  std::string queryString;
  utils::splitTarget(target, path, queryString);

  if (path == "/") {
    response = utils::makeHttpResponse(constants::kHttpStatusOk, constants::kBodyAlive);
    return true;
  }

  if (path != kSocketIoPath && path != "/socket.io") {
    return false;
  }

  const auto query = utils::parseQuery(queryString);
  const auto eioIt = query.find("EIO");
  const auto transportIt = query.find("transport");
  if (eioIt == query.end() || transportIt == query.end()) {
    response = utils::makeHttpResponse(constants::kHttpStatusBadRequest, constants::kBodyMissingQuery);
    return true;
  }

  if (!protocol::isEngineIoVersion4(eioIt->second) || !protocol::isPollingTransport(transportIt->second)) {
    response = utils::makeHttpResponse(constants::kHttpStatusBadRequest, constants::kBodyUnsupportedTransport);
    return true;
  }

  if (method == "GET") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      const std::string sid = createSession();
      const std::string privateId = createPrivateId();
      SessionState session;
      session.sid = sid;
      session.privateId = privateId;
      session.lastSeenAt = std::chrono::steady_clock::now();
      session.connectedNamespaces.insert("/");
      {
        std::lock_guard<std::mutex> lock(sessionsMutex);
        sessions.emplace(sid, std::move(session));
      }
      const std::string openPacket = protocol::makeEngineIoOpenPacket(
          sid, privateId, config.pingIntervalMs, config.pingTimeoutMs,
          static_cast<std::uint32_t>(config.maxIncomingPacketBytes));
      response = utils::makeHttpResponse(constants::kHttpStatusOk, openPacket);
      return true;
    }

    std::deque<std::string> pendingPackets;
    std::uint64_t recoveryOffset = 0;
    bool hasRecoveryOffset = false;
    const auto offsetIt = query.find("offset");
    if (offsetIt != query.end()) {
      hasRecoveryOffset = parseOffsetValue(offsetIt->second, recoveryOffset);
    }
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto it = sessions.find(sidIt->second);
      if (it == sessions.end()) {
        response = utils::makeHttpResponse(constants::kHttpStatusBadRequest, constants::kBodyUnknownSid);
        return true;
      }
      it->second.online = true;
      it->second.disconnectedAt = std::chrono::steady_clock::time_point::min();
      it->second.lastSeenAt = std::chrono::steady_clock::now();
      bool recoveryAllowed = config.enableSessionRecovery && hasRecoveryOffset;
      const auto privateIdIt = query.find("pid");
      if (recoveryAllowed) {
        if (privateIdIt == query.end() || privateIdIt->second != it->second.privateId) {
          recoveryAllowed = false;
        }
      }
      if (recoveryAllowed) {
        collectPacketsSinceOffsetUnlocked(it->second, recoveryOffset, pendingPackets);
      } else {
        pendingPackets.swap(it->second.outgoingPackets);
      }
    }

    if (pendingPackets.empty()) {
      response = utils::makeHttpResponse(constants::kHttpStatusOk, protocol::kEngineIoPacketNoop);
      return true;
    }

    response = utils::makeHttpResponse(constants::kHttpStatusOk, utils::joinEngineIoPayload(pendingPackets));
    return true;
  }

  if (method == "POST") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      response = utils::makeHttpResponse(constants::kHttpStatusBadRequest, constants::kBodyMissingSid);
      return true;
    }

    const std::string sid = sidIt->second;
    if (!hasSession(sid)) {
      response = utils::makeHttpResponse(constants::kHttpStatusBadRequest, constants::kBodyUnknownSid);
      return true;
    }
    touchSession(sid);

    const std::string body = utils::parseRequestBody(request);
    if (config.maxPollingBodyBytes > 0 && body.size() > config.maxPollingBodyBytes) {
      response = utils::makeHttpResponse(constants::kHttpStatusPayloadTooLarge, constants::kMessagePayloadTooLarge);
      return true;
    }
    const auto packets = utils::splitEngineIoPayload(body);
    if (config.maxPacketsPerPollingPost > 0 && packets.size() > config.maxPacketsPerPollingPost) {
      response = utils::makeHttpResponse(constants::kHttpStatusPayloadTooLarge, constants::kBodyTooManyPackets);
      return true;
    }
    for (const std::string& packet : packets) {
      processEngineIoPacket(sid, packet);
    }

    response = utils::makeHttpResponse(constants::kHttpStatusOk, constants::kBodyOk);
    return true;
  }

  response = utils::makeHttpResponse(constants::kHttpStatusMethodNotAllowed, constants::kBodyMethodNotAllowed);
  return true;
}

}  // namespace socketIoServer
