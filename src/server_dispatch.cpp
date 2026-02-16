#include "server.h"
#include "constants.h"
#include "protocol/types.h"

#include <string>

namespace socketIoServer {

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

  const InboundEventContext context{sid, protocol::normalizeNamespace(nsp), eventName, eventData, ackId};
  const std::optional<ConnectDecision> middlewareDecision = evaluateEventMiddleware(context);
  if (middlewareDecision.has_value() && !middlewareDecision->allowed) {
    sendPacket(protocol::makeSocketIoErrorEventPacket(nsp, middlewareDecision->code, middlewareDecision->message));
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

void Server::dispatchSocketIoAckData(
    const std::string& sid, const std::string& nsp, const std::string& ackId, const std::string& ackPayload)
{
  const std::string normalizedNsp = protocol::normalizeNamespace(nsp);
  ClientEventAckCallback pendingAckCallback;
  {
    std::lock_guard<std::mutex> lock(pendingAcksMutex);
    const std::string key = buildPendingAckKey(sid, normalizedNsp, ackId);
    const auto it = pendingAcks.find(key);
    if (it != pendingAcks.end()) {
      pendingAckCallback = std::move(it->second.callback);
      pendingAcks.erase(it);
    }
  }
  if (pendingAckCallback) {
    pendingAckCallback(true, ackPayload);
  }

  ClientAckHandler handlerCopy;
  {
    std::lock_guard<std::mutex> lock(ackHandlerMutex);
    handlerCopy = clientAckHandler;
  }
  if (!handlerCopy) {
    return;
  }
  handlerCopy(*this, sid, normalizedNsp, ackId, ackPayload);
}

}  // namespace socketIoServer
