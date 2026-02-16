#include "server.h"
#include "protocol/types.h"

#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace socketIoServer {

Server::BroadcastBuilder::BroadcastBuilder(Server& serverRef, std::string nspValue)
    : server(serverRef), nsp(std::move(nspValue))
{
}

Server::BroadcastBuilder& Server::BroadcastBuilder::to(const std::string& room)
{
  if (!room.empty()) {
    rooms.push_back(room);
  }
  return *this;
}

Server::BroadcastBuilder& Server::BroadcastBuilder::in(const std::string& room)
{
  return to(room);
}

Server::BroadcastBuilder& Server::BroadcastBuilder::except(const std::string& sid)
{
  if (!sid.empty()) {
    excludedSids.push_back(sid);
  }
  return *this;
}

Server::BroadcastBuilder& Server::BroadcastBuilder::volatileBroadcast(bool enable)
{
  isVolatile = enable;
  return *this;
}

void Server::BroadcastBuilder::emit(const std::string& eventName, const std::string& jsonObjectPayload)
{
  if (isVolatile) {
    server.emitToRoomsEventVolatile(nsp, rooms, eventName, jsonObjectPayload, excludedSids);
    return;
  }
  server.emitToRoomsEvent(nsp, rooms, eventName, jsonObjectPayload, excludedSids);
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

void Server::addNamespaceMiddleware(NamespaceMiddleware middleware)
{
  std::lock_guard<std::mutex> lock(namespaceMiddlewareMutex);
  namespaceMiddlewares.push_back(std::move(middleware));
}

void Server::clearNamespaceMiddlewares()
{
  std::lock_guard<std::mutex> lock(namespaceMiddlewareMutex);
  namespaceMiddlewares.clear();
}

void Server::addEventMiddleware(EventMiddleware middleware)
{
  std::lock_guard<std::mutex> lock(eventMiddlewareMutex);
  eventMiddlewares.push_back(std::move(middleware));
}

void Server::clearEventMiddlewares()
{
  std::lock_guard<std::mutex> lock(eventMiddlewareMutex);
  eventMiddlewares.clear();
}

void Server::setClientAckHandler(ClientAckHandler handler)
{
  std::lock_guard<std::mutex> lock(ackHandlerMutex);
  clientAckHandler = std::move(handler);
}

Server::BroadcastBuilder Server::to(const std::string& room, const std::string& nsp)
{
  BroadcastBuilder builder(*this, protocol::normalizeNamespace(nsp));
  builder.to(room);
  return builder;
}

Server::BroadcastBuilder Server::in(const std::string& room, const std::string& nsp)
{
  return to(room, nsp);
}

bool Server::emitToSidEventWithAck(
    const std::string& sid, const std::string& nsp, const std::string& eventName, const std::string& jsonObjectPayload,
    std::uint32_t timeoutMs, ClientEventAckCallback callback)
{
  if (sid.empty() || eventName.empty() || !callback) {
    return false;
  }

  const std::string normalizedNsp = protocol::normalizeNamespace(nsp);
  if (!hasSession(sid) || !isNamespaceConnected(sid, normalizedNsp)) {
    return false;
  }

  const std::string ackId = std::to_string(nextOutboundAckId.fetch_add(1));
  PendingAckState state;
  state.sid = sid;
  state.nsp = normalizedNsp;
  state.ackId = ackId;
  state.expiresAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  state.callback = std::move(callback);

  {
    std::lock_guard<std::mutex> lock(pendingAcksMutex);
    pendingAcks.emplace(buildPendingAckKey(sid, normalizedNsp, ackId), std::move(state));
  }

  enqueuePacket(
      sid, protocol::makeSocketIoEventPacket(eventName, jsonObjectPayload, normalizedNsp, ackId));
  return true;
}

Server::ConnectDecision Server::evaluateNamespaceConnect(
    const std::string& sid, const std::string& nsp, const std::string& authJson)
{
  std::vector<NamespaceMiddleware> middlewareChain;
  {
    std::lock_guard<std::mutex> lock(namespaceMiddlewareMutex);
    middlewareChain = namespaceMiddlewares;
  }

  const std::string normalizedNsp = protocol::normalizeNamespace(nsp);
  for (const auto& middleware : middlewareChain) {
    if (!middleware) {
      continue;
    }

    const std::optional<ConnectDecision> decision = middleware(*this, sid, normalizedNsp, authJson);
    if (decision.has_value()) {
      return *decision;
    }
  }

  NamespaceConnectHandler handlerCopy;
  {
    std::lock_guard<std::mutex> lock(connectHandlerMutex);
    handlerCopy = namespaceConnectHandler;
  }
  if (!handlerCopy) {
    return ConnectDecision{};
  }
  return handlerCopy(*this, sid, normalizedNsp, authJson);
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

std::optional<Server::ConnectDecision> Server::evaluateEventMiddleware(const InboundEventContext& context)
{
  std::vector<EventMiddleware> middlewareChain;
  {
    std::lock_guard<std::mutex> lock(eventMiddlewareMutex);
    middlewareChain = eventMiddlewares;
  }

  for (const auto& middleware : middlewareChain) {
    if (!middleware) {
      continue;
    }

    std::optional<ConnectDecision> decision = middleware(*this, context);
    if (decision.has_value()) {
      return decision;
    }
  }

  return std::nullopt;
}

std::chrono::milliseconds Server::sessionTtl() const
{
  return std::chrono::milliseconds(
      static_cast<std::uint64_t>(config.pingIntervalMs) + static_cast<std::uint64_t>(config.pingTimeoutMs));
}

std::string Server::buildPendingAckKey(const std::string& sid, const std::string& nsp, const std::string& ackId) const
{
  return sid + "|" + nsp + "|" + ackId;
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

}  // namespace socketIoServer
