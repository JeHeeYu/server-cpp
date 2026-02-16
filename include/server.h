#pragma once

#include "server_config.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace socketIoServer {

class Server {
 public:
  class BroadcastBuilder {
   public:
    BroadcastBuilder(Server& serverRef, std::string nspValue);
    BroadcastBuilder& to(const std::string& room);
    BroadcastBuilder& in(const std::string& room);
    BroadcastBuilder& except(const std::string& sid);
    BroadcastBuilder& volatileBroadcast(bool enable = true);
    void emit(const std::string& eventName, const std::string& jsonObjectPayload);

   private:
    Server& server;
    std::string nsp;
    std::vector<std::string> rooms;
    std::vector<std::string> excludedSids;
    bool isVolatile = false;
  };

  using AckCallback = std::function<void(const std::string& ackJsonArrayPayload)>;
  struct ConnectDecision {
    bool allowed = true;
    int code = 403;
    std::string message = "namespace rejected";
  };
  struct InboundEventContext {
    std::string sid;
    std::string nsp;
    std::string eventName;
    std::string eventData;
    std::string ackId;
  };
  using EventMiddleware = std::function<std::optional<ConnectDecision>(Server& server, const InboundEventContext& context)>;
  using EventHandler = std::function<void(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& eventName,
      const std::string& eventData, const AckCallback& ack)>;
  using EventGuardHandler = std::function<ConnectDecision(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& eventName,
      const std::string& eventData)>;
  using NamespaceConnectHandler = std::function<ConnectDecision(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& authJson)>;
  using NamespaceMiddleware = std::function<std::optional<ConnectDecision>(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& authJson)>;
  using ClientAckHandler = std::function<void(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& ackId,
      const std::string& ackPayload)>;
  using ClientEventAckCallback = std::function<void(bool success, const std::string& ackPayload)>;

  explicit Server(ServerConfig config);

  bool start();
  void stop();
  bool isRunning() const;
  void setEventHandler(EventHandler handler);
  void setEventGuardHandler(EventGuardHandler handler);
  void setNamespaceConnectHandler(NamespaceConnectHandler handler);
  void addNamespaceMiddleware(NamespaceMiddleware middleware);
  void clearNamespaceMiddlewares();
  void addEventMiddleware(EventMiddleware middleware);
  void clearEventMiddlewares();
  void setClientAckHandler(ClientAckHandler handler);
  bool emitToSidEventWithAck(
      const std::string& sid, const std::string& nsp, const std::string& eventName,
      const std::string& jsonObjectPayload, std::uint32_t timeoutMs, ClientEventAckCallback callback);
  void joinRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void leaveRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void emitToRoomEvent(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::string& excludeSid = "");
  void emitToRoomEvent(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids);
  void emitToRoomEventVolatile(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::string& excludeSid = "");
  void emitToRoomEventVolatile(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids);
  void emitToRoomsEvent(
      const std::string& nsp, const std::vector<std::string>& rooms, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids = {});
  void emitToRoomsEventVolatile(
      const std::string& nsp, const std::vector<std::string>& rooms, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids = {});
  BroadcastBuilder to(const std::string& room, const std::string& nsp = "/");
  BroadcastBuilder in(const std::string& room, const std::string& nsp = "/");

 private:
#include "server_internal.h"
};

}  // namespace socketIoServer
