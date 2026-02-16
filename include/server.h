#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <vector>

namespace socketIoServer {

struct ServerConfig {
  enum class OutgoingOverflowPolicy {
    dropOldest,
    dropNewest
  };

  std::string host = "0.0.0.0";
  std::uint16_t port = 1000;
  std::uint16_t maxConnections = 1024;
  std::uint32_t pingIntervalMs = 25000;
  std::uint32_t pingTimeoutMs = 20000;
  std::uint32_t sessionRecoveryMs = 30000;
  std::size_t maxInboundPacketsPerSecond = 0;
  std::size_t maxPacketsPerPollingPost = 0;
  std::size_t maxPollingBodyBytes = 0;
  std::size_t maxOutgoingPacketsPerSession = 1024;
  OutgoingOverflowPolicy outgoingOverflowPolicy = OutgoingOverflowPolicy::dropOldest;
  std::size_t maxIncomingPacketBytes = 1024 * 1024;
  std::size_t maxBinaryAttachmentsPerEvent = 16;
  std::size_t maxBinaryAttachmentBytes = 2 * 1024 * 1024;
  std::size_t maxBinaryTotalBytesPerEvent = 8 * 1024 * 1024;
  std::size_t maxNamespaceLength = 128;
  std::size_t maxEventNameLength = 128;
  std::size_t maxRoomNameLength = 128;
};

class Server {
 public:
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
  using ClientAckHandler = std::function<void(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& ackId,
      const std::string& ackPayload)>;

  explicit Server(ServerConfig config);

  bool start();
  void stop();
  bool isRunning() const;
  void setEventHandler(EventHandler handler);
  void setEventGuardHandler(EventGuardHandler handler);
  void setNamespaceConnectHandler(NamespaceConnectHandler handler);
  void addEventMiddleware(EventMiddleware middleware);
  void clearEventMiddlewares();
  void setClientAckHandler(ClientAckHandler handler);
  void joinRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void leaveRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void emitToRoomEvent(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::string& excludeSid = "");
  void emitToRoomEvent(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::vector<std::string>& excludedSids);

 private:
  struct SessionState {
    std::string sid;
    std::unordered_set<std::string> connectedNamespaces;
    std::deque<std::string> outgoingPackets;
    bool online = true;
    std::string pendingBinaryNsp;
    std::string pendingBinaryAckId;
    std::string pendingBinaryEventPayload;
    bool pendingBinaryIsAck = false;
    std::size_t pendingBinaryExpectedAttachmentCount = 0;
    std::size_t pendingBinaryTotalBytes = 0;
    std::vector<std::string> pendingBinaryAttachments;
    std::chrono::steady_clock::time_point inboundWindowStart = std::chrono::steady_clock::time_point::min();
    std::size_t inboundPacketCountInWindow = 0;
    std::chrono::steady_clock::time_point lastSeenAt = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point disconnectedAt = std::chrono::steady_clock::time_point::min();
  };

  void acceptLoop();
  void sessionLoop();
  void handleClient(int clientFd);
  void processEngineIoPacket(const std::string& sid, const std::string& packet);
  bool handleWebSocketHandshake(
      int clientFd, const std::string& request, const std::unordered_map<std::string, std::string>& query);
  void serveWebSocket(int clientFd, const std::string& sid);
  std::string createSession();
  bool hasSession(const std::string& sid);
  void touchSession(const std::string& sid);
  void markSessionDisconnected(const std::string& sid);
  void removeSession(const std::string& sid);
  void connectNamespace(const std::string& sid, const std::string& nsp);
  void disconnectNamespace(const std::string& sid, const std::string& nsp);
  bool isNamespaceConnected(const std::string& sid, const std::string& nsp);
  ConnectDecision evaluateNamespaceConnect(
      const std::string& sid, const std::string& nsp, const std::string& authJson);
  ConnectDecision evaluateEventGuard(
      const std::string& sid, const std::string& nsp, const std::string& eventName, const std::string& eventData);
  std::optional<ConnectDecision> evaluateEventMiddleware(const InboundEventContext& context);
  std::chrono::milliseconds sessionTtl() const;
  void dispatchSocketIoEvent(
      const std::string& sid, const std::string& packet, const std::function<void(const std::string&)>& sendPacket);
  void dispatchSocketIoEventData(
      const std::string& sid, const std::string& nsp, const std::string& ackId, const std::string& eventName,
      const std::string& eventData, const std::function<void(const std::string&)>& sendPacket);
  void dispatchSocketIoAckData(
      const std::string& sid, const std::string& nsp, const std::string& ackId, const std::string& ackPayload);
  std::string makeRoomKey(const std::string& nsp, const std::string& room) const;
  void broadcastToRoom(
      const std::string& nsp, const std::string& room, const std::string& packet,
      const std::string& excludeSid = "");
  void broadcastToRoom(
      const std::string& nsp, const std::string& room, const std::string& packet,
      const std::vector<std::string>& excludedSids);
  bool handleHttpRequest(const std::string& request, std::string& response);
  void enqueuePacket(const std::string& sid, const std::string& packet);
  void enqueuePacketUnlocked(SessionState& session, const std::string& packet);
  void registerWebSocketClient(int clientFd, const std::string& sid);
  void unregisterWebSocketClient(int clientFd);
  void resetPendingBinaryState(SessionState& session);
  bool consumeInboundPacketBudget(const std::string& sid);

  ServerConfig config;
  std::atomic<bool> running{false};
  int listenFd = -1;
  std::thread acceptThread;
  std::thread sessionThread;
  std::atomic<std::uint64_t> nextSid{1};
  std::mutex sessionsMutex;
  std::mutex eventHandlerMutex;
  std::mutex eventGuardMutex;
  std::mutex eventMiddlewareMutex;
  std::mutex connectHandlerMutex;
  std::mutex ackHandlerMutex;
  EventHandler eventHandler;
  EventGuardHandler eventGuardHandler;
  std::vector<EventMiddleware> eventMiddlewares;
  NamespaceConnectHandler namespaceConnectHandler;
  ClientAckHandler clientAckHandler;
  std::unordered_map<std::string, SessionState> sessions;
  std::unordered_map<std::string, std::unordered_set<std::string>> roomMembers;
  std::unordered_map<std::string, std::unordered_set<std::string>> sessionRooms;
  std::mutex webSocketClientsMutex;
  std::unordered_map<int, std::string> webSocketClients;
};

}  // namespace socketIoServer
