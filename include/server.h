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
#include <vector>

namespace socketIoServer {

struct ServerConfig {
  std::string host = "0.0.0.0";
  std::uint16_t port = 1000;
  std::uint16_t maxConnections = 1024;
  std::uint32_t pingIntervalMs = 25000;
  std::uint32_t pingTimeoutMs = 20000;
  std::uint32_t sessionRecoveryMs = 30000;
  std::size_t maxOutgoingPacketsPerSession = 1024;
  std::size_t maxIncomingPacketBytes = 1024 * 1024;
  std::size_t maxBinaryAttachmentsPerEvent = 16;
  std::size_t maxBinaryAttachmentBytes = 2 * 1024 * 1024;
};

class Server {
 public:
  using AckCallback = std::function<void(const std::string& ackJsonArrayPayload)>;
  struct ConnectDecision {
    bool allowed = true;
    int code = 403;
    std::string message = "namespace rejected";
  };
  using EventHandler = std::function<void(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& eventName,
      const std::string& eventData, const AckCallback& ack)>;
  using EventGuardHandler = std::function<ConnectDecision(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& eventName,
      const std::string& eventData)>;
  using NamespaceConnectHandler = std::function<ConnectDecision(
      Server& server, const std::string& sid, const std::string& nsp, const std::string& authJson)>;

  explicit Server(ServerConfig config);

  bool start();
  void stop();
  bool isRunning() const;
  void setEventHandler(EventHandler handler);
  void setEventGuardHandler(EventGuardHandler handler);
  void setNamespaceConnectHandler(NamespaceConnectHandler handler);
  void joinRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void leaveRoom(const std::string& sid, const std::string& nsp, const std::string& room);
  void emitToRoomEvent(
      const std::string& nsp, const std::string& room, const std::string& eventName,
      const std::string& jsonObjectPayload, const std::string& excludeSid = "");

 private:
  struct SessionState {
    std::string sid;
    std::unordered_set<std::string> connectedNamespaces;
    std::deque<std::string> outgoingPackets;
    bool online = true;
    std::string pendingBinaryNsp;
    std::string pendingBinaryAckId;
    std::string pendingBinaryEventPayload;
    std::size_t pendingBinaryExpectedAttachmentCount = 0;
    std::size_t pendingBinaryTotalBytes = 0;
    std::vector<std::string> pendingBinaryAttachments;
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
  std::chrono::milliseconds sessionTtl() const;
  void dispatchSocketIoEvent(
      const std::string& sid, const std::string& packet, const std::function<void(const std::string&)>& sendPacket);
  void dispatchSocketIoEventData(
      const std::string& sid, const std::string& nsp, const std::string& ackId, const std::string& eventName,
      const std::string& eventData, const std::function<void(const std::string&)>& sendPacket);
  std::string makeRoomKey(const std::string& nsp, const std::string& room) const;
  void broadcastToRoom(
      const std::string& nsp, const std::string& room, const std::string& packet,
      const std::string& excludeSid = "");
  bool handleHttpRequest(const std::string& request, std::string& response);
  void enqueuePacket(const std::string& sid, const std::string& packet);
  void registerWebSocketClient(int clientFd, const std::string& sid);
  void unregisterWebSocketClient(int clientFd);
  void resetPendingBinaryState(SessionState& session);

  ServerConfig config;
  std::atomic<bool> running{false};
  int listenFd = -1;
  std::thread acceptThread;
  std::thread sessionThread;
  std::atomic<std::uint64_t> nextSid{1};
  std::mutex sessionsMutex;
  std::mutex eventHandlerMutex;
  std::mutex eventGuardMutex;
  std::mutex connectHandlerMutex;
  EventHandler eventHandler;
  EventGuardHandler eventGuardHandler;
  NamespaceConnectHandler namespaceConnectHandler;
  std::unordered_map<std::string, SessionState> sessions;
  std::unordered_map<std::string, std::unordered_set<std::string>> roomMembers;
  std::unordered_map<std::string, std::unordered_set<std::string>> sessionRooms;
  std::mutex webSocketClientsMutex;
  std::unordered_map<int, std::string> webSocketClients;
};

}  // namespace socketIoServer
