#pragma once

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
  struct PendingAckState {
    std::string sid;
    std::string nsp;
    std::string ackId;
    std::chrono::steady_clock::time_point expiresAt;
    ClientEventAckCallback callback;
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
  void broadcastToRoom(
      const std::string& nsp, const std::string& room, const std::string& packet, bool isVolatile,
      const std::string& excludeSid = "");
  void broadcastToRoom(
      const std::string& nsp, const std::string& room, const std::string& packet, bool isVolatile,
      const std::vector<std::string>& excludedSids);
  bool handleHttpRequest(const std::string& request, std::string& response);
  void enqueuePacket(const std::string& sid, const std::string& packet);
  void enqueuePacketWithPolicy(const std::string& sid, const std::string& packet, bool isVolatile);
  void enqueuePacketUnlocked(SessionState& session, const std::string& packet);
  void registerWebSocketClient(int clientFd, const std::string& sid);
  void unregisterWebSocketClient(int clientFd);
  void resetPendingBinaryState(SessionState& session);
  bool consumeInboundPacketBudget(const std::string& sid);
  std::string buildPendingAckKey(const std::string& sid, const std::string& nsp, const std::string& ackId) const;

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
  std::mutex namespaceMiddlewareMutex;
  std::mutex ackHandlerMutex;
  std::mutex pendingAcksMutex;
  EventHandler eventHandler;
  EventGuardHandler eventGuardHandler;
  std::vector<EventMiddleware> eventMiddlewares;
  NamespaceConnectHandler namespaceConnectHandler;
  std::vector<NamespaceMiddleware> namespaceMiddlewares;
  ClientAckHandler clientAckHandler;
  std::unordered_map<std::string, PendingAckState> pendingAcks;
  std::atomic<std::uint64_t> nextOutboundAckId{1};
  std::unordered_map<std::string, SessionState> sessions;
  std::unordered_map<std::string, std::unordered_set<std::string>> roomMembers;
  std::unordered_map<std::string, std::unordered_set<std::string>> sessionRooms;
  std::mutex webSocketClientsMutex;
  std::unordered_map<int, std::string> webSocketClients;
