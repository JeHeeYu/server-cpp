#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace socketIoServer {

struct ServerConfig {
  std::string host = "0.0.0.0";
  std::uint16_t port = 1000;
  std::uint16_t maxConnections = 1024;
  std::uint32_t pingIntervalMs = 25000;
  std::uint32_t pingTimeoutMs = 20000;
};

class Server {
 public:
  explicit Server(ServerConfig config);

  bool start();
  void stop();
  bool isRunning() const;

 private:
  struct SessionState {
    std::string sid;
    bool namespaceConnected = false;
    std::deque<std::string> outgoingPackets;
    std::chrono::steady_clock::time_point lastSeenAt = std::chrono::steady_clock::now();
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
  void removeSession(const std::string& sid);
  std::chrono::milliseconds sessionTtl() const;
  bool handleHttpRequest(const std::string& request, std::string& response);
  void enqueuePacket(const std::string& sid, const std::string& packet);

  ServerConfig config;
  std::atomic<bool> running{false};
  int listenFd = -1;
  std::thread acceptThread;
  std::thread sessionThread;
  std::atomic<std::uint64_t> nextSid{1};
  std::mutex sessionsMutex;
  std::unordered_map<std::string, SessionState> sessions;
};

}  // namespace socketIoServer
