#pragma once

#include <atomic>
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
  };

  void acceptLoop();
  std::string createSession();
  bool handleHttpRequest(const std::string& request, std::string& response);
  void enqueuePacket(const std::string& sid, const std::string& packet);

  ServerConfig config;
  std::atomic<bool> running{false};
  int listenFd = -1;
  std::thread acceptThread;
  std::atomic<std::uint64_t> nextSid{1};
  std::mutex sessionsMutex;
  std::unordered_map<std::string, SessionState> sessions;
};

}  // namespace socketIoServer
