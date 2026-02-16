#pragma once

#include <cstdint>
#include <string>

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

}  // namespace socketIoServer
