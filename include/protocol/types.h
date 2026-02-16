#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace socketIoServer::protocol {

inline constexpr const char* kEngineIoVersion = "4";
inline constexpr const char* kEngineIoTransportPolling = "polling";
inline constexpr const char* kEngineIoTransportWebSocket = "websocket";
inline constexpr const char* kEngineIoPacketOpenPrefix = "0";
inline constexpr const char* kEngineIoPacketClose = "1";
inline constexpr const char* kEngineIoPacketPing = "2";
inline constexpr const char* kEngineIoPacketPong = "3";
inline constexpr const char* kEngineIoPacketUpgrade = "5";
inline constexpr const char* kEngineIoPacketNoop = "6";
inline constexpr const char* kEngineIoPacketBinaryPrefix = "b";
inline constexpr const char* kEngineIoPacketProbePing = "2probe";
inline constexpr const char* kEngineIoPacketProbePong = "3probe";
inline constexpr std::uint32_t kEngineIoDefaultPingIntervalMs = 25000;
inline constexpr std::uint32_t kEngineIoDefaultPingTimeoutMs = 20000;
inline constexpr std::uint32_t kEngineIoDefaultMaxPayload = 1000000;

inline constexpr const char* kSocketIoPacketConnectPrefix = "40";
inline constexpr const char* kSocketIoPacketConnectErrorPrefix = "44";
inline constexpr const char* kSocketIoPacketDisconnectPrefix = "41";
inline constexpr const char* kSocketIoPacketEventPrefix = "42";
inline constexpr const char* kSocketIoPacketAckPrefix = "43";
inline constexpr const char* kSocketIoPacketBinaryEventPrefix = "45";
inline constexpr const char* kSocketIoPacketBinaryAckPrefix = "46";
inline constexpr const char* kSocketIoServerErrorEventName = "server_error";

enum class EngineIoControlPacket {
  unknown,
  close,
  ping,
  pong,
  noop
};

enum class SocketIoPacketType {
  unknown,
  connect,
  connectError,
  disconnect,
  event,
  ack,
  binaryEvent,
  binaryAck
};

bool isEngineIoVersion4(const std::string& version);
bool isPollingTransport(const std::string& transport);
bool isWebSocketTransport(const std::string& transport);
std::string normalizeNamespace(const std::string& nsp);

EngineIoControlPacket parseEngineIoControlPacket(const std::string& packet);
SocketIoPacketType parseSocketIoPacketType(const std::string& packet);
std::string toWirePacket(EngineIoControlPacket packetType);

std::string makeEngineIoOpenPacket(
    const std::string& sid, std::uint32_t pingIntervalMs, std::uint32_t pingTimeoutMs,
    std::uint32_t maxPayload = kEngineIoDefaultMaxPayload);
std::string makeEngineIoProbePongPacket();
std::string makeEngineIoUpgradePacket();
std::string makeSocketIoConnectPacket(const std::string& sid, const std::string& nsp = "/");
std::string makeSocketIoConnectErrorPacket(
    const std::string& nsp, int code, const std::string& message);
std::string makeSocketIoAckPacket(const std::string& ackId);
std::string makeSocketIoAckPacket(
    const std::string& ackId, const std::string& ackJsonArrayPayload, const std::string& nsp = "/");
std::string makeSocketIoEventPacket(
    const std::string& eventName, const std::string& jsonObjectPayload, const std::string& nsp = "/");
std::string makeSocketIoEventPacket(
    const std::string& eventName, const std::string& jsonObjectPayload, const std::string& nsp,
    const std::string& ackId);
std::string makeSocketIoErrorEventPacket(const std::string& nsp, int code, const std::string& message);

bool isEngineIoProbePingPacket(const std::string& packet);
bool isEngineIoUpgradePacket(const std::string& packet);
std::size_t socketIoPayloadStartOffset(SocketIoPacketType packetType);
std::string parseSocketIoNamespace(const std::string& packet);

struct SocketIoEventPacket {
  std::string nsp = "/";
  std::string ackId;
  std::string eventPayload;
  int attachmentCount = 0;
  bool isBinary = false;
};

struct SocketIoConnectPacket {
  std::string nsp = "/";
  std::string authJson;
};

struct SocketIoAckPacket {
  std::string nsp = "/";
  std::string ackId;
  std::string ackPayload;
  int attachmentCount = 0;
  bool isBinary = false;
};

bool parseSocketIoConnectPacket(
    const std::string& packet, SocketIoConnectPacket& connectPacketOut);
bool parseSocketIoEventPacket(
    const std::string& packet, SocketIoEventPacket& eventPacketOut);
bool parseSocketIoAckPacket(
    const std::string& packet, SocketIoAckPacket& ackPacketOut);
std::string parseSocketIoEventName(const std::string& eventPayload);
std::string parseSocketIoEventData(const std::string& eventPayload);
std::string parseSocketIoStringField(const std::string& eventPayload, const std::string& fieldName);
std::string mergeSocketIoBinaryEventData(
    const std::string& eventDataJson, const std::vector<std::string>& binaryAttachments);

}  // namespace socketIoServer::protocol
