#pragma once

#include <cstddef>
#include <string>

namespace socketIoServer::protocol {

enum class EngineIoControlPacket {
  unknown,
  ping,
  pong,
  noop
};

enum class SocketIoPacketType {
  unknown,
  connect,
  event,
  ack
};

bool isEngineIoVersion4(const std::string& version);
bool isPollingTransport(const std::string& transport);
bool isWebSocketTransport(const std::string& transport);

EngineIoControlPacket parseEngineIoControlPacket(const std::string& packet);
SocketIoPacketType parseSocketIoPacketType(const std::string& packet);
std::string toWirePacket(EngineIoControlPacket packetType);

std::string makeEngineIoOpenPacket(const std::string& sid);
std::string makeSocketIoConnectPacket(const std::string& sid);
std::string makeSocketIoAckPacket(const std::string& ackId);

bool hasPingEventName(const std::string& payload);
std::size_t socketIoPayloadStartOffset(SocketIoPacketType packetType);

}  // namespace socketIoServer::protocol
