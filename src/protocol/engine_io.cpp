#include "protocol/types.h"
#include "utils/json_util.h"

namespace socketIoServer::protocol {

bool isEngineIoVersion4(const std::string& version)
{
  return version == kEngineIoVersion;
}

bool isPollingTransport(const std::string& transport)
{
  return transport == kEngineIoTransportPolling;
}

bool isWebSocketTransport(const std::string& transport)
{
  return transport == kEngineIoTransportWebSocket;
}

EngineIoControlPacket parseEngineIoControlPacket(const std::string& packet)
{
  if (packet == kEngineIoPacketClose) {
    return EngineIoControlPacket::close;
  }
  if (packet == kEngineIoPacketPing) {
    return EngineIoControlPacket::ping;
  }
  if (packet == kEngineIoPacketPong) {
    return EngineIoControlPacket::pong;
  }
  if (packet == kEngineIoPacketNoop) {
    return EngineIoControlPacket::noop;
  }
  return EngineIoControlPacket::unknown;
}

std::string toWirePacket(EngineIoControlPacket packetType)
{
  if (packetType == EngineIoControlPacket::ping) {
    return kEngineIoPacketPing;
  }
  if (packetType == EngineIoControlPacket::pong) {
    return kEngineIoPacketPong;
  }
  if (packetType == EngineIoControlPacket::noop) {
    return kEngineIoPacketNoop;
  }
  return "";
}

std::string makeEngineIoOpenPacket(
    const std::string& sid, const std::string& privateId, std::uint32_t pingIntervalMs, std::uint32_t pingTimeoutMs,
    std::uint32_t maxPayload)
{
  return std::string(kEngineIoPacketOpenPrefix) +
         utils::makeEngineIoOpenPayload(sid, privateId, pingIntervalMs, pingTimeoutMs, maxPayload);
}

std::string makeEngineIoProbePongPacket()
{
  return kEngineIoPacketProbePong;
}

std::string makeEngineIoUpgradePacket()
{
  return kEngineIoPacketUpgrade;
}

bool isEngineIoProbePingPacket(const std::string& packet)
{
  return packet == kEngineIoPacketProbePing;
}

bool isEngineIoUpgradePacket(const std::string& packet)
{
  return packet == kEngineIoPacketUpgrade;
}

}  // namespace socketIoServer::protocol
