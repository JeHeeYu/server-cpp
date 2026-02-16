#include "protocol/types.h"

namespace socketIoServer::protocol {

bool isEngineIoVersion4(const std::string& version)
{
  return version == "4";
}

bool isPollingTransport(const std::string& transport)
{
  return transport == "polling";
}

bool isWebSocketTransport(const std::string& transport)
{
  return transport == "websocket";
}

EngineIoControlPacket parseEngineIoControlPacket(const std::string& packet)
{
  if (packet == "1") {
    return EngineIoControlPacket::close;
  }
  if (packet == "2") {
    return EngineIoControlPacket::ping;
  }
  if (packet == "3") {
    return EngineIoControlPacket::pong;
  }
  if (packet == "6") {
    return EngineIoControlPacket::noop;
  }
  return EngineIoControlPacket::unknown;
}

std::string toWirePacket(EngineIoControlPacket packetType)
{
  if (packetType == EngineIoControlPacket::ping) {
    return "2";
  }
  if (packetType == EngineIoControlPacket::pong) {
    return "3";
  }
  if (packetType == EngineIoControlPacket::noop) {
    return "6";
  }
  return "";
}

std::string makeEngineIoOpenPacket(const std::string& sid)
{
  return "0{\"sid\":\"" + sid +
         "\",\"upgrades\":[\"websocket\"],\"pingInterval\":25000,\"pingTimeout\":20000,\"maxPayload\":1000000}";
}

std::string makeEngineIoProbePongPacket()
{
  return "3probe";
}

std::string makeEngineIoUpgradePacket()
{
  return "5";
}

bool isEngineIoProbePingPacket(const std::string& packet)
{
  return packet == "2probe";
}

bool isEngineIoUpgradePacket(const std::string& packet)
{
  return packet == "5";
}

}  // namespace socketIoServer::protocol
