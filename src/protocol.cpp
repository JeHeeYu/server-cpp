#include "protocol.h"

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

SocketIoPacketType parseSocketIoPacketType(const std::string& packet)
{
  if (packet.rfind("40", 0) == 0) {
    return SocketIoPacketType::connect;
  }
  if (packet.rfind("42", 0) == 0) {
    return SocketIoPacketType::event;
  }
  if (packet.rfind("43", 0) == 0) {
    return SocketIoPacketType::ack;
  }
  return SocketIoPacketType::unknown;
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

std::string makeSocketIoConnectPacket(const std::string& sid)
{
  return "40{\"sid\":\"" + sid + "\"}";
}

std::string makeSocketIoAckPacket(const std::string& ackId)
{
  return "43" + ackId + "[{\"ok\":true}]";
}

bool hasPingEventName(const std::string& payload)
{
  return payload.find("\"ping\"") != std::string::npos;
}

bool isEngineIoProbePingPacket(const std::string& packet)
{
  return packet == "2probe";
}

bool isEngineIoUpgradePacket(const std::string& packet)
{
  return packet == "5";
}

std::size_t socketIoPayloadStartOffset(SocketIoPacketType packetType)
{
  if (packetType == SocketIoPacketType::connect || packetType == SocketIoPacketType::event ||
      packetType == SocketIoPacketType::ack) {
    return 2;
  }
  return 0;
}

}  // namespace socketIoServer::protocol
