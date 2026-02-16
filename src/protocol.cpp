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

SocketIoPacketType parseSocketIoPacketType(const std::string& packet)
{
  if (packet.rfind("40", 0) == 0) {
    return SocketIoPacketType::connect;
  }
  if (packet.rfind("41", 0) == 0) {
    return SocketIoPacketType::disconnect;
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

std::string makeSocketIoAckPacket(const std::string& ackId, const std::string& ackJsonArrayPayload)
{
  if (ackJsonArrayPayload.empty()) {
    return "43" + ackId + "[]";
  }
  return "43" + ackId + ackJsonArrayPayload;
}

std::string makeSocketIoEventPacket(const std::string& eventName, const std::string& jsonObjectPayload)
{
  return "42[\"" + eventName + "\"," + jsonObjectPayload + "]";
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

bool parseSocketIoEventPacket(
    const std::string& packet, std::string& ackIdOut, std::string& eventPayloadOut)
{
  ackIdOut.clear();
  eventPayloadOut.clear();

  if (parseSocketIoPacketType(packet) != SocketIoPacketType::event) {
    return false;
  }

  std::size_t pos = socketIoPayloadStartOffset(SocketIoPacketType::event);
  while (pos < packet.size() && packet[pos] >= '0' && packet[pos] <= '9') {
    ackIdOut.push_back(packet[pos]);
    ++pos;
  }
  eventPayloadOut = packet.substr(pos);
  return true;
}

std::string parseSocketIoEventName(const std::string& eventPayload)
{
  const std::string prefix = "[\"";
  const std::size_t start = eventPayload.find(prefix);
  if (start == std::string::npos) {
    return "";
  }
  const std::size_t nameStart = start + prefix.size();
  const std::size_t nameEnd = eventPayload.find('"', nameStart);
  if (nameEnd == std::string::npos || nameEnd <= nameStart) {
    return "";
  }
  return eventPayload.substr(nameStart, nameEnd - nameStart);
}

std::string parseSocketIoEventData(const std::string& eventPayload)
{
  const std::size_t commaPos = eventPayload.find(',');
  if (commaPos == std::string::npos) {
    return "";
  }
  const std::size_t endPos = eventPayload.rfind(']');
  if (endPos == std::string::npos || endPos <= commaPos + 1) {
    return "";
  }
  return eventPayload.substr(commaPos + 1, endPos - commaPos - 1);
}

std::string parseSocketIoStringField(const std::string& eventPayload, const std::string& fieldName)
{
  const std::string needle = "\"" + fieldName + "\":\"";
  const std::size_t start = eventPayload.find(needle);
  if (start == std::string::npos) {
    return "";
  }
  const std::size_t valueStart = start + needle.size();
  const std::size_t valueEnd = eventPayload.find('"', valueStart);
  if (valueEnd == std::string::npos || valueEnd <= valueStart) {
    return "";
  }
  return eventPayload.substr(valueStart, valueEnd - valueStart);
}

}  // namespace socketIoServer::protocol
