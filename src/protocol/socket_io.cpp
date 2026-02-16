#include "protocol/types.h"
#include "utils/json_util.h"

#include <boost/json.hpp>
#include <boost/system/error_code.hpp>

#include <cstdlib>

namespace socketIoServer::protocol {

namespace {

std::string namespacePrefix(const std::string& packetTypeCode, const std::string& nsp)
{
  const std::string normalized = normalizeNamespace(nsp);
  if (normalized == "/") {
    return packetTypeCode;
  }
  return packetTypeCode + normalized + ",";
}

}  // namespace

SocketIoPacketType parseSocketIoPacketType(const std::string& packet)
{
  if (packet.rfind(kSocketIoPacketConnectPrefix, 0) == 0) {
    return SocketIoPacketType::connect;
  }
  if (packet.rfind(kSocketIoPacketConnectErrorPrefix, 0) == 0) {
    return SocketIoPacketType::connectError;
  }
  if (packet.rfind(kSocketIoPacketDisconnectPrefix, 0) == 0) {
    return SocketIoPacketType::disconnect;
  }
  if (packet.rfind(kSocketIoPacketEventPrefix, 0) == 0) {
    return SocketIoPacketType::event;
  }
  if (packet.rfind(kSocketIoPacketAckPrefix, 0) == 0) {
    return SocketIoPacketType::ack;
  }
  if (packet.rfind(kSocketIoPacketBinaryEventPrefix, 0) == 0) {
    return SocketIoPacketType::binaryEvent;
  }
  if (packet.rfind(kSocketIoPacketBinaryAckPrefix, 0) == 0) {
    return SocketIoPacketType::binaryAck;
  }
  return SocketIoPacketType::unknown;
}

std::size_t socketIoPayloadStartOffset(SocketIoPacketType packetType)
{
  if (packetType == SocketIoPacketType::connect || packetType == SocketIoPacketType::event ||
      packetType == SocketIoPacketType::ack || packetType == SocketIoPacketType::binaryEvent ||
      packetType == SocketIoPacketType::binaryAck) {
    return 2;
  }
  return 0;
}

std::string makeSocketIoConnectPacket(const std::string& sid, const std::string& nsp)
{
  return namespacePrefix(kSocketIoPacketConnectPrefix, nsp) + utils::makeSocketIoConnectPayload(sid);
}

std::string makeSocketIoConnectErrorPacket(const std::string& nsp, int code, const std::string& message)
{
  return namespacePrefix(kSocketIoPacketConnectErrorPrefix, nsp) +
         utils::makeSocketIoConnectErrorPayload(code, message);
}

std::string makeSocketIoAckPacket(const std::string& ackId)
{
  return std::string(kSocketIoPacketAckPrefix) + ackId + utils::makeSocketIoAckOkPayload();
}

std::string makeSocketIoAckPacket(
    const std::string& ackId, const std::string& ackJsonArrayPayload, const std::string& nsp)
{
  const std::string prefix = namespacePrefix(kSocketIoPacketAckPrefix, nsp);
  if (ackJsonArrayPayload.empty()) {
    return prefix + ackId + "[]";
  }
  return prefix + ackId + ackJsonArrayPayload;
}

std::string makeSocketIoEventPacket(
    const std::string& eventName, const std::string& jsonObjectPayload, const std::string& nsp)
{
  return namespacePrefix(kSocketIoPacketEventPrefix, nsp) +
         utils::makeSocketIoEventArrayPayload(eventName, jsonObjectPayload);
}

std::string makeSocketIoErrorEventPacket(const std::string& nsp, int code, const std::string& message)
{
  return makeSocketIoEventPacket(kSocketIoServerErrorEventName, utils::makeServerErrorPayload(code, message), nsp);
}

bool hasPingEventName(const std::string& payload)
{
  return payload.find("\"ping\"") != std::string::npos;
}

bool parseSocketIoConnectPacket(const std::string& packet, SocketIoConnectPacket& connectPacketOut)
{
  connectPacketOut = SocketIoConnectPacket{};
  if (parseSocketIoPacketType(packet) != SocketIoPacketType::connect) {
    return false;
  }

  std::size_t pos = socketIoPayloadStartOffset(SocketIoPacketType::connect);
  if (pos < packet.size() && packet[pos] == '/') {
    const std::size_t endPos = packet.find(',', pos);
    if (endPos == std::string::npos) {
      connectPacketOut.nsp = normalizeNamespace(packet.substr(pos));
      return true;
    }
    connectPacketOut.nsp = normalizeNamespace(packet.substr(pos, endPos - pos));
    pos = endPos + 1;
  }

  if (pos < packet.size()) {
    connectPacketOut.authJson = packet.substr(pos);
  }
  return true;
}

bool parseSocketIoEventPacket(const std::string& packet, SocketIoEventPacket& eventPacketOut)
{
  eventPacketOut = SocketIoEventPacket{};

  const SocketIoPacketType packetType = parseSocketIoPacketType(packet);
  if (packetType != SocketIoPacketType::event && packetType != SocketIoPacketType::binaryEvent) {
    return false;
  }

  std::size_t pos = socketIoPayloadStartOffset(packetType);
  if (packetType == SocketIoPacketType::binaryEvent) {
    eventPacketOut.isBinary = true;
    std::string count;
    while (pos < packet.size() && packet[pos] >= '0' && packet[pos] <= '9') {
      count.push_back(packet[pos]);
      ++pos;
    }
    if (count.empty() || pos >= packet.size() || packet[pos] != '-') {
      return false;
    }
    eventPacketOut.attachmentCount = std::atoi(count.c_str());
    ++pos;
  }

  if (pos < packet.size() && packet[pos] == '/') {
    const std::size_t endPos = packet.find(',', pos);
    if (endPos == std::string::npos) {
      return false;
    }
    eventPacketOut.nsp = normalizeNamespace(packet.substr(pos, endPos - pos));
    pos = endPos + 1;
  }

  while (pos < packet.size() && packet[pos] >= '0' && packet[pos] <= '9') {
    eventPacketOut.ackId.push_back(packet[pos]);
    ++pos;
  }
  eventPacketOut.eventPayload = packet.substr(pos);
  return true;
}

std::string parseSocketIoEventName(const std::string& eventPayload)
{
  boost::system::error_code ec;
  const boost::json::value parsed = boost::json::parse(eventPayload, ec);
  if (ec || !parsed.is_array()) {
    return "";
  }

  const boost::json::array& payloadArray = parsed.as_array();
  if (payloadArray.empty() || !payloadArray[0].is_string()) {
    return "";
  }
  return std::string(payloadArray[0].as_string().c_str());
}

std::string parseSocketIoEventData(const std::string& eventPayload)
{
  boost::system::error_code ec;
  const boost::json::value parsed = boost::json::parse(eventPayload, ec);
  if (ec || !parsed.is_array()) {
    return "";
  }

  const boost::json::array& payloadArray = parsed.as_array();
  if (payloadArray.size() < 2) {
    return "";
  }
  return boost::json::serialize(payloadArray[1]);
}

std::string parseSocketIoStringField(const std::string& eventPayload, const std::string& fieldName)
{
  if (fieldName.empty()) {
    return "";
  }

  boost::system::error_code ec;
  const boost::json::value parsed = boost::json::parse(eventPayload, ec);
  if (ec || !parsed.is_object()) {
    return "";
  }

  const boost::json::object& payloadObject = parsed.as_object();
  const auto it = payloadObject.find(fieldName);
  if (it == payloadObject.end() || !it->value().is_string()) {
    return "";
  }

  return std::string(it->value().as_string().c_str());
}

namespace {

void replaceBinaryPlaceholders(boost::json::value& node, const std::vector<std::string>& binaryAttachments)
{
  if (node.is_object()) {
    boost::json::object& obj = node.as_object();
    const auto placeholderIt = obj.find("_placeholder");
    const auto numIt = obj.find("num");
    if (placeholderIt != obj.end() && numIt != obj.end() && placeholderIt->value().is_bool() &&
        placeholderIt->value().as_bool() && numIt->value().is_int64()) {
      const std::int64_t index = numIt->value().as_int64();
      if (index >= 0 && static_cast<std::size_t>(index) < binaryAttachments.size()) {
        node = binaryAttachments[static_cast<std::size_t>(index)];
      } else {
        node = "";
      }
      return;
    }

    for (auto& it : obj) {
      replaceBinaryPlaceholders(it.value(), binaryAttachments);
    }
    return;
  }

  if (node.is_array()) {
    boost::json::array& arr = node.as_array();
    for (auto& value : arr) {
      replaceBinaryPlaceholders(value, binaryAttachments);
    }
  }
}

}  // namespace

std::string mergeSocketIoBinaryEventData(
    const std::string& eventDataJson, const std::vector<std::string>& binaryAttachments)
{
  if (eventDataJson.empty() || binaryAttachments.empty()) {
    return eventDataJson;
  }

  boost::system::error_code ec;
  boost::json::value data = boost::json::parse(eventDataJson, ec);
  if (ec) {
    return eventDataJson;
  }

  replaceBinaryPlaceholders(data, binaryAttachments);
  return boost::json::serialize(data);
}

}  // namespace socketIoServer::protocol
