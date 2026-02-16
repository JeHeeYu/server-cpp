#include "protocol/types.h"

namespace socketIoServer::protocol {

std::string normalizeNamespace(const std::string& nsp)
{
  if (nsp.empty()) {
    return "/";
  }
  if (nsp[0] != '/') {
    return "/" + nsp;
  }
  return nsp;
}

std::string parseSocketIoNamespace(const std::string& packet)
{
  const SocketIoPacketType packetType = parseSocketIoPacketType(packet);
  if (packetType == SocketIoPacketType::unknown) {
    return "/";
  }

  std::size_t pos = socketIoPayloadStartOffset(packetType);
  if (packetType == SocketIoPacketType::binaryEvent || packetType == SocketIoPacketType::binaryAck) {
    const std::size_t dashPos = packet.find('-', pos);
    if (dashPos == std::string::npos) {
      return "/";
    }
    pos = dashPos + 1;
  }

  if (pos >= packet.size() || packet[pos] != '/') {
    return "/";
  }

  const std::size_t endPos = packet.find(',', pos);
  if (endPos == std::string::npos) {
    return normalizeNamespace(packet.substr(pos));
  }
  return normalizeNamespace(packet.substr(pos, endPos - pos));
}

}  // namespace socketIoServer::protocol
