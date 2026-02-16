#pragma once

#include <cstdint>
#include <string>

namespace socketIoServer::utils {

std::string makeEngineIoOpenPayload(
    const std::string& sid, std::uint32_t pingIntervalMs, std::uint32_t pingTimeoutMs, std::uint32_t maxPayload);
std::string makeSocketIoConnectPayload(const std::string& sid);
std::string makeSocketIoConnectErrorPayload(int code, const std::string& message);
std::string makeSocketIoAckOkPayload();
std::string makeSocketIoEventArrayPayload(const std::string& eventName, const std::string& jsonObjectPayload);
std::string makeServerErrorPayload(int code, const std::string& message);
std::string makeRoomMessagePayload(
    const std::string& roomField, const std::string& room, const std::string& fromSid,
    const std::string& messageField, const std::string& message);
std::string encodeBase64(const std::string& binaryData);
bool getDecodedBase64Size(const std::string& encoded, std::size_t& decodedSizeOut);

}  // namespace socketIoServer::utils
