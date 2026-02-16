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
std::string encodeBase64(const std::string& binaryData);

}  // namespace socketIoServer::utils
