#include "utils/json_util.h"

#include <boost/json.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>

namespace socketIoServer::utils {

namespace {

std::int8_t base64Value(unsigned char c)
{
  if (c >= 'A' && c <= 'Z') {
    return static_cast<std::int8_t>(c - 'A');
  }
  if (c >= 'a' && c <= 'z') {
    return static_cast<std::int8_t>(26 + (c - 'a'));
  }
  if (c >= '0' && c <= '9') {
    return static_cast<std::int8_t>(52 + (c - '0'));
  }
  if (c == '+') {
    return 62;
  }
  if (c == '/') {
    return 63;
  }
  return -1;
}

std::string base64Encode(const std::uint8_t* data, std::size_t size)
{
  static constexpr char kTable[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((size + 2) / 3) * 4);

  for (std::size_t i = 0; i < size; i += 3) {
    const std::uint32_t b0 = data[i];
    const std::uint32_t b1 = (i + 1 < size) ? data[i + 1] : 0;
    const std::uint32_t b2 = (i + 2 < size) ? data[i + 2] : 0;
    const std::uint32_t triple = (b0 << 16) | (b1 << 8) | b2;

    out.push_back(kTable[(triple >> 18) & 0x3F]);
    out.push_back(kTable[(triple >> 12) & 0x3F]);
    out.push_back((i + 1 < size) ? kTable[(triple >> 6) & 0x3F] : '=');
    out.push_back((i + 2 < size) ? kTable[triple & 0x3F] : '=');
  }

  return out;
}

}  // namespace

std::string makeEngineIoOpenPayload(
    const std::string& sid, const std::string& privateId, std::uint32_t pingIntervalMs, std::uint32_t pingTimeoutMs,
    std::uint32_t maxPayload)
{
  boost::json::object payload;
  payload["sid"] = sid;
  if (!privateId.empty()) {
    payload["pid"] = privateId;
  }

  boost::json::array upgrades;
  upgrades.emplace_back("websocket");
  payload["upgrades"] = upgrades;
  payload["pingInterval"] = pingIntervalMs;
  payload["pingTimeout"] = pingTimeoutMs;
  payload["maxPayload"] = maxPayload;
  return boost::json::serialize(payload);
}

std::string makeSocketIoConnectPayload(const std::string& sid)
{
  boost::json::object payload;
  payload["sid"] = sid;
  return boost::json::serialize(payload);
}

std::string makeSocketIoConnectErrorPayload(int code, const std::string& message)
{
  boost::json::object payload;
  payload["message"] = message;
  payload["code"] = code;
  return boost::json::serialize(payload);
}

std::string makeSocketIoAckOkPayload()
{
  boost::json::object ack;
  ack["ok"] = true;
  boost::json::array payload;
  payload.emplace_back(ack);
  return boost::json::serialize(payload);
}

std::string makeSocketIoEventArrayPayload(const std::string& eventName, const std::string& jsonObjectPayload)
{
  boost::json::value eventData = boost::json::object();
  boost::system::error_code ec;
  if (!jsonObjectPayload.empty()) {
    boost::json::value parsed = boost::json::parse(jsonObjectPayload, ec);
    if (!ec) {
      eventData = std::move(parsed);
    }
  }

  boost::json::array payload;
  payload.emplace_back(eventName);
  payload.emplace_back(std::move(eventData));
  return boost::json::serialize(payload);
}

std::string makeServerErrorPayload(int code, const std::string& message)
{
  boost::json::object payload;
  payload["code"] = code;
  payload["message"] = message;
  return boost::json::serialize(payload);
}

std::string makeRoomMessagePayload(
    const std::string& roomField, const std::string& room, const std::string& fromSid,
    const std::string& messageField, const std::string& message)
{
  boost::json::object payload;
  payload[roomField] = room;
  payload["from"] = fromSid;
  payload[messageField] = message;
  return boost::json::serialize(payload);
}

std::string encodeBase64(const std::string& binaryData)
{
  return base64Encode(
      reinterpret_cast<const std::uint8_t*>(binaryData.data()), static_cast<std::size_t>(binaryData.size()));
}

bool getDecodedBase64Size(const std::string& encoded, std::size_t& decodedSizeOut)
{
  decodedSizeOut = 0;
  if (encoded.empty() || (encoded.size() % 4) != 0) {
    return false;
  }

  std::size_t paddingCount = 0;
  if (!encoded.empty() && encoded[encoded.size() - 1] == '=') {
    paddingCount = 1;
    if (encoded.size() > 1 && encoded[encoded.size() - 2] == '=') {
      paddingCount = 2;
    }
  }

  for (std::size_t i = 0; i < encoded.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(encoded[i]);
    if (c == '=') {
      if (i < encoded.size() - paddingCount) {
        return false;
      }
      continue;
    }
    if (base64Value(c) < 0) {
      return false;
    }
  }

  decodedSizeOut = (encoded.size() / 4) * 3 - paddingCount;
  return true;
}

}  // namespace socketIoServer::utils
