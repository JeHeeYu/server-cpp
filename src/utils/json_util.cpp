#include "utils/json_util.h"

#include <boost/json.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>

namespace socketIoServer::utils {

namespace {

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
    const std::string& sid, std::uint32_t pingIntervalMs, std::uint32_t pingTimeoutMs, std::uint32_t maxPayload)
{
  boost::json::object payload;
  payload["sid"] = sid;

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

std::string encodeBase64(const std::string& binaryData)
{
  return base64Encode(
      reinterpret_cast<const std::uint8_t*>(binaryData.data()), static_cast<std::size_t>(binaryData.size()));
}

}  // namespace socketIoServer::utils
