#include "utils/json_util.h"

#include <boost/json.hpp>
#include <boost/system/error_code.hpp>

namespace socketIoServer::utils {

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

}  // namespace socketIoServer::utils
