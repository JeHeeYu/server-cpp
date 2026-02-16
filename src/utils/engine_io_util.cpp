#include "utils/engine_io_util.h"

namespace socketIoServer::utils {

std::vector<std::string> splitEngineIoPayload(const std::string& payload)
{
  std::vector<std::string> packets;
  std::size_t start = 0;
  while (start <= payload.size()) {
    std::size_t sep = payload.find('\x1e', start);
    if (sep == std::string::npos) {
      sep = payload.size();
    }
    packets.push_back(payload.substr(start, sep - start));
    if (sep == payload.size()) {
      break;
    }
    start = sep + 1;
  }
  return packets;
}

std::string joinEngineIoPayload(const std::deque<std::string>& packets)
{
  std::string out;
  for (std::size_t i = 0; i < packets.size(); ++i) {
    if (i > 0) {
      out.push_back('\x1e');
    }
    out += packets[i];
  }
  return out;
}

}  // namespace socketIoServer::utils
