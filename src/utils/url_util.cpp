#include "utils/url_util.h"

namespace socketIoServer::utils {

void splitTarget(const std::string& target, std::string& path, std::string& query)
{
  const std::size_t queryPos = target.find('?');
  if (queryPos == std::string::npos) {
    path = target;
    query.clear();
    return;
  }
  path = target.substr(0, queryPos);
  query = target.substr(queryPos + 1);
}

std::unordered_map<std::string, std::string> parseQuery(const std::string& query)
{
  std::unordered_map<std::string, std::string> out;
  std::size_t start = 0;
  while (start < query.size()) {
    std::size_t amp = query.find('&', start);
    if (amp == std::string::npos) {
      amp = query.size();
    }
    const std::string pair = query.substr(start, amp - start);
    const std::size_t eq = pair.find('=');
    if (eq == std::string::npos) {
      out[pair] = "";
    } else {
      out[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
    start = amp + 1;
  }
  return out;
}

}  // namespace socketIoServer::utils
