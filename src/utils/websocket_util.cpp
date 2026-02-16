#include "utils/websocket_util.h"

#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace socketIoServer::utils {

namespace {

constexpr const char* kWebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

std::string trim(const std::string& value)
{
  std::size_t begin = 0;
  while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin]))) {
    ++begin;
  }

  std::size_t end = value.size();
  while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
    --end;
  }

  return value.substr(begin, end - begin);
}

std::string toLowerAscii(std::string value)
{
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

bool recvExact(int fd, void* data, std::size_t size)
{
  std::size_t offset = 0;
  auto* bytes = static_cast<std::uint8_t*>(data);
  while (offset < size) {
    const ssize_t n = ::recv(fd, bytes + offset, size - offset, 0);
    if (n <= 0) {
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

bool sendAll(int fd, const void* data, std::size_t size)
{
  std::size_t offset = 0;
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  while (offset < size) {
    const ssize_t n = ::send(fd, bytes + offset, size - offset, 0);
    if (n <= 0) {
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

std::array<std::uint32_t, 80> buildSha1Schedule(const std::uint8_t* block)
{
  std::array<std::uint32_t, 80> w{};
  for (int i = 0; i < 16; ++i) {
    const int j = i * 4;
    w[i] = (static_cast<std::uint32_t>(block[j]) << 24) |
           (static_cast<std::uint32_t>(block[j + 1]) << 16) |
           (static_cast<std::uint32_t>(block[j + 2]) << 8) |
           static_cast<std::uint32_t>(block[j + 3]);
  }
  for (int i = 16; i < 80; ++i) {
    const std::uint32_t x = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
    w[i] = (x << 1) | (x >> 31);
  }
  return w;
}

std::array<std::uint8_t, 20> sha1(const std::string& input)
{
  std::vector<std::uint8_t> msg(input.begin(), input.end());
  const std::uint64_t bitLength = static_cast<std::uint64_t>(msg.size()) * 8ULL;

  msg.push_back(0x80);
  while ((msg.size() % 64) != 56) {
    msg.push_back(0x00);
  }

  for (int i = 7; i >= 0; --i) {
    msg.push_back(static_cast<std::uint8_t>((bitLength >> (i * 8)) & 0xFF));
  }

  std::uint32_t h0 = 0x67452301;
  std::uint32_t h1 = 0xEFCDAB89;
  std::uint32_t h2 = 0x98BADCFE;
  std::uint32_t h3 = 0x10325476;
  std::uint32_t h4 = 0xC3D2E1F0;

  for (std::size_t offset = 0; offset < msg.size(); offset += 64) {
    const auto w = buildSha1Schedule(msg.data() + offset);
    std::uint32_t a = h0;
    std::uint32_t b = h1;
    std::uint32_t c = h2;
    std::uint32_t d = h3;
    std::uint32_t e = h4;

    for (int i = 0; i < 80; ++i) {
      std::uint32_t f = 0;
      std::uint32_t k = 0;
      if (i < 20) {
        f = (b & c) | ((~b) & d);
        k = 0x5A827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      const std::uint32_t rotA = (a << 5) | (a >> 27);
      const std::uint32_t rotB = (b << 30) | (b >> 2);
      const std::uint32_t temp = rotA + f + e + k + w[static_cast<std::size_t>(i)];
      e = d;
      d = c;
      c = rotB;
      b = a;
      a = temp;
    }

    h0 += a;
    h1 += b;
    h2 += c;
    h3 += d;
    h4 += e;
  }

  std::array<std::uint8_t, 20> out{};
  const std::array<std::uint32_t, 5> h{h0, h1, h2, h3, h4};
  for (std::size_t i = 0; i < h.size(); ++i) {
    out[i * 4] = static_cast<std::uint8_t>((h[i] >> 24) & 0xFF);
    out[i * 4 + 1] = static_cast<std::uint8_t>((h[i] >> 16) & 0xFF);
    out[i * 4 + 2] = static_cast<std::uint8_t>((h[i] >> 8) & 0xFF);
    out[i * 4 + 3] = static_cast<std::uint8_t>(h[i] & 0xFF);
  }
  return out;
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

std::string getHttpHeader(const std::string& request, const std::string& headerName)
{
  const std::string needle = toLowerAscii(headerName) + ":";
  std::size_t start = 0;
  while (start < request.size()) {
    const std::size_t lineEnd = request.find("\r\n", start);
    if (lineEnd == std::string::npos) {
      break;
    }
    const std::string line = request.substr(start, lineEnd - start);
    if (line.empty()) {
      break;
    }
    const std::string lowerLine = toLowerAscii(line);
    if (lowerLine.size() >= needle.size() && lowerLine.compare(0, needle.size(), needle) == 0) {
      return trim(line.substr(needle.size()));
    }
    start = lineEnd + 2;
  }
  return "";
}

std::string computeWebSocketAccept(const std::string& secWebSocketKey)
{
  const std::string source = secWebSocketKey + kWebSocketGuid;
  const auto hash = sha1(source);
  return base64Encode(hash.data(), hash.size());
}

bool sendWebSocketTextFrame(int fd, const std::string& payload)
{
  std::vector<std::uint8_t> frame;
  frame.push_back(0x81);
  const std::size_t n = payload.size();
  if (n <= 125) {
    frame.push_back(static_cast<std::uint8_t>(n));
  } else if (n <= 65535) {
    frame.push_back(126);
    frame.push_back(static_cast<std::uint8_t>((n >> 8) & 0xFF));
    frame.push_back(static_cast<std::uint8_t>(n & 0xFF));
  } else {
    frame.push_back(127);
    for (int i = 7; i >= 0; --i) {
      frame.push_back(static_cast<std::uint8_t>((n >> (i * 8)) & 0xFF));
    }
  }
  frame.insert(frame.end(), payload.begin(), payload.end());
  return sendAll(fd, frame.data(), frame.size());
}

bool readWebSocketTextFrame(int fd, std::string& payloadOut)
{
  payloadOut.clear();
  std::array<std::uint8_t, 2> header{};
  if (!recvExact(fd, header.data(), header.size())) {
    return false;
  }

  const std::uint8_t opcode = static_cast<std::uint8_t>(header[0] & 0x0F);
  const bool masked = (header[1] & 0x80) != 0;
  std::uint64_t payloadLen = static_cast<std::uint8_t>(header[1] & 0x7F);

  if (payloadLen == 126) {
    std::array<std::uint8_t, 2> ext{};
    if (!recvExact(fd, ext.data(), ext.size())) {
      return false;
    }
    payloadLen = (static_cast<std::uint64_t>(ext[0]) << 8) | static_cast<std::uint64_t>(ext[1]);
  } else if (payloadLen == 127) {
    std::array<std::uint8_t, 8> ext{};
    if (!recvExact(fd, ext.data(), ext.size())) {
      return false;
    }
    payloadLen = 0;
    for (std::uint8_t b : ext) {
      payloadLen = (payloadLen << 8) | static_cast<std::uint64_t>(b);
    }
  }

  std::array<std::uint8_t, 4> maskKey{};
  if (masked) {
    if (!recvExact(fd, maskKey.data(), maskKey.size())) {
      return false;
    }
  }

  std::vector<std::uint8_t> payload(static_cast<std::size_t>(payloadLen));
  if (payloadLen > 0 && !recvExact(fd, payload.data(), static_cast<std::size_t>(payloadLen))) {
    return false;
  }

  if (masked) {
    for (std::size_t i = 0; i < payload.size(); ++i) {
      payload[i] ^= maskKey[i % 4];
    }
  }

  if (opcode == 0x8) {
    return false;
  }
  if (opcode != 0x1) {
    return true;
  }

  payloadOut.assign(reinterpret_cast<const char*>(payload.data()), payload.size());
  return true;
}

}  // namespace socketIoServer::utils
