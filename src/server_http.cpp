#include "server.h"
#include "protocol.h"
#include "utils/engine_io_util.h"
#include "utils/http_util.h"
#include "utils/url_util.h"

#include <chrono>

namespace socketIoServer {

namespace {

constexpr const char* kSocketIoPath = "/socket.io/";

}

void Server::processEngineIoPacket(const std::string& sid, const std::string& packet)
{
  if (packet.empty()) {
    return;
  }

  touchSession(sid);

  const protocol::SocketIoPacketType packetType = protocol::parseSocketIoPacketType(packet);
  const protocol::EngineIoControlPacket controlType = protocol::parseEngineIoControlPacket(packet);
  if (controlType == protocol::EngineIoControlPacket::close) {
    removeSession(sid);
    return;
  }
  if (controlType == protocol::EngineIoControlPacket::ping) {
    enqueuePacket(sid, protocol::toWirePacket(protocol::EngineIoControlPacket::pong));
    return;
  }

  if (packetType == protocol::SocketIoPacketType::connect) {
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      auto it = sessions.find(sid);
      if (it != sessions.end()) {
        it->second.namespaceConnected = true;
      }
    }
    enqueuePacket(sid, protocol::makeSocketIoConnectPacket(sid));
    return;
  }

  if (packetType == protocol::SocketIoPacketType::disconnect) {
    removeSession(sid);
    return;
  }

  if (packetType == protocol::SocketIoPacketType::event) {
    dispatchSocketIoEvent(sid, packet, [this, sid](const std::string& packetToSend) {
      enqueuePacket(sid, packetToSend);
    });
    return;
  }
}

bool Server::handleHttpRequest(const std::string& request, std::string& response)
{
  const std::string method = utils::parseRequestMethod(request);
  const std::string target = utils::parseRequestTarget(request);
  std::string path;
  std::string queryString;
  utils::splitTarget(target, path, queryString);

  if (path == "/") {
    response = utils::makeHttpResponse("200 OK", "socketIoServerCpp alive\n");
    return true;
  }

  if (path != kSocketIoPath && path != "/socket.io") {
    return false;
  }

  const auto query = utils::parseQuery(queryString);
  const auto eioIt = query.find("EIO");
  const auto transportIt = query.find("transport");
  if (eioIt == query.end() || transportIt == query.end()) {
    response = utils::makeHttpResponse("400 Bad Request", "missing query");
    return true;
  }

  if (!protocol::isEngineIoVersion4(eioIt->second) || !protocol::isPollingTransport(transportIt->second)) {
    response = utils::makeHttpResponse("400 Bad Request", "unsupported transport");
    return true;
  }

  if (method == "GET") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      const std::string sid = createSession();
      SessionState session;
      session.sid = sid;
      session.lastSeenAt = std::chrono::steady_clock::now();
      {
        std::lock_guard<std::mutex> lock(sessionsMutex);
        sessions.emplace(sid, std::move(session));
      }
      const std::string openPacket = protocol::makeEngineIoOpenPacket(sid);
      response = utils::makeHttpResponse("200 OK", openPacket);
      return true;
    }

    std::deque<std::string> pendingPackets;
    {
      std::lock_guard<std::mutex> lock(sessionsMutex);
      const auto it = sessions.find(sidIt->second);
      if (it == sessions.end()) {
        response = utils::makeHttpResponse("400 Bad Request", "unknown sid");
        return true;
      }
      it->second.lastSeenAt = std::chrono::steady_clock::now();
      pendingPackets.swap(it->second.outgoingPackets);
    }

    if (pendingPackets.empty()) {
      response = utils::makeHttpResponse("200 OK", "6");
      return true;
    }

    response = utils::makeHttpResponse("200 OK", utils::joinEngineIoPayload(pendingPackets));
    return true;
  }

  if (method == "POST") {
    const auto sidIt = query.find("sid");
    if (sidIt == query.end()) {
      response = utils::makeHttpResponse("400 Bad Request", "missing sid");
      return true;
    }

    const std::string sid = sidIt->second;
    if (!hasSession(sid)) {
      response = utils::makeHttpResponse("400 Bad Request", "unknown sid");
      return true;
    }
    touchSession(sid);

    const std::string body = utils::parseRequestBody(request);
    const auto packets = utils::splitEngineIoPayload(body);
    for (const std::string& packet : packets) {
      processEngineIoPacket(sid, packet);
    }

    response = utils::makeHttpResponse("200 OK", "ok");
    return true;
  }

  response = utils::makeHttpResponse("405 Method Not Allowed", "method not allowed");
  return true;
}

}  // namespace socketIoServer
