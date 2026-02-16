#include "server_handlers.h"

#include <string>

#include "protocol/types.h"
#include "server.h"
#include "utils/json_util.h"

namespace socketIoServerHandlers {

void registerDefaultHandlers(socketIoServer::Server& server, const HandlerOptions& options)
{
  server.setEventGuardHandler(
      [](socketIoServer::Server&, const std::string&, const std::string&, const std::string&,
          const std::string&) {
        return socketIoServer::Server::ConnectDecision{};
      });

  server.setNamespaceConnectHandler(
      [](socketIoServer::Server&, const std::string&, const std::string&, const std::string&) {
        return socketIoServer::Server::ConnectDecision{};
      });

  server.setEventHandler(
      [options](socketIoServer::Server& server, const std::string& sid, const std::string& nsp,
          const std::string& eventName, const std::string& eventData,
          const socketIoServer::Server::AckCallback& ack) {
        if (eventName == options.pingEvent) {
          ack(socketIoServer::utils::makeSocketIoAckOkPayload());
          return;
        }

        if (eventName == options.joinRoomEvent) {
          const std::string room = socketIoServer::protocol::parseSocketIoStringField(eventData, options.roomField);
          if (!room.empty()) {
            server.joinRoom(sid, nsp, room);
          }
          ack(socketIoServer::utils::makeSocketIoAckOkPayload());
          return;
        }

        if (eventName == options.leaveRoomEvent) {
          const std::string room = socketIoServer::protocol::parseSocketIoStringField(eventData, options.roomField);
          if (!room.empty()) {
            server.leaveRoom(sid, nsp, room);
          }
          ack(socketIoServer::utils::makeSocketIoAckOkPayload());
          return;
        }

        if (eventName == options.roomMessageEvent) {
          const std::string room = socketIoServer::protocol::parseSocketIoStringField(eventData, options.roomField);
          const std::string message =
              socketIoServer::protocol::parseSocketIoStringField(eventData, options.messageField);

          if (!room.empty()) {
            const std::string body = socketIoServer::utils::makeRoomMessagePayload(
                options.roomField, room, sid, options.messageField, message);
            server.emitToRoomEvent(nsp, room, options.roomMessageEvent, body, sid);
          }

          ack(socketIoServer::utils::makeSocketIoAckOkPayload());
          return;
        }

        ack(socketIoServer::utils::makeSocketIoAckOkPayload());
      });
}

void registerDefaultHandlers(socketIoServer::Server& server)
{
  registerDefaultHandlers(server, HandlerOptions{});
}

}  // namespace socketIoServerHandlers
