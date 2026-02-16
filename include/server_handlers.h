#pragma once

#include <string>

namespace socketIoServer {
class Server;
}

namespace socketIoServerHandlers {

struct HandlerOptions {
  std::string pingEvent = "ping";
  std::string joinRoomEvent = "join_room";
  std::string leaveRoomEvent = "leave_room";
  std::string roomMessageEvent = "room_message";
  std::string roomField = "room";
  std::string messageField = "message";
};

void registerDefaultHandlers(socketIoServer::Server& server);
void registerDefaultHandlers(socketIoServer::Server& server, const HandlerOptions& options);

}  // namespace socketIoServerHandlers
