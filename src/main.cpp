#include <iostream>
#include <cstdlib>
#include <string>

#include "protocol/types.h"
#include "server.h"

int main()
{
  socketIoServer::ServerConfig config;
  config.host = "0.0.0.0";
  config.port = 3100;
  if (const char* portEnv = std::getenv("PORT")) {
    const int parsedPort = std::atoi(portEnv);
    if (parsedPort > 0 && parsedPort <= 65535) {
      config.port = static_cast<std::uint16_t>(parsedPort);
    }
  }

  socketIoServer::Server server(config);
  server.setEventHandler(
      [](socketIoServer::Server& serverRef, const std::string& sid, const std::string& nsp,
          const std::string& eventName, const std::string& eventData,
          const socketIoServer::Server::AckCallback& ack) {
        if (eventName == "ping") {
          ack("[{\"ok\":true}]");
          return;
        }

        if (eventName == "join_room") {
          const std::string room = socketIoServer::protocol::parseSocketIoStringField(eventData, "room");
          if (!room.empty()) {
            serverRef.joinRoom(sid, nsp, room);
          }
          ack("[{\"ok\":true}]");
          return;
        }

        if (eventName == "room_ping") {
          const std::string room = socketIoServer::protocol::parseSocketIoStringField(eventData, "room");
          const std::string message = socketIoServer::protocol::parseSocketIoStringField(eventData, "message");
          if (!room.empty()) {
            const std::string body =
                "{\"room\":\"" + room + "\",\"from\":\"" + sid + "\",\"message\":\"" + message + "\"}";
            serverRef.emitToRoomEvent(nsp, room, "room_message", body);
          }
          ack("[{\"ok\":true}]");
        }
      });

  if (!server.start()) {
    std::cerr << "Failed to start socketIoServerCpp" << std::endl;
    return 1;
  }

  std::cout << "socketIoServerCpp listening on " << config.host << ":" << config.port
            << std::endl;
  std::cout << "Press Enter to stop..." << std::endl;
  std::string line;
  std::getline(std::cin, line);
  server.stop();
  return 0;
}
