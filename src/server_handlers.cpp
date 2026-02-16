#include "server_handlers.h"

#include <string>

#include "server.h"
#include "utils/json_util.h"

namespace socketIoServerHandlers {

void registerDefaultHandlers(socketIoServer::Server& server)
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
      [](socketIoServer::Server&, const std::string&, const std::string&, const std::string&, const std::string&,
          const socketIoServer::Server::AckCallback& ack) {
        ack(socketIoServer::utils::makeSocketIoAckOkPayload());
      });
}

}  // namespace socketIoServerHandlers
