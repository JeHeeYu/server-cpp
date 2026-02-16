#pragma once

namespace socketIoServer {
class Server;
}

namespace socketIoServerHandlers {

void registerDefaultHandlers(socketIoServer::Server& server);

}  // namespace socketIoServerHandlers
