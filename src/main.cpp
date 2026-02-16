#include <iostream>
#include <cstdlib>
#include <string>

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
