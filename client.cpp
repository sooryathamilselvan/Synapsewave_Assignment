
#define WIN32_LEAN_AND_MEAN

#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <string>

bool receiveLine(SOCKET sock, std::string& pending, std::string& line)
{
    char buffer[1024];

    while (true)
    {
        size_t pos = pending.find('\n');

        if (pos != std::string::npos)
        {
            line = pending.substr(0, pos);
            pending.erase(0, pos + 1);

            if (!line.empty() && line.back() == '\r')
                line.pop_back();

            return true;
        }

        int received = recv(sock, buffer, sizeof(buffer), 0);

        if (received <= 0)
            return false;

        pending.append(buffer, received);
    }
}

int main()
{
    WSADATA wsaData{};

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "Winsock initialization failed.\n";
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (sock == INVALID_SOCKET)
    {
        std::cerr << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_port = htons(5000);

    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    if (connect(
        sock,
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address)
    ) == SOCKET_ERROR)
    {
        std::cerr << "Connection failed.\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    std::string pending;

    // Send the user ID first
    std::string userId;

    std::cout << "Enter user ID (user1/user2/user3): ";
    std::getline(std::cin, userId);

    userId += "\n";

    if (send(sock, userId.c_str(),
             static_cast<int>(userId.size()), 0) == SOCKET_ERROR)
    {
        std::cerr << "Failed to send user ID.\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    // Wait for server validation
    std::string response;

    if (!receiveLine(sock, pending, response))
    {
        std::cerr << "Server disconnected.\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    if (response != "AUTH_OK")
    {
        std::cout << "Access denied: invalid user ID.\n";

        closesocket(sock);
        WSACleanup();
        return 0;
    }

    std::cout << "User validated successfully!\n";

    // Send a chat message
    std::string message;

    std::cout << "Enter message: ";
    std::getline(std::cin, message);

    message += "\n";

    if (send(sock, message.c_str(),
             static_cast<int>(message.size()), 0) != SOCKET_ERROR)
    {
        if (receiveLine(sock, pending, response))
            std::cout << response << '\n';
    }

    shutdown(sock, SD_BOTH);
    closesocket(sock);
    WSACleanup();

    return 0;
}
