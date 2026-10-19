
#define WIN32_LEAN_AND_MEAN

#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>


const int TCP_PORT = 5000;
const int UDP_PORT = 5001;

std::atomic<bool> clientRunning{true};
std::mutex consoleMutex;

// Read one complete line from the TCP stream.
bool receiveLine(
    SOCKET sock,
    std::string& pending,
    std::string& line
)
{
    char buffer[1024];

    while (clientRunning)
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

    return false;
}

bool sendAll(SOCKET sock, const std::string& message)
{
    size_t totalSent = 0;

    while (totalSent < message.size())
    {
        int sent = send(
            sock,
            message.data() + totalSent,
            static_cast<int>(message.size() - totalSent),
            0
        );

        if (sent <= 0)
            return false;

        totalSent += static_cast<size_t>(sent);
    }

    return true;
}

// Receive broadcasts continuously in a separate thread.
void receiveMessages(SOCKET tcpSocket)
{
    std::string pending;
    std::string message;

    while (clientRunning)
    {
        if (!receiveLine(tcpSocket, pending, message))
            break;

        std::lock_guard<std::mutex> lock(consoleMutex);

        std::cout << "\n" << message << std::endl;
    }

    clientRunning = false;
}

// Send a UDP heartbeat every five seconds.
void heartbeatSender(
    SOCKET udpSocket,
    sockaddr_in udpAddress,
    std::string userId
)
{
    std::string heartbeat = "HEARTBEAT:" + userId;

    while (clientRunning)
    {
        int sent = sendto(
            udpSocket,
            heartbeat.c_str(),
            static_cast<int>(heartbeat.size()),
            0,
            reinterpret_cast<sockaddr*>(&udpAddress),
            sizeof(udpAddress)
        );

        if (sent == SOCKET_ERROR && clientRunning)
        {
            std::lock_guard<std::mutex> lock(consoleMutex);
            std::cerr << "Heartbeat send failed: "
                      << WSAGetLastError() << '\n';
        }

        for (int i = 0; i < 50 && clientRunning; ++i)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        }
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

    SOCKET tcpSocket = socket(
        AF_INET, SOCK_STREAM, IPPROTO_TCP
    );

    if (tcpSocket == INVALID_SOCKET)
    {
        std::cerr << "TCP socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in tcpAddress{};
    tcpAddress.sin_family = AF_INET;
    tcpAddress.sin_port = htons(TCP_PORT);

    inet_pton(AF_INET, "127.0.0.1", &tcpAddress.sin_addr);

    if (connect(
        tcpSocket,
        reinterpret_cast<sockaddr*>(&tcpAddress),
        sizeof(tcpAddress)
    ) == SOCKET_ERROR)
    {
        std::cerr << "TCP connection failed.\n";
        closesocket(tcpSocket);
        WSACleanup();
        return 1;
    }

    std::cout << "Connected to TCP chat server.\n";

    // Authenticate before starting background threads.
    std::string userId;

    std::cout << "Enter user ID (user1/user2/..../user100): ";
    std::getline(std::cin, userId);

    if (userId.empty() || !sendAll(tcpSocket, userId + "\n"))
    {
        std::cerr << "Failed to send user ID.\n";
        closesocket(tcpSocket);
        WSACleanup();
        return 1;
    }

    std::string pending;
    std::string response;

    if (!receiveLine(tcpSocket, pending, response))
    {
        std::cerr << "Server disconnected.\n";
        closesocket(tcpSocket);
        WSACleanup();
        return 1;
    }

    if (response != "AUTH_OK")
    {
        std::cout << "Access denied: invalid user ID.\n";
        closesocket(tcpSocket);
        WSACleanup();
        return 0;
    }

    std::cout << "Authentication successful!\n";
    std::cout << "Type /quit to disconnect.\n";

    SOCKET udpSocket = socket(
        AF_INET, SOCK_DGRAM, IPPROTO_UDP
    );

    if (udpSocket == INVALID_SOCKET)
    {
        std::cerr << "UDP socket creation failed.\n";
        closesocket(tcpSocket);
        WSACleanup();
        return 1;
    }

    sockaddr_in udpAddress{};
    udpAddress.sin_family = AF_INET;
    udpAddress.sin_port = htons(UDP_PORT);

    inet_pton(AF_INET, "127.0.0.1", &udpAddress.sin_addr);

    // Start continuous message receiving and heartbeats.
    std::thread receiverThread(receiveMessages, tcpSocket);

    std::thread heartbeatThread(
        heartbeatSender, udpSocket, udpAddress, userId
    );

    // Main thread handles keyboard input.
    std::string message;

    while (clientRunning)
    {
        {
            std::lock_guard<std::mutex> lock(consoleMutex);
            std::cout << "\nYou: " << std::flush;
        }

        if (!std::getline(std::cin, message))
            break;

        if (message == "/quit")
        {
            sendAll(tcpSocket, "/quit\n");
            break;
        }

        if (message.empty())
            continue;

        if (!sendAll(
            tcpSocket,
            message + "\n"
        ))
        {
            std::cerr << "Failed to send message.\n";
            break;
        }
    }

    clientRunning = false;

    // Closing the TCP socket wakes the blocked recv() call.
    shutdown(tcpSocket, SD_BOTH);

    receiverThread.join();
    heartbeatThread.join();

    closesocket(tcpSocket);
    closesocket(udpSocket);

    WSACleanup();

    std::cout << "Client disconnected.\n";
    return 0;
}
