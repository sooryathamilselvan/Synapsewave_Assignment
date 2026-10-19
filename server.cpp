
#define WIN32_LEAN_AND_MEAN

#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <chrono>
#include <ctime>
#include <unordered_set>

const int TCP_PORT = 5000;

std::atomic<bool> serverRunning{true};

std::mutex logMutex;
std::mutex clientsMutex;

std::vector<SOCKET> clients;

// Temporary user database.
// We will replace this with Redis + SQL later.
const std::unordered_set<std::string> validUsers = {
    "user1",
    "user2",
    "user3"
};

// Thread-safe logging
void logMessage(const std::string& message)
{
    std::lock_guard<std::mutex> lock(logMutex);

    auto now = std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::now()
    );

    std::tm timeInfo{};
    localtime_s(&timeInfo, &now);

    char timeBuffer[20];

    std::strftime(
        timeBuffer,
        sizeof(timeBuffer),
        "%H:%M:%S",
        &timeInfo
    );

    std::cout << "[" << timeBuffer << "] "
              << message << std::endl;
}

// Read one complete line from the TCP stream.
// TCP does not preserve message boundaries, so we buffer bytes.
bool receiveLine(
    SOCKET clientSocket,
    std::string& pendingData,
    std::string& line
)
{
    char buffer[1024];

    while (true)
    {
        size_t newlinePosition = pendingData.find('\n');

        if (newlinePosition != std::string::npos)
        {
            line = pendingData.substr(0, newlinePosition);

            pendingData.erase(0, newlinePosition + 1);

            if (!line.empty() && line.back() == '\r')
                line.pop_back();

            return true;
        }

        if (pendingData.size() > 4096)
            return false;

        int bytesReceived = recv(
            clientSocket,
            buffer,
            sizeof(buffer),
            0
        );

        if (bytesReceived <= 0)
            return false;

        pendingData.append(buffer, bytesReceived);
    }
}

// Send a complete response
bool sendAll(
    SOCKET clientSocket,
    const std::string& message
)
{
    size_t totalSent = 0;

    while (totalSent < message.size())
    {
        int sent = send(
            clientSocket,
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

// Check whether a user ID exists
bool validateUser(const std::string& userId)
{
    return validUsers.find(userId) != validUsers.end();
}

// Remove a client from the shared client list
void removeClient(SOCKET clientSocket)
{
    std::lock_guard<std::mutex> lock(clientsMutex);

    for (auto it = clients.begin(); it != clients.end(); ++it)
    {
        if (*it == clientSocket)
        {
            clients.erase(it);
            break;
        }
    }
}

// Handle one connected client
void handleClient(
    SOCKET clientSocket,
    std::string clientAddress
)
{
    std::string pendingData;
    std::string userId;

    logMessage("Client connected: " + clientAddress);

    // STEP 1: Receive user ID
    if (!receiveLine(clientSocket, pendingData, userId))
    {
        logMessage("Could not receive user ID from "
                   + clientAddress);

        goto cleanup;
    }

    // STEP 2: Validate user ID
    if (!validateUser(userId))
    {
        logMessage("Authentication failed for user ID: "
                   + userId);

        sendAll(clientSocket, "AUTH_FAIL\n");

        goto cleanup;
    }

    // STEP 3: Accept valid user
    logMessage("User validated: " + userId);

    if (!sendAll(clientSocket, "AUTH_OK\n"))
        goto cleanup;

    // STEP 4: Process chat messages only after validation
    while (serverRunning)
    {
        std::string message;

        if (!receiveLine(clientSocket, pendingData, message))
            break;

        if (message.empty())
            continue;

        logMessage(
            "Message from " + userId + ": " + message
        );

        std::string response =
            "Server received from " + userId +
            ": " + message + "\n";

        if (!sendAll(clientSocket, response))
            break;
    }

cleanup:

    removeClient(clientSocket);

    shutdown(clientSocket, SD_BOTH);
    closesocket(clientSocket);

    logMessage("Client disconnected: " + clientAddress);
}

// Main server
int main()
{
    WSADATA wsaData{};

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "Winsock initialization failed.\n";
        return 1;
    }

    SOCKET serverSocket = socket(
        AF_INET,
        SOCK_STREAM,
        IPPROTO_TCP
    );

    if (serverSocket == INVALID_SOCKET)
    {
        std::cerr << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in serverAddress{};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(TCP_PORT);
    serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(
        serverSocket,
        reinterpret_cast<sockaddr*>(&serverAddress),
        sizeof(serverAddress)
    ) == SOCKET_ERROR)
    {
        std::cerr << "Bind failed. Is port 5000 already in use?\n";

        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
    {
        std::cerr << "Listen failed.\n";

        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    logMessage("TCP Chat Server started.");
    logMessage("Listening on port 5000.");
    logMessage("Valid users: user1, user2, user3.");

    while (serverRunning)
    {
        sockaddr_in clientAddress{};
        int addressLength = sizeof(clientAddress);

        SOCKET clientSocket = accept(
            serverSocket,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &addressLength
        );

        if (clientSocket == INVALID_SOCKET)
        {
            if (serverRunning)
                logMessage("Accept failed.");

            break;
        }

        char ipAddress[INET_ADDRSTRLEN]{};

        inet_ntop(
            AF_INET,
            &clientAddress.sin_addr,
            ipAddress,
            sizeof(ipAddress)
        );

        std::string address =
            std::string(ipAddress) + ":" +
            std::to_string(ntohs(clientAddress.sin_port));

        {
            std::lock_guard<std::mutex> lock(clientsMutex);
            clients.push_back(clientSocket);
        }

        std::thread(
            handleClient,
            clientSocket,
            address
        ).detach();
    }

    closesocket(serverSocket);
    WSACleanup();

    return 0;
}
