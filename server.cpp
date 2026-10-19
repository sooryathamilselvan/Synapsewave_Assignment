
#define WIN32_LEAN_AND_MEAN

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <vector>
#include <mysql/mysql.h>
#include <hiredis/hiredis.h>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <chrono>
#include <ctime>
#include <unordered_set>
#include <unordered_map>

const int TCP_PORT = 5000;
const int UDP_PORT = 5001;
const int HEARTBEAT_TIMEOUT = 15;

std::atomic<bool> serverRunning{true};
MYSQL* dbConnection = nullptr;
std::mutex dbMutex;
std::mutex logMutex;
std::mutex clientsMutex;
std::mutex heartbeatMutex;
std::mutex userSocketsMutex;
std::mutex clientThreadsMutex;
std::vector<std::thread> clientThreads;
std::vector<SOCKET> clients;
redisContext* redisConnection = nullptr;
std::mutex redisMutex;

const int REDIS_CACHE_TTL = 300;


// Maps authenticated user IDs to their TCP sockets.
std::unordered_map<std::string, SOCKET> userSockets;

// Users currently authenticated over TCP.
std::unordered_set<std::string> authenticatedUsers;

// Last heartbeat received for each authenticated user.
std::unordered_map<
    std::string,
    std::chrono::steady_clock::time_point
> lastHeartbeat;

// Users whose heartbeat has timed out.
std::unordered_set<std::string> timedOutUsers;


// --------------------------------------------------
// THREAD-SAFE SERVER LOGGING
// --------------------------------------------------

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


// --------------------------------------------------
// RECEIVE ONE NEWLINE-DELIMITED MESSAGE
// --------------------------------------------------

bool receiveLine(
    SOCKET sock,
    std::string& pending,
    std::string& line
)
{
    char buffer[1024];

    while (serverRunning)
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

        if (pending.size() > 4096)
            return false;

        int received = recv(
            sock,
            buffer,
            sizeof(buffer),
            0
        );

        if (received <= 0)
            return false;

        pending.append(buffer, received);
    }

    return false;
}


// --------------------------------------------------
// SEND THE COMPLETE MESSAGE
// --------------------------------------------------

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

bool connectToDatabase()
{
    const char* username = std::getenv("CHATDB_USER");
    const char* password = std::getenv("CHATDB_PASSWORD");

    if (username == nullptr || password == nullptr)
    {
        std::cerr << "Missing CHATDB_USER or CHATDB_PASSWORD.\n";
        return false;
    }

    dbConnection = mysql_init(nullptr);

    if (dbConnection == nullptr)
    {
        std::cerr << "MySQL initialization failed.\n";
        return false;
    }

    if (mysql_real_connect(
            dbConnection,
            "127.0.0.1",
            username,
            password,
            "ChatDB",
            3306,
            nullptr,
            0) == nullptr)
    {
        std::cerr << "MySQL connection failed: "
                  << mysql_error(dbConnection) << '\n';

        mysql_close(dbConnection);
        dbConnection = nullptr;
        return false;
    }

    std::cout << "Connected to ChatDB successfully.\n";
    return true;
}


bool connectToRedis()
{
    // Connect to Redis running in WSL.
    // WSL2 commonly forwards Windows localhost to its Linux services.
    redisConnection = redisConnect("127.0.0.1", 6379);

    if (redisConnection == nullptr)
    {
        std::cerr << "Could not allocate Redis connection.\n";
        return false;
    }

    if (redisConnection->err)
    {
        std::cerr << "Redis connection failed: "
                  << redisConnection->errstr << '\n';

        redisFree(redisConnection);
        redisConnection = nullptr;
        return false;
    }

    std::cout << "Connected to Redis successfully.\n";
    return true;
}


// --------------------------------------------------
// VALIDATE USER ID
// --------------------------------------------------


bool validateUserInDatabase(const std::string& userId)
{
    // Protect the shared MySQL connection.
    std::lock_guard<std::mutex> lock(dbMutex);

    if (dbConnection == nullptr)
    {
        std::cerr << "Database connection is unavailable.\n";
        return false;
    }

    // Create a prepared SQL statement.
    MYSQL_STMT* stmt = mysql_stmt_init(dbConnection);

    if (stmt == nullptr)
    {
        std::cerr << "Could not initialize SQL statement.\n";
        return false;
    }

    const char* query =
        "SELECT UserId FROM tbl_UserDetails "
        "WHERE UserId = ? LIMIT 1";

    if (mysql_stmt_prepare(
            stmt, query,
            static_cast<unsigned long>(std::strlen(query))) != 0)
    {
        std::cerr << "SQL prepare failed: "
                  << mysql_stmt_error(stmt) << '\n';

        mysql_stmt_close(stmt);
        return false;
    }

    // Bind the incoming User ID to the ? placeholder.
    MYSQL_BIND param{};
    unsigned long userIdLength =
        static_cast<unsigned long>(userId.size());

    param.buffer_type = MYSQL_TYPE_STRING;
    param.buffer = const_cast<char*>(userId.c_str());
    param.buffer_length = userIdLength;
    param.length = &userIdLength;

    if (mysql_stmt_bind_param(stmt, &param) != 0 ||
        mysql_stmt_execute(stmt) != 0)
    {
        std::cerr << "SQL query failed: "
                  << mysql_stmt_error(stmt) << '\n';

        mysql_stmt_close(stmt);
        return false;
    }

    // Check whether MySQL found the requested user.
    bool valid = false;

    if (mysql_stmt_store_result(stmt) == 0)
    {
        valid = mysql_stmt_num_rows(stmt) > 0;
    }
    else
    {
        std::cerr << "SQL result failed: "
                  << mysql_stmt_error(stmt) << '\n';
    }

    mysql_stmt_close(stmt);

    if (valid)
        std::cout << "User validated: " << userId << '\n';
    else
        std::cout << "User rejected: " << userId << '\n';

    return valid;
}


bool validateUser(const std::string& userId)
{
    const std::string key = "chat:user-valid:" + userId;

    // 1. Try Redis first.
    {
        std::lock_guard<std::mutex> lock(redisMutex);

        if (redisConnection != nullptr &&
            redisConnection->err == 0)
        {
            redisReply* reply = static_cast<redisReply*>(
                redisCommand(
                    redisConnection,
                    "GET %s",
                    key.c_str()
                )
            );

            if (reply != nullptr)
            {
                if (reply->type == REDIS_REPLY_STRING)
                {
                    std::string cachedValue(reply->str, reply->len);
                    freeReplyObject(reply);

                    if (cachedValue == "1")
                        return true;

                    if (cachedValue == "0")
                        return false;
                }
                else
                {
                    // A nil response means the key is not cached.
                    freeReplyObject(reply);
                }
            }
        }
    }

    // 2. Cache miss or Redis unavailable: query MySQL.
    const bool valid = validateUserInDatabase(userId);

    // 3. Cache the result for 300 seconds when Redis is available.
    {
        std::lock_guard<std::mutex> lock(redisMutex);

        if (redisConnection != nullptr &&
            redisConnection->err == 0)
        {
            const char* value = valid ? "1" : "0";

            redisReply* reply = static_cast<redisReply*>(
                redisCommand(
                    redisConnection,
                    "SET %s %s EX %d",
                    key.c_str(),
                    value,
                    REDIS_CACHE_TTL
                )
            );

            if (reply != nullptr)
                freeReplyObject(reply);
        }
    }

    return valid;
}


void removeClient(SOCKET sock)
{
    std::lock_guard<std::mutex> lock(clientsMutex);

    for (auto it = clients.begin(); it != clients.end(); ++it)
    {
        if (*it == sock)
        {
            clients.erase(it);
            break;
        }
    }
}


// --------------------------------------------------
// STEP 3A: RECEIVE UDP HEARTBEATS
// --------------------------------------------------

void heartbeatReceiver(SOCKET udpSocket)
{
    char buffer[512];

    while (serverRunning)
    {
        sockaddr_in senderAddress{};
        int senderLength = sizeof(senderAddress);

        int received = recvfrom(
            udpSocket,
            buffer,
            sizeof(buffer) - 1,
            0,
            reinterpret_cast<sockaddr*>(&senderAddress),
            &senderLength
        );

        if (received == SOCKET_ERROR)
        {
            int error = WSAGetLastError();

            if (error == WSAETIMEDOUT ||
                error == WSAEWOULDBLOCK)
            {
                continue;
            }

            if (serverRunning)
            {
                logMessage(
                    "UDP recvfrom failed: " +
                    std::to_string(error)
                );
            }

            break;
        }

        buffer[received] = '\0';
        std::string packet(buffer);

        const std::string prefix = "HEARTBEAT:";

        if (packet.rfind(prefix, 0) != 0)
        {
            logMessage("Ignored invalid UDP packet.");
            continue;
        }

        std::string userId = packet.substr(prefix.size());

        std::lock_guard<std::mutex> lock(heartbeatMutex);

        // Only authenticated users can update heartbeat state.
        if (authenticatedUsers.find(userId) ==
            authenticatedUsers.end())
        {
            logMessage(
                "Ignored heartbeat from unauthenticated user: " +
                userId
            );

            continue;
        }

        lastHeartbeat[userId] =
            std::chrono::steady_clock::now();

        if (timedOutUsers.erase(userId) > 0)
        {
            logMessage(
                "Heartbeat restored for user: " + userId
            );
        }

        logMessage("Heartbeat received from: " + userId);
    }
}


// --------------------------------------------------
// STEP 3B: MONITOR HEARTBEAT TIMEOUTS
// --------------------------------------------------

void heartbeatMonitor()
{
    while (serverRunning)
    {
        std::this_thread::sleep_for(
            std::chrono::seconds(1)
        );

        auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lock(heartbeatMutex);

        for (const auto& entry : lastHeartbeat)
        {
            const std::string& userId = entry.first;

            auto elapsed =
                std::chrono::duration_cast<std::chrono::seconds>(
                    now - entry.second
                ).count();

            if (elapsed >= HEARTBEAT_TIMEOUT &&
                timedOutUsers.insert(userId).second)
            {
                logMessage(
                    "HEARTBEAT TIMEOUT: " + userId +
                    " has been inactive for " +
                    std::to_string(elapsed) +
                    " seconds."
                );
            }
        }
    }
}


// --------------------------------------------------
// PUBLIC MESSAGE BROADCAST
// --------------------------------------------------

void broadcastMessage(
    const std::string& message,
    SOCKET senderSocket
)
{
    std::lock_guard<std::mutex> lock(clientsMutex);

    for (SOCKET clientSocket : clients)
    {
        // Do not send the public message back to its sender.
        if (clientSocket == senderSocket)
            continue;

        if (!sendAll(clientSocket, message))
        {
            logMessage(
                "Failed to broadcast a message to a client."
            );
        }
    }
}


// --------------------------------------------------
// PRIVATE MESSAGE
// Command: /msg user2 Hello
// --------------------------------------------------

void sendPrivateMessage(
    const std::string& sender,
    const std::string& recipient,
    const std::string& message
)
{
    SOCKET recipientSocket = INVALID_SOCKET;

    // Look up the recipient's socket safely.
    {
        std::lock_guard<std::mutex> lock(userSocketsMutex);

        auto it = userSockets.find(recipient);

        if (it != userSockets.end())
            recipientSocket = it->second;
    }

    if (recipientSocket == INVALID_SOCKET)
    {
        logMessage(
            "Private message failed: " + recipient +
            " is not connected."
        );

        return;
    }

    std::string formattedMessage =
        "[Private from " + sender + "]: " +
        message + "\n";

    if (!sendAll(recipientSocket, formattedMessage))
    {
        logMessage(
            "Failed to deliver private message to " +
            recipient
        );
    }
    else
    {
        logMessage(
            "Private message sent from " + sender +
            " to " + recipient
        );
    }
}


// --------------------------------------------------
// HANDLE EACH TCP CLIENT
// --------------------------------------------------

void handleClient(
    SOCKET clientSocket,
    std::string clientAddress
)
{
    std::string pendingData;
    std::string userId;

    bool registered = false;

    logMessage("Client connected: " + clientAddress);

    // STEP 1: Receive the user ID.
    if (!receiveLine(clientSocket, pendingData, userId))
        goto cleanup;

    // STEP 2: Validate the user ID.
    if (!validateUser(userId))
    {
        logMessage(
            "Authentication failed for user ID: " + userId
        );

        sendAll(clientSocket, "AUTH_FAIL\n");

        goto cleanup;
    }

    // STEP 3: Register the authenticated user for heartbeat tracking.
    {
        std::lock_guard<std::mutex> lock(heartbeatMutex);

        authenticatedUsers.insert(userId);

        lastHeartbeat[userId] =
            std::chrono::steady_clock::now();

        timedOutUsers.erase(userId);

        registered = true;
    }

    logMessage("User validated: " + userId);

    // Inform the client that authentication succeeded.
    if (!sendAll(clientSocket, "AUTH_OK\n"))
        goto cleanup;

    // STEP 4: Register the user's TCP socket for private messages.
    {
        std::lock_guard<std::mutex> lock(userSocketsMutex);

        userSockets[userId] = clientSocket;
    }

    logMessage(
        "Waiting for UDP heartbeats from: " + userId
    );

    // STEP 5: Process chat messages.
    while (serverRunning)
    {
        std::string message;

        if (!receiveLine(
                clientSocket,
                pendingData,
                message))
        {
            break;
        }

        if (message.empty())
            continue;

        // Disconnect command.
        if (message == "/quit")
            break;

        // ------------------------------------------
        // PRIVATE MESSAGE COMMAND
        // Format: /msg <userID> <message>
        // ------------------------------------------

        if (message.rfind("/msg ", 0) == 0)
        {
            size_t recipientStart = 5;

            size_t separator = message.find(
                ' ',
                recipientStart
            );

            if (separator == std::string::npos)
            {
                sendAll(
                    clientSocket,
                    "Usage: /msg <userID> <message>\n"
                );

                continue;
            }

            std::string recipient = message.substr(
                recipientStart,
                separator - recipientStart
            );

            std::string privateText =
                message.substr(separator + 1);

            if (recipient.empty() || privateText.empty())
            {
                sendAll(
                    clientSocket,
                    "Usage: /msg <userID> <message>\n"
                );

                continue;
            }

            if (recipient == userId)
            {
                sendAll(
                    clientSocket,
                    "You cannot send a private message to yourself.\n"
                );

                continue;
            }

            // Only send private messages to valid user IDs.
            if (!validateUser(recipient))
            {
                sendAll(
                    clientSocket,
                    "Unknown recipient user ID.\n"
                );

                continue;
            }

            sendPrivateMessage(
                userId,
                recipient,
                privateText
            );

            continue;
        }

        // ------------------------------------------
        // PUBLIC MESSAGE
        // ------------------------------------------

        logMessage(
            "Message from " + userId + ": " + message
        );

        std::string broadcast =
            "[" + userId + "]: " + message + "\n";

        broadcastMessage(broadcast, clientSocket);

        logMessage(
            "Broadcast message from " + userId
        );
    }

cleanup:

    // STEP 6: Remove the user from heartbeat tracking.
    if (registered)
    {
        {
            std::lock_guard<std::mutex> lock(heartbeatMutex);

            authenticatedUsers.erase(userId);
            lastHeartbeat.erase(userId);
            timedOutUsers.erase(userId);
        }

        // STEP 7: Remove the user's private-message socket mapping.
        {
            std::lock_guard<std::mutex> lock(userSocketsMutex);

            auto it = userSockets.find(userId);

            // Do not erase a mapping that belongs to another socket.
            if (it != userSockets.end() &&
                it->second == clientSocket)
            {
                userSockets.erase(it);
            }
        }
    }

    // STEP 8: Remove the client and close its socket.
    removeClient(clientSocket);

    shutdown(clientSocket, SD_BOTH);
    closesocket(clientSocket);

    logMessage("Client disconnected: " + clientAddress);
}


// --------------------------------------------------
// MAIN SERVER
// --------------------------------------------------

int main()
{
    WSADATA wsaData{};

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "Winsock initialization failed.\n";
        return 1;
    }

    // Create the TCP server socket.
    SOCKET serverSocket = socket(
        AF_INET,
        SOCK_STREAM,
        IPPROTO_TCP
    );

    if (serverSocket == INVALID_SOCKET)
    {
        std::cerr << "TCP socket creation failed.\n";

        WSACleanup();

        return 1;
    }

    sockaddr_in tcpAddress{};

    tcpAddress.sin_family = AF_INET;
    tcpAddress.sin_port = htons(TCP_PORT);
    tcpAddress.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(
            serverSocket,
            reinterpret_cast<sockaddr*>(&tcpAddress),
            sizeof(tcpAddress)) == SOCKET_ERROR)
    {
        std::cerr << "TCP bind failed.\n";

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
    {
        std::cerr << "TCP listen failed.\n";

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    // Create the UDP heartbeat socket.
    SOCKET udpSocket = socket(
        AF_INET,
        SOCK_DGRAM,
        IPPROTO_UDP
    );

    if (udpSocket == INVALID_SOCKET)
    {
        std::cerr << "UDP socket creation failed.\n";

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    sockaddr_in udpAddress{};

    udpAddress.sin_family = AF_INET;
    udpAddress.sin_port = htons(UDP_PORT);
    udpAddress.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(
            udpSocket,
            reinterpret_cast<sockaddr*>(&udpAddress),
            sizeof(udpAddress)) == SOCKET_ERROR)
    {
        std::cerr << "UDP bind failed. Is port 5001 in use?\n";

        closesocket(udpSocket);
        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    // Let UDP receive time out periodically so shutdown can be checked.
    DWORD timeoutMs = 1000;

    setsockopt(
        udpSocket,
        SOL_SOCKET,
        SO_RCVTIMEO,
        reinterpret_cast<const char*>(&timeoutMs),
        sizeof(timeoutMs)
    );

        // Connect to the MySQL database before starting the server.
    if (!connectToDatabase())
    {
        logMessage("Database connection failed. Server shutting down.");

        closesocket(udpSocket);
        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }
    if (!connectToRedis())
{
    logMessage("Redis unavailable. Continuing with MySQL fallback.");
}

    logMessage("TCP Chat Server started on port 5000.");
    logMessage("UDP Heartbeat Server started on port 5001.");
    logMessage("Heartbeat timeout: 15 seconds.");

    std::thread receiverThread(
        heartbeatReceiver,
        udpSocket
    );

    std::thread monitorThread(
        heartbeatMonitor
    );

    // Accept incoming TCP clients.
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
                logMessage("TCP accept failed.");

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

        {
    std::lock_guard<std::mutex> lock(clientThreadsMutex);

    clientThreads.emplace_back(
        handleClient,
        clientSocket,
        address
    );
}
    }

    // Server shutdown.
    serverRunning = false;

    shutdown(serverSocket, SD_BOTH);
    closesocket(serverSocket);

    // Wake the UDP receiver if it is blocked.
    closesocket(udpSocket);

    receiverThread.join();
    monitorThread.join();

    WSACleanup();

    return 0;
}
