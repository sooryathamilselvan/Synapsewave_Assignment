
#define WIN32_LEAN_AND_MEAN

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <iomanip>
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
#include <sstream>

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
RdKafka::Producer* kafkaProducer = nullptr;
std::mutex kafkaMutex;

const std::string KAFKA_TOPIC = "chat-messages";

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



bool validateUserInDatabase(
    const std::string& userId,
    const std::string& password
)
{
    // Protect the shared MySQL connection.
    std::lock_guard<std::mutex> lock(dbMutex);

    if (dbConnection == nullptr)
    {
        std::cerr << "Database connection is unavailable.\n";
        return false;
    }

    MYSQL_STMT* stmt = mysql_stmt_init(dbConnection);

    if (stmt == nullptr)
    {
        std::cerr << "Could not initialize SQL statement.\n";
        return false;
    }

    // Check that BOTH the user ID and password match.
    const char* query =
        "SELECT UserId FROM tbl_UserDetails "
        "WHERE UserId = ? AND Password = ? LIMIT 1";

    if (mysql_stmt_prepare(
            stmt,
            query,
            static_cast<unsigned long>(std::strlen(query))) != 0)
    {
        std::cerr << "SQL prepare failed: "
                  << mysql_stmt_error(stmt) << '\n';

        mysql_stmt_close(stmt);
        return false;
    }

    // Bind both input values to the prepared statement.
    MYSQL_BIND params[2]{};

    unsigned long userIdLength =
        static_cast<unsigned long>(userId.size());

    unsigned long passwordLength =
        static_cast<unsigned long>(password.size());

    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer =
        const_cast<char*>(userId.c_str());
    params[0].buffer_length = userIdLength;
    params[0].length = &userIdLength;

    params[1].buffer_type = MYSQL_TYPE_STRING;
    params[1].buffer =
        const_cast<char*>(password.c_str());
    params[1].buffer_length = passwordLength;
    params[1].length = &passwordLength;

    if (mysql_stmt_bind_param(stmt, params) != 0 ||
        mysql_stmt_execute(stmt) != 0)
    {
        std::cerr << "SQL query failed: "
                  << mysql_stmt_error(stmt) << '\n';

        mysql_stmt_close(stmt);
        return false;
    }

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
        std::cout << "Credentials validated for: "
                  << userId << '\n';
    else
        std::cout << "Invalid credentials for: "
                  << userId << '\n';

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
                    std::string cachedValue(
                        reply->str, reply->len
                    );

                    freeReplyObject(reply);

                    if (cachedValue == "1")
                        return true;

                    if (cachedValue == "0")
                        return false;
                }
                else
                {
                    freeReplyObject(reply);
                }
            }
        }
    }

    // 2. Redis cache miss: check only whether the user exists.
    bool valid = false;

    {
        std::lock_guard<std::mutex> lock(dbMutex);

        if (dbConnection != nullptr)
        {
            MYSQL_STMT* stmt = mysql_stmt_init(dbConnection);

            if (stmt != nullptr)
            {
                const char* query =
                    "SELECT UserId FROM tbl_UserDetails "
                    "WHERE UserId = ? LIMIT 1";

                if (mysql_stmt_prepare(
                        stmt, query,
                        static_cast<unsigned long>(
                            std::strlen(query))) == 0)
                {
                    MYSQL_BIND param{};

                    unsigned long userIdLength =
                        static_cast<unsigned long>(userId.size());

                    param.buffer_type = MYSQL_TYPE_STRING;
                    param.buffer =
                        const_cast<char*>(userId.c_str());
                    param.buffer_length = userIdLength;
                    param.length = &userIdLength;

                    if (mysql_stmt_bind_param(stmt, &param) == 0 &&
                        mysql_stmt_execute(stmt) == 0 &&
                        mysql_stmt_store_result(stmt) == 0)
                    {
                        valid = mysql_stmt_num_rows(stmt) > 0;
                    }
                }

                mysql_stmt_close(stmt);
            }
        }
    }

    // 3. Cache the user-existence result for 300 seconds.
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


bool validateUserCredentials(
    const std::string& userId,
    const std::string& password
)
{
    // Always verify BOTH credentials against MySQL.
    // Do not let the old Redis user-ID cache bypass login.
    return validateUserInDatabase(userId, password);
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

void broadcastMessage(const std::string& message, SOCKET senderSocket)
{
    std::lock_guard<std::mutex> lock(clientsMutex);

    for (SOCKET clientSocket : clients)
    {
        // Send the message to every client except the sender.
        if (clientSocket == senderSocket)
            continue;

        if (!sendAll(clientSocket, message))
        {
            logMessage("Failed to send message to a client.");
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

std::string escapeJson(const std::string& value)
{
    std::string result;

    for (unsigned char ch : value)
    {
        switch (ch)
        {
        case '"':  result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\b': result += "\\b";  break;
        case '\f': result += "\\f";  break;
        case '\n': result += "\\n";  break;
        case '\r': result += "\\r";  break;
        case '\t': result += "\\t";  break;

        default:
            if (ch < 0x20)
            {
                const char hex[] = "0123456789abcdef";
                result += "\\u00";
                result += hex[ch >> 4];
                result += hex[ch & 0x0f];
            }
            else
            {
                result += static_cast<char>(ch);
            }
        }
    }

    return result;
}

std::string currentTimestamp()
{
    std::time_t now = std::time(nullptr);
    std::tm timeInfo{};

#ifdef _WIN32
    localtime_s(&timeInfo, &now);
#else
    localtime_r(&now, &timeInfo);
#endif

    std::ostringstream output;
    output << std::put_time(&timeInfo, "%Y-%m-%d %H:%M:%S");

    return output.str();
}

bool publishChatMessage(
    const std::string& userId,
    const std::string& message)
{
    std::lock_guard<std::mutex> lock(kafkaMutex);

    if (kafkaProducer == nullptr)
    {
        logMessage("Kafka unavailable; message was not published.");
        return false;
    }

    const std::string payload =
        "{\"userId\":\"" + escapeJson(userId) +
        "\",\"timestamp\":\"" + currentTimestamp() +
        "\",\"message\":\"" + escapeJson(message) + "\"}";

    
RdKafka::ErrorCode result = kafkaProducer->produce(
    KAFKA_TOPIC,
    RdKafka::Topic::PARTITION_UA,
    RdKafka::Producer::RK_MSG_COPY,
    const_cast<char*>(payload.data()),
    payload.size(),
    userId.data(),
    userId.size(),
    0,
    nullptr
);


    if (result != RdKafka::ERR_NO_ERROR)
    {
        logMessage(
            "Kafka publish enqueue failed: " +
            RdKafka::err2str(result)
        );

        return false;
    }

    // Process available Kafka events and delivery reports.
    kafkaProducer->poll(0);

    logMessage("Chat message queued for Kafka: " + userId);
    return true;
}




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
    std::string password;

    if (!receiveLine(clientSocket, pendingData, userId))
        goto cleanup;

    // STEP 2: Receive the password.
    if (!receiveLine(clientSocket, pendingData, password))
        goto cleanup;

    // STEP 3: Validate both credentials against MySQL.
    if (!validateUserCredentials(userId, password))
    {
        logMessage(
            "Authentication failed for user ID: " + userId
        );

        sendAll(clientSocket, "AUTH_FAIL\n");
        goto cleanup;
    }

    // STEP 4: Reserve this User ID for one active TCP session.
    {
        std::lock_guard<std::mutex> lock(userSocketsMutex);

        if (userSockets.find(userId) != userSockets.end())
        {
            logMessage(
                "Duplicate login rejected for: " + userId
            );

            sendAll(clientSocket, "AUTH_FAIL\n");
            goto cleanup;
        }

        userSockets[userId] = clientSocket;
    }

    // STEP 5: Register the authenticated user for heartbeat tracking.
    {
        std::lock_guard<std::mutex> lock(heartbeatMutex);

        authenticatedUsers.insert(userId);

        lastHeartbeat[userId] =
            std::chrono::steady_clock::now();

        timedOutUsers.erase(userId);

        registered = true;
    }

    logMessage("User validated: " + userId);

    // STEP 6: Confirm successful authentication.
    if (!sendAll(clientSocket, "AUTH_OK\n"))
        goto cleanup;

    logMessage(
        "Waiting for UDP heartbeats from: " + userId
    );

    // STEP 7: Process incoming chat messages.
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

        // PRIVATE MESSAGE
        // Format: /msg <userID> <message>
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

            // Check whether the recipient is a valid user.
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

        // PUBLIC MESSAGE
        logMessage(
            "Message from " + userId + ": " + message
        );

        publishChatMessage(userId, message);

        std::string broadcast =
            "[" + userId + "]: " + message + "\n";

        broadcastMessage(broadcast, clientSocket);

        logMessage(
            "Broadcast message from " + userId
        );
    }

cleanup:

    // STEP 8: Remove the socket mapping only if this
    // connection still owns the User ID.
    bool ownsSession = false;

    {
        std::lock_guard<std::mutex> lock(userSocketsMutex);

        auto it = userSockets.find(userId);

        if (it != userSockets.end() &&
            it->second == clientSocket)
        {
            userSockets.erase(it);
            ownsSession = true;
        }
    }

    // STEP 9: Clear heartbeat state only for this
    // authenticated, owning session.
    if (registered && ownsSession)
    {
        std::lock_guard<std::mutex> lock(heartbeatMutex);

        authenticatedUsers.erase(userId);
        lastHeartbeat.erase(userId);
        timedOutUsers.erase(userId);
    }

    // STEP 10: Remove the client and close the socket.
    removeClient(clientSocket);

    shutdown(clientSocket, SD_BOTH);
    closesocket(clientSocket);

    logMessage(
        "Client disconnected: " + clientAddress
    );
}


bool connectToKafka()
{
    std::string error;

    RdKafka::Conf* config =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if (config == nullptr)
    {
        std::cerr << "Could not create Kafka configuration.\n";
        return false;
    }

    if (config->set(
            "metadata.broker.list",
            "127.0.0.1:9092",
            error) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "Kafka configuration failed: "
                  << error << '\n';

        delete config;
        return false;
    }

    kafkaProducer = RdKafka::Producer::create(config, error);

    // The Producer takes ownership of the configuration.
    if (kafkaProducer == nullptr)
    {
        std::cerr << "Kafka producer creation failed: "
                  << error << '\n';

        delete config;
        return false;
    }

    std::cout << "Kafka producer initialized successfully.\n";
    return true;
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

    if (!connectToKafka())
{
    logMessage("Kafka producer unavailable.");
}

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

    // Server shutdown.
    serverRunning = false;

    // 1. Stop accepting new TCP connections.
    shutdown(serverSocket, SD_BOTH);
    closesocket(serverSocket);

    // 2. Interrupt all connected clients so their threads can exit.
    {
        std::lock_guard<std::mutex> lock(clientsMutex);

        for (SOCKET clientSocket : clients)
        {
            shutdown(clientSocket, SD_BOTH);
        }
    }

    // 3. Stop the UDP heartbeat receiver.
    closesocket(udpSocket);

    // 4. Wait for the heartbeat threads to finish.
    if (receiverThread.joinable())
        receiverThread.join();

    if (monitorThread.joinable())
        monitorThread.join();

    // 5. Wait for every client-handling thread to finish.
    for (std::thread& clientThread : clientThreads)
    {
        if (clientThread.joinable())
            clientThread.join();
    }

    // 6. Release Winsock resources.
    WSACleanup();

    logMessage("Server shutdown completed.");

    return 0;
}

