
#include <iostream>
#include <cstdlib>
#include <string>
#include <memory>

#include <mysql/mysql.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main()
{
    // Read MySQL credentials from environment variables.
    const char* dbUser = std::getenv("CHATDB_USER");
    const char* dbPassword = std::getenv("CHATDB_PASSWORD");

    if (!dbUser || !dbPassword)
    {
        std::cerr << "Set CHATDB_USER and CHATDB_PASSWORD first.\n";
        return 1;
    }

    // Connect to MySQL.
    MYSQL* db = mysql_init(nullptr);

    if (!db || !mysql_real_connect(
        db, "127.0.0.1", dbUser, dbPassword,
        "ChatDB", 3306, nullptr, 0))
    {
        std::cerr << "MySQL connection failed: "
                  << (db ? mysql_error(db) : "mysql_init failed")
                  << '\n';

        if (db)
            mysql_close(db);

        return 1;
    }

    std::cout << "Connected to MySQL.\n";

    // Configure Kafka consumer.
    std::string error;

    std::unique_ptr<RdKafka::Conf> config(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
    );

    if (!config ||
        config->set("bootstrap.servers", "127.0.0.1:9092", error)
            != RdKafka::Conf::CONF_OK ||
        config->set("group.id", "chat-db-writer", error)
            != RdKafka::Conf::CONF_OK ||
        config->set("auto.offset.reset", "earliest", error)
            != RdKafka::Conf::CONF_OK ||
        config->set("enable.auto.commit", "false", error)
            != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "Kafka configuration error: "
                  << error << '\n';
        mysql_close(db);
        return 1;
    }

    RdKafka::KafkaConsumer* consumer =
        RdKafka::KafkaConsumer::create(config.get(), error);

    if (!consumer)
    {
        std::cerr << "Kafka consumer creation failed: "
                  << error << '\n';
        mysql_close(db);
        return 1;
    }

    if (consumer->subscribe({"chat-messages"})
        != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Could not subscribe to chat-messages.\n";
        consumer->close();
        delete consumer;
        mysql_close(db);
        return 1;
    }

    std::cout << "Listening for Kafka chat messages...\n";

    // Consume messages and insert them into MySQL.
    while (true)
    {
        RdKafka::Message* message = consumer->consume(1000);

        if (message->err() == RdKafka::ERR__TIMED_OUT)
        {
            delete message;
            continue;
        }

        if (message->err() != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "Kafka read error: "
                      << message->errstr() << '\n';
            delete message;
            continue;
        }

        bool saved = false;

        try
        {
            std::string payload(
                static_cast<const char*>(message->payload()),
                message->len()
            );

            json data = json::parse(payload);

            std::string userId =
                data.at("userId").get<std::string>();

            std::string chatText =
                data.at("message").get<std::string>();

            // Escape SQL string values.
            std::string safeUser(userId.size() * 2 + 1, '\0');
            std::string safeText(chatText.size() * 2 + 1, '\0');

            unsigned long userLength = mysql_real_escape_string(
                db, safeUser.data(), userId.c_str(),
                static_cast<unsigned long>(userId.size())
            );

            unsigned long textLength = mysql_real_escape_string(
                db, safeText.data(), chatText.c_str(),
                static_cast<unsigned long>(chatText.size())
            );

            safeUser.resize(userLength);
            safeText.resize(textLength);

            std::string query =
                "INSERT INTO tbl_ChatDetails (UserId, ChatMessage) "
                "VALUES ('" + safeUser + "', '" + safeText + "')";

            if (mysql_query(db, query.c_str()) == 0)
            {
                saved = true;
                std::cout << "Saved message from " << userId
                          << ": " << chatText << '\n';
            }
            else
            {
                std::cerr << "MySQL insert failed: "
                          << mysql_error(db) << '\n';
            }
        }
        catch (const std::exception& ex)
        {
            std::cerr << "Invalid chat JSON: "
                      << ex.what() << '\n';
        }

        // Commit only after a successful database insert.
        if (saved)
        {
            RdKafka::ErrorCode result =
                consumer->commitSync(message);

            if (result != RdKafka::ERR_NO_ERROR)
            {
                std::cerr << "Kafka offset commit failed: "
                          << RdKafka::err2str(result) << '\n';
            }
        }

        delete message;
    }

    // The loop currently runs continuously.
    consumer->close();
    delete consumer;
    mysql_close(db);

    return 0;
}
