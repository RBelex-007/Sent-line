#include <iostream>
#include <string>
#include <iomanip>
#include <librdkafka/rdkafkacpp.h>
#include <chrono>
#include <ctime>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main() {
    std::string errstr;
    
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "localhost:9092", errstr);
    conf->set("group.id", "monitor_cpp_group", errstr);
    conf->set("auto.offset.reset", "latest", errstr);

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) { std::cerr << "Failed to create consumer: " << errstr << std::endl; return 1; }
    consumer->subscribe({"sentiment_stream"});

    std::cout << "⏳ Connecting to Kafka stream for monitoring..." << std::endl;
    std::cout << "✅ Connected. Waiting for sentiment signals...\n" << std::endl;
    
    // Table Header
    std::cout << std::left << std::setw(25) << "Timestamp" << std::setw(80) << "Text" << "Score" << std::endl;
    std::cout << std::string(115, '-') << std::endl;

    while (true) {
        RdKafka::Message *msg = consumer->consume(1000);
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            std::string payload(static_cast<const char *>(msg->payload()), msg->len());
            try {
                json data = json::parse(payload);
                double ts = data["timestamp"];
                std::string text = data["text"];
                double score = data["sentiment"]["compound"];

                // Format Timestamp
                std::time_t t = static_cast<std::time_t>(ts);
                char buf[30];
                std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));

                // Terminal ANSI Color Codes
                std::string color = "\033[0m"; // Default Reset
                if (score > 0.05) color = "\033[1;32m"; // Bold Green
                else if (score < -0.05) color = "\033[1;31m"; // Bold Red
                else color = "\033[1;33m"; // Yellow

                std::cout << std::left << std::setw(25) << buf
                          << std::setw(80) << text.substr(0, 75)
                          << color << std::showpos << std::fixed << std::setprecision(4) << score << "\033[0m" << std::noshowpos
                          << std::endl;

            } catch (const std::exception& e) {
                std::cerr << "Parse error: " << e.what() << std::endl;
            }
        }
        delete msg;
    }
    return 0;
}