#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <cstdio>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

const std::vector<std::string> SAMPLE_MESSAGES = {
    "BIGCORP INC reports record profits for Q3, shares surge.",
    "REGULATOR announces investigation into SMALLCO's accounting practices.",
    "Market sentiment is bearish on tech stocks following interest rate hikes.",
    "NEWTECH launches revolutionary product, analysts are optimistic.",
    "OLDGUARD CORP misses earnings estimates, stock price plummets."
};

int main() {
    std::string brokers = "localhost:9092";
    std::string topic = "raw_text_stream";
    std::string errstr;

    // Configure Kafka Producer
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, SAMPLE_MESSAGES.size() - 1);
    std::uniform_real_distribution<> sleep_time(1.0, 4.0);

    std::cout << "✅ Starting to stream data to topic '" << topic << "'..." << std::endl;

    while (true) {
        auto now = std::chrono::system_clock::now();
        double timestamp = std::chrono::duration<double>(now.time_since_epoch()).count();

        const std::string text = SAMPLE_MESSAGES[dis(gen)];

        json j;
        j["timestamp"] = timestamp;
        j["text"] = text;
        std::string payload = j.dump();

        RdKafka::ErrorCode resp = producer->produce(
            topic, RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(payload.c_str()), payload.size(),
            NULL, 0, 0, NULL, NULL);

        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Produce failed: " << RdKafka::err2str(resp) << std::endl;
        } else {
            std::cout << "Sent: " << text << std::endl;
        }

        producer->poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(sleep_time(gen) * 1000)));
    }

    delete producer;
    delete conf;
    return 0;
}