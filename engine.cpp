#include <iostream>
#include <string>
#include <chrono>
#include <algorithm>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>

using json = nlohmann::json;

// A high-speed native C++ mock for VADER sentiment analysis
double analyze_sentiment(const std::string& text) {
    std::string lower_text = text;
    std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(), ::tolower);

    if (lower_text.find("surge") != std::string::npos || lower_text.find("optimistic") != std::string::npos || lower_text.find("record") != std::string::npos)
        return 0.85;
    if (lower_text.find("plummets") != std::string::npos || lower_text.find("investigation") != std::string::npos)
        return -0.75;
    if (lower_text.find("bearish") != std::string::npos || lower_text.find("misses") != std::string::npos)
        return -0.50;
    
    return 0.0;
}

void persist_to_questdb(double ts, const std::string& text, double score) {
    try {
        // Using libpqxx to connect to QuestDB over PostgreSQL wire protocol
        pqxx::connection c("postgresql://admin:quest@127.0.0.1:8812/qdb");
        pqxx::work w(c);
        int64_t ts_micro = static_cast<int64_t>(ts * 1000000);
        w.exec_params("INSERT INTO sentiment_signals (ts, text, compound_score) VALUES ($1, $2, $3)", ts_micro, text, score);
        w.commit();
    } catch (const std::exception &e) {
        std::cerr << "🚨 Error writing to QuestDB: " << e.what() << std::endl;
    }
}

int main() {
    std::string brokers = "localhost:9092";
    std::string errstr;

    // Set up Kafka Consumer (replaces Faust app)
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", "engine_cpp_group", errstr);
    conf->set("auto.offset.reset", "latest", errstr);

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) { std::cerr << "Failed to create consumer: " << errstr << std::endl; return 1; }
    consumer->subscribe({"raw_text_stream"});

    // Set up Kafka Producer (for outgoing sentiment)
    RdKafka::Conf *p_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    p_conf->set("bootstrap.servers", brokers, errstr);
    RdKafka::Producer *producer = RdKafka::Producer::create(p_conf, errstr);

    std::cout << "✅ VibeFactor Engine (C++) starting..." << std::endl;

    while (true) {
        RdKafka::Message *msg = consumer->consume(1000); // 1 second timeout
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            std::string payload(static_cast<const char *>(msg->payload()), msg->len());
            try {
                json input = json::parse(payload);
                std::string text = input["text"];
                double ts = input["timestamp"];

                // 1. Perform Sentiment Analysis
                double compound_score = analyze_sentiment(text);

                // 2. Create Enriched Record
                json output = {
                    {"timestamp", ts},
                    {"text", text},
                    {"sentiment", {{"compound", compound_score}}}
                };

                // 3. Send to Output Topic
                std::string out_payload = output.dump();
                producer->produce("sentiment_stream", RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                                  const_cast<char *>(out_payload.c_str()), out_payload.size(), NULL, 0, 0, NULL, NULL);

                std::cout << "Processed: " << text.substr(0, 40) << "... -> " << compound_score << std::endl;
                
                // 4. Persist to Database
                persist_to_questdb(ts, text, compound_score);

            } catch (const std::exception& e) {
                std::cerr << "Error parsing/processing message: " << e.what() << std::endl;
            }
        }
        delete msg; // Avoid memory leaks
        producer->poll(0);
    }
    
    delete consumer;
    delete producer;
    return 0;
}