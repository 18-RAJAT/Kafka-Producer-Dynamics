package com.example.kafka_producer_example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig {
    @Bean
    public NewTopic createTopic()
    {
        return new NewTopic("kafka-topic-4",7,(short)1);
    }
}
