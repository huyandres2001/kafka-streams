package com.example.kafkastream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.stream.topic-in}")
    private String kStreamTopicIn;

    @Value("${kafka.stream.topic-out}")
    private String kStreamTopicOut;

    @Value("${kafka.stream.filtering-topic-in}")
    private String filteringTopicIn;

    @Value("${kafka.stream.filtering-topic-out}")
    private String filteringTopicOut;

    @Bean
    public KafkaAdmin.NewTopics topics() {

        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(kStreamTopicIn).partitions(1).build(),
                TopicBuilder.name(kStreamTopicOut).build(),
                TopicBuilder.name(filteringTopicIn).build(),
                TopicBuilder.name(filteringTopicOut).build()
        );
    }
}
