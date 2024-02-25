package com.example.kafkastream.integrationtest;

import com.example.kafkastream.KStreamConfig;
import com.example.kafkastream.KafkaTopicConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootTest(
        classes = {
                KafkaAutoConfiguration.class,
                KStreamConfig.class,
                KafkaTopicConfig.class,
                KStreamConfigIntegrationTest.KafkaListenerConfig.class,
                KafkaStreamsDefaultConfiguration.class
        },
        properties = {
                "${spring.kafka.producer.key-serializer}=org.apache.kafka.common.serialization.StringSerializer",
                "${spring.kafka.producer.value-serializer}=org.apache.kafka.common.serialization.StringSerializer" //set the integer serializer for the producer
        }
)
@DirtiesContext
@Slf4j
class KStreamConfigIntegrationTest {

    @Value("${kafka.stream.topic-in}")
    String kafkaStreamTopicIn;

    @Value("${kafka.stream.topic-out}")
    String kafkaStreamTopicOut;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CompletableFuture<ConsumerRecord<?, String>> resultFuture;


    @Test
    void testKStream() throws InterruptedException, ExecutionException {
        String key = "key";
        String value = "value";
        kafkaTemplate.send(kafkaStreamTopicIn, key, value);
        kafkaTemplate.flush();
        MatcherAssert.assertThat(resultFuture.get().value(), Matchers.equalTo(value.concat(" out")));
    }

    @Configuration
    public static class KafkaListenerConfig {

        @Bean
        public CompletableFuture<ConsumerRecord<?, String>> resultFuture() {
            return new CompletableFuture<>();
        }

        @KafkaListener(topics = {"${kafka.stream.topic-out}"}, groupId = "kafka-streams")
        public void streamingTopicOut(ConsumerRecord<String, String> payload) {
            resultFuture().complete(payload);
        }
    }

}