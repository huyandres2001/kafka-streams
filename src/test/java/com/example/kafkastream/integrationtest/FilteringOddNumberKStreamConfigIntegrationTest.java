package com.example.kafkastream.integrationtest;


import com.example.kafkastream.FilteringOddNumberKStreamConfig;
import com.example.kafkastream.KafkaTopicConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootTest(
        classes = {
                KafkaAutoConfiguration.class,
                FilteringOddNumberKStreamConfig.class,
                KafkaTopicConfig.class,
                FilteringOddNumberKStreamConfigIntegrationTest.KafkaListenerConfig.class
        },
        properties = {
                "${spring.kafka.producer.key-serializer}=org.apache.kafka.common.serialization.StringSerializer",
                "${spring.kafka.producer.value-serializer}=org.apache.kafka.common.serialization.IntegerSerializer",
                "${spring.kafka.consumer.value-deserializer}=org.apache.kafka.common.serialization.IntegerDeserializer",
                "${spring.kafka.consumer.key-deserializer}=org.apache.kafka.common.serialization.StringDeserializer"

                //set the integer serializer for the producer
        }
)
@Slf4j
@DirtiesContext
class FilteringOddNumberKStreamConfigIntegrationTest {

    @Value("${kafka.stream.filtering-topic-in}")
    String filteringTopicIn;

    @Value("${kafka.stream.filtering-topic-out}")
    String filteringTopicOut;

    @Autowired
    KafkaTemplate<String, Integer> kafkaTemplate;

    @Autowired
    ArrayList<ConsumerRecord<String, Integer>> output;

    @Autowired
    CompletableFuture<ConsumerRecord<String, Integer>> completableFuture;

    @Test
    void testFilterOddNumberKStream() throws InterruptedException, ExecutionException {
        List<Integer> integers = List.of(-3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        integers.forEach(integer -> kafkaTemplate.send(filteringTopicIn, integer));
        kafkaTemplate.flush();
        while (output.size() < 7) {
            output.add(completableFuture.get());
        }
        Assertions.assertThat(output).isNotEmpty()
                .allSatisfy(consumerRecord -> Assertions.assertThat(consumerRecord.value()).is(new Condition<>(integer -> integer % 2 == 1, "Is odd number")));
    }


    @Configuration
    public static class KafkaListenerConfig {

        @Bean
        public ArrayList<ConsumerRecord<String, Integer>> blockingQueue() {
            return new ArrayList<>();
        }

        @Bean
        public CompletableFuture<ConsumerRecord<String, Integer>> completableFuture() {
            return new CompletableFuture<>();
        }

        @KafkaListener(
                topics = {"${kafka.stream.filtering-topic-out}"},
                groupId = "kafka-streams"
        )
        public void filterTopicOut(ConsumerRecord<String, Integer> payload) {
            completableFuture().complete(payload);
        }

    }
}
