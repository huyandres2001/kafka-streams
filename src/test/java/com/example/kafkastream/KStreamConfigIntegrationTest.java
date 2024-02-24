package com.example.kafkastream;

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
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Objects;
import java.util.concurrent.*;

@SpringBootTest(
        classes = {
                KafkaAutoConfiguration.class, KafkaTemplateConfig.class, KafkaStreamsDefaultConfiguration.class, KStreamConfig.class,
                KStreamConfigIntegrationTest.KafkaListenerConfig.class
        }
)
@DirtiesContext
@Slf4j
class KStreamConfigIntegrationTest {

    @Value("${kafka.stream.topic-in}")
    String kafkaStreamTopicIn;

    @Value("${kafka.stream.topic-out}")
    String kafkaStreamTopicOut;

    @Value("${spring.kafka.streams.bootstrap-servers}")
    String kafkaStreamBootstrapServers;

    @Value("${spring.kafka.bootstrap-servers}")
    String kafkaBootstrapServers;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private CompletableFuture<ConsumerRecord<?, String>> resultFuture;

    static final BlockingQueue<ConsumerRecord<String, String>> output = new LinkedBlockingQueue<>();


    @Test
    void testKStream() throws InterruptedException, ExecutionException {
        String key = "key";
        String value = "value";
        kafkaTemplate.send(kafkaStreamTopicIn, key, value);
        kafkaTemplate.flush();
        MatcherAssert.assertThat(resultFuture.get().value(), Matchers.equalTo(value.concat(" out")));
        MatcherAssert.assertThat(Objects.requireNonNull(output.poll(2, TimeUnit.MINUTES)).value(), Matchers.equalTo(value.concat(" out")));
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
            output.add(payload);
        }
    }

}