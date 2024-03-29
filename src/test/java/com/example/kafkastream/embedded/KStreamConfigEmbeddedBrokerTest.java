package com.example.kafkastream.embedded;

import com.example.kafkastream.KStreamConfig;
import com.example.kafkastream.config.EmbeddedKafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@EmbeddedKafka(
        brokerProperties = {
                "auto.create.topics.enable=${topics.autoCreate:true}",
                "delete.topic.enable=${topic.delete:true}"
        }
)
@SpringBootTest(//when define static configuration nested inside the test class, we should import these configuration class below to wire the bean.
        classes = {
                KafkaAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class, KStreamConfig.class,
                KStreamConfigEmbeddedBrokerTest.KafkaListenerConfig.class
        }
)
@Slf4j
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(locations = "classpath:application-kafka-streams.properties")//must use .properties file, application.yml will not work
class KStreamConfigEmbeddedBrokerTest {

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
    @Qualifier("defaultKafkaStreamsBuilder")
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private CompletableFuture<ConsumerRecord<?, String>> resultFuture;

    @Test
    void testEmbeddedKafka(@Autowired EmbeddedKafkaKraftBroker embeddedKafkaKraftBroker) {
        assertNotNull(embeddedKafkaKraftBroker);
        String brokers = embeddedKafkaKraftBroker.getBrokersAsString();
        MatcherAssert.assertThat(kafkaBootstrapServers, Matchers.equalTo(brokers));
        MatcherAssert.assertThat(kafkaStreamBootstrapServers, Matchers.equalTo(brokers));
    }

    @Test
    void testKStream() throws InterruptedException, ExecutionException {
        this.streamsBuilderFactoryBean.stop();
        this.streamsBuilderFactoryBean.start();
        String key = "key";
        String value = "value";
        kafkaTemplate.send(kafkaStreamTopicIn, key, value);
        kafkaTemplate.flush();
        MatcherAssert.assertThat(resultFuture.get().value(), Matchers.equalTo(value.concat(" out")));
    }

    @Configuration
    @EnableKafka
    @EnableKafkaStreams
    @Import(EmbeddedKafkaConfig.class)
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