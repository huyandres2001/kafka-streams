package com.example.kafkastream.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class EmbeddedKafkaConfig {

    @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    private String brokerAddresses;

    @Bean
    public ProducerFactory<?, ?> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> producerConfigs = KafkaTestUtils.producerProps(this.brokerAddresses);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class); //must set key-value serializer
        return producerConfigs;
    }

    @Bean
    public KafkaTemplate<?, ?> template(ConsumerFactory<Object, Object> consumerFactory, ProducerFactory<Object, Object> producerFactory) {
        KafkaTemplate<Object, Object> template = new KafkaTemplate<>(producerFactory, true);
        template.setConsumerFactory(consumerFactory);
        return template;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> consumerConfigs = KafkaTestUtils.consumerProps(
                this.brokerAddresses, "kafka-streams",
                "false"
        ); //must set broker addresses in order to use embedded broker
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); //must set k-v deser
        return consumerConfigs;
    }

    @Bean
    public ConsumerFactory<?, ?> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }


}
