package com.example.kafkastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KStreamConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig(
            @Value("${spring.kafka.streams.bootstrap-servers}") String kafkaStreamBootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String kStreamApplicationId
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kStreamApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamBootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> kStream(
            @Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder streamsBuilder, @Value("${kafka.stream.topic-in}") String streamingTopicIn,
            @Value("${kafka.stream.topic-out}") String streamingTopicOut
    ) {
        KStream<String, String> kStream = streamsBuilder.stream(streamingTopicIn);
        kStream.mapValues((readOnlyKey, value) -> value.concat(" out")).to(streamingTopicOut);
        Topology topology = streamsBuilder.build();
        log.info("Kstream topology describe: {}", topology.describe());
        return kStream;
    }

}
