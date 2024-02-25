package com.example.kafkastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Configuration
@Slf4j
public class FilteringOddNumberKStreamConfig {

    @Bean(name = "filteringOddNumberKStreamsConfig")
    public KafkaStreamsConfiguration kafkaStreamsConfig(
            @Value("${spring.kafka.streams.bootstrap-servers}") String kafkaStreamBootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String kStreamApplicationId
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kStreamApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamBootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean("filteringOddNumberKStreamStreamBuilder")
    public StreamsBuilderFactoryBean filteringOddNumberKStreamStreamBuilder(
            @Qualifier("filteringOddNumberKStreamsConfig")
            ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider, ObjectProvider<StreamsBuilderFactoryBeanConfigurer> configurerProvider
    ) {
        KafkaStreamsConfiguration streamsConfig = streamsConfigProvider.getIfAvailable();
        StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean(Objects.requireNonNull(streamsConfig));
        configurerProvider.orderedStream().forEach(configurer -> configurer.configure(fb));
        return fb;
    }

    @Bean("filteringOddNumberKStream")
    public KStream<String, Integer> filteringOddNumberKStream(
            @Qualifier("filteringOddNumberKStreamStreamBuilder") StreamsBuilder streamsBuilder,
            @Value("${kafka.stream.filtering-topic-in}") String filteringStreamingTopicIn,
            @Value("${kafka.stream.filtering-topic-out}") String filteringStreamingTopicOut
    ) {
        KStream<String, Integer> filterOddNumberStream = streamsBuilder.stream(filteringStreamingTopicIn);
        filterOddNumberStream.filter((key, value) -> {
            log.info("Filter key, value pair");
            return value % 2 == 1;
        }).to(filteringStreamingTopicOut);
        Topology topology = streamsBuilder.build();
        log.info("Filtering odd number stream topology describe: {}", topology.describe());
        return filterOddNumberStream;

    }

}
