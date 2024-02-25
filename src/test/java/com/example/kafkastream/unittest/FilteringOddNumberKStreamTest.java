package com.example.kafkastream.unittest;

import com.example.kafkastream.FilteringOddNumberKStreamConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;


class FilteringOddNumberKStreamTest extends AbstractKStreamTopologyTestDriverTest {

    FilteringOddNumberKStreamConfig filteringOddNumberKStream;

    Properties defaultProperties;

    @BeforeEach
    void setup() {
        filteringOddNumberKStream = new FilteringOddNumberKStreamConfig();
        defaultProperties = new Properties();
        defaultProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        defaultProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    }


    @Test
    void testFilteringKStream() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        filteringOddNumberKStream.filteringOddNumberKStream(streamsBuilder, STREAMING_TOPIC_IN, STREAMING_TOPIC_OUT);
        Topology topology = streamsBuilder.build();
        defaultProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, defaultProperties)) {
            TestInputTopic<String, Integer> inputTopic = testDriver.createInputTopic(STREAMING_TOPIC_IN, STRING_SERIALIZER, INTEGER_SERIALIZER);
            TestOutputTopic<String, Integer> outputTopic = testDriver.createOutputTopic(STREAMING_TOPIC_OUT, STRING_DESERIALIZER, INTEGER_DESERIALIZER);

            List<Integer> integers = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
            inputTopic.pipeValueList(integers);
            List<KeyValue<String, Integer>> keyValues = outputTopic.readKeyValuesToList();
            Assertions.assertThat(keyValues.stream().map(stringIntegerKeyValue -> stringIntegerKeyValue.value).toList()).containsExactly(1, 3, 5, 7, 9);
        }
    }
}