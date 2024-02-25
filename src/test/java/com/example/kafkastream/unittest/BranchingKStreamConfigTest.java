package com.example.kafkastream.unittest;

import com.example.kafkastream.BranchingKStreamConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class BranchingKStreamConfigTest extends AbstractKStreamTopologyTestDriverTest {

    BranchingKStreamConfig branchingKStreamConfig;

    Properties defaultProperties;

    @BeforeEach
    void setup() {
        branchingKStreamConfig = new BranchingKStreamConfig();
        defaultProperties = new Properties();
        defaultProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        defaultProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    }

    @Test
    void testBranchingKStream() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        branchingKStreamConfig.branchingPositiveEvenAndOddKStream(streamsBuilder, STREAMING_TOPIC_IN, STREAMING_TOPIC_EVEN_NUMBER, STREAMING_TOPIC_ODD_NUMBER);
        Topology topology = streamsBuilder.build();
        defaultProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, defaultProperties)) {
            TestInputTopic<String, Integer> inputTopic = testDriver.createInputTopic(STREAMING_TOPIC_IN, STRING_SERIALIZER, INTEGER_SERIALIZER);
            TestOutputTopic<String, Integer> evenNumberOutputTopic =
                    testDriver.createOutputTopic(STREAMING_TOPIC_EVEN_NUMBER, STRING_DESERIALIZER, INTEGER_DESERIALIZER);
            TestOutputTopic<String, Integer> oddNumberOutputTopic =
                    testDriver.createOutputTopic(STREAMING_TOPIC_ODD_NUMBER, STRING_DESERIALIZER, INTEGER_DESERIALIZER);

            List<Integer> integers = List.of(-3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
            inputTopic.pipeValueList(integers);
            List<KeyValue<String, Integer>> evenNumbers = evenNumberOutputTopic.readKeyValuesToList();
            List<KeyValue<String, Integer>> oddNumbers = oddNumberOutputTopic.readKeyValuesToList();
            Assertions.assertThat(evenNumbers.stream().map(stringIntegerKeyValue -> stringIntegerKeyValue.value).toList()).containsExactly(2, 4, 6, 8);
            Assertions.assertThat(oddNumbers.stream().map(stringIntegerKeyValue -> stringIntegerKeyValue.value).toList()).containsExactly(1, 3, 5, 7, 9);


        }
    }

}
