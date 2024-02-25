package com.example.kafkastream.unittest;


import com.example.kafkastream.KStreamConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

/**
 * KstreamTest with TopologyTestDriver
 */
class KStreamTopologyTestDriverTest {

    public static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    public static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    private static final String STREAMING_TOPIC_IN = "STREAMING_TOPIC_IN";

    private static final String STREAMING_TOPIC_OUT = "STREAMING_TOPIC_OUT";

    KStreamConfig kStreamConfig;

    @BeforeEach
    void setup() {
        kStreamConfig = new KStreamConfig();
    }

    @Test
    void testKStream() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kStreamConfig.kStream(streamsBuilder, STREAMING_TOPIC_IN, STREAMING_TOPIC_OUT);
        Topology topology = streamsBuilder.build();

        Properties config = new Properties();
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, config)) { //should put the outputTopic.readKeyValuesToList() inside the try-with resources block, otherwise, the output
            TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(STREAMING_TOPIC_IN, STRING_SERIALIZER, STRING_SERIALIZER);
            TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(STREAMING_TOPIC_OUT, STRING_DESERIALIZER, STRING_DESERIALIZER);
            String message = "message";
            String key = "key";
            inputTopic.pipeInput(key,message);
            List<KeyValue<String, String>> keyValueList = outputTopic.readKeyValuesToList();
            Assertions.assertThat(keyValueList).containsExactly(new KeyValue<>(key, message.concat(" out")));
        }


    }
}
