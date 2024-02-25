package com.example.kafkastream.unittest;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

abstract class AbstractKStreamTopologyTestDriverTest {
    protected static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    protected static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    protected static final String STREAMING_TOPIC_IN = "STREAMING_TOPIC_IN";

    protected static final String STREAMING_TOPIC_OUT = "STREAMING_TOPIC_OUT";

    protected static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();

    protected static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();

    static final String STREAMING_TOPIC_ODD_NUMBER = "STREAMING_TOPIC_ODD_NUMBER";

    static final String STREAMING_TOPIC_EVEN_NUMBER = "STREAMING_TOPIC_EVEN_NUMBER";
}
