package com.example.kafkastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class BranchingKStreamConfig {

    @Bean("branchingPositiveEvenAndOddKStream")
    public KStream<String, Integer> branchingPositiveEvenAndOddKStream(
            @Qualifier("filteringOddNumberKStreamStreamBuilder") StreamsBuilder streamsBuilder,
            @Value("${kafka.stream.branching-topic-in}") String branchingTopicIn,
            @Value("${kafka.stream.branching-even-topic}") String branchingEvenTopic,
            @Value("${kafka.stream.branching-odd-topic}") String branchingOddTopic
    ) {
        KStream<String, Integer> branchingPositiveEvenAndOddKStream = streamsBuilder.stream(branchingTopicIn);
        Predicate<String, Integer> positiveNumber = (key, value) -> value > 0;
        Predicate<String, Integer> evenNumber = (key, value) -> value % 2 == 0;
        Predicate<String, Integer> oddNumber = (key, value) -> value % 2 == 1;
        branchingPositiveEvenAndOddKStream.filter(positiveNumber).split()
                .branch(evenNumber, Branched.withConsumer(stringIntegerKStream -> stringIntegerKStream.to(branchingEvenTopic)))
                .branch(oddNumber, Branched.withConsumer(stringIntegerKStream -> stringIntegerKStream.to(branchingOddTopic)));
        log.info("Branching positive even and odd kstream: {}", streamsBuilder.build().describe());
        return branchingPositiveEvenAndOddKStream;

    }
}
