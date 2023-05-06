package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        createTopics(properties, List.of(GreetingsTopology.GREETINGS_UPPERCASE, GreetingsTopology.GREETINGS));

        Topology greetingsTopology = GreetingsTopology.buildTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(greetingsTopology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try {
            kafkaStreams.start();

        } catch (Exception ex) {
            log.error("Exception in starting stream : {}", ex.getMessage(), ex);
        }
    }


    private static void createTopics(Properties config, List<String> greetings) {

        try (AdminClient admin = AdminClient.create(config)) {
            int partitions = 2;
            short replication = 1;

            List<NewTopic> newTopics = greetings
                    .stream()
                    .map(topic -> new NewTopic(topic, partitions, replication))
                    .collect(Collectors.toList());

            CreateTopicsResult createTopicResult = admin.createTopics(newTopics);

            createTopicResult.all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ", e.getMessage(), e);
        }
    }
}
