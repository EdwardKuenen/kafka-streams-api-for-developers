package nl.cibg.integratieteam.springdemo.config;


import nl.cibg.integratieteam.springdemo.topology.GreetingStreamTopology;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class GreetingStreamConfiguration {

    @Bean
    public NewTopic greetingsTopic() {
        return TopicBuilder.name(GreetingStreamTopology.GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic greetingsOutputTopic() {
        return TopicBuilder.name(GreetingStreamTopology.GREETINGS_UPPERCASE)
                .partitions(2)
                .replicas(1)
                .build();
    }

}
