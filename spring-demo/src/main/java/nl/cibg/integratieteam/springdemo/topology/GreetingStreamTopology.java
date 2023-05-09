package nl.cibg.integratieteam.springdemo.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GreetingStreamTopology {


    public static final String GREETINGS="greetings";
    public static final String GREETINGS_UPPERCASE ="greetings-uppercase";


    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        var greetingStream = streamsBuilder
                .stream(GREETINGS,
        Consumed.with(Serdes.String(), Serdes.String()));

        greetingStream.print(Printed.<String, String>toSysOut().withLabel("greetingStream"));

        var modifiedStream = greetingStream
                .mapValues((readonlyKey, value) -> value.toUpperCase());

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        modifiedStream
                .to(GREETINGS_UPPERCASE,
                        Produced.with(Serdes.String(), Serdes.String()));


    }
}
