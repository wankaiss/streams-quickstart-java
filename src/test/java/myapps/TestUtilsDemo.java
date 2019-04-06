package myapps;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;

import java.util.Properties;

public class TestUtilsDemo {

    @Test
    public void test_demo1() {
        // Processor API
        /*Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("processor", null, "sourceProcessor");
        topology.addSink("sinkProcessor", "processor");*/

        // Using DSL(Domain Specific Language)
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").filter((o1, o2) -> true).to("output-topic");
        Topology topology1 = builder.build();

        // Setup test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver testDriver = new TopologyTestDriver(topology1, props);

        ConsumerRecordFactory<String, Integer> factory = new ConsumerRecordFactory<>(
            "input-topic", new StringSerializer(), new IntegerSerializer());
        testDriver.pipeInput(factory.create("input-topic","a",1));

        ProducerRecord<String, Integer> producerRecord = testDriver.readOutput("output-topic",
            new StringDeserializer(), new IntegerDeserializer());

        OutputVerifier.compareKeyValue(producerRecord, "a", 1);

        testDriver.close();
    }
}
