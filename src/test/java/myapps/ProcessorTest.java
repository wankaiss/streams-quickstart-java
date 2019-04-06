package myapps;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class ProcessorTest {

    Processor processorUnderTest;
    MockProcessorContext context;
    KeyValueStore<String, Integer> store;

    @Before
    public void setup() {
        processorUnderTest = new UnderTestProcessor();
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put("some.other.config", "some config value");
        context = new MockProcessorContext(props);
        context.setRecordMetadata("topicName", /*partition*/ 0, /*offset*/ 0L, new TestHeaders(), /*timestamp*/ 0L);
        context.setTopic("topicName");
        context.setPartition(0);
        context.setOffset(0L);
        context.setTimestamp(0L);

        store = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("myStore"),
            Serdes.String(),
            Serdes.Integer()
        ).withLoggingDisabled().build();

        store.init(context, store);
        context.register(store, null);

        processorUnderTest.init(context);
    }


    static class UnderTestProcessor implements Processor<String, String> {

        ProcessorContext context;
        private KeyValueStore<String, Long> store;

        @Override
        public void init(ProcessorContext processorContext) {
            context = processorContext;
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
            context.schedule(60000, PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
        }

        @Override
        public void process(String o, String o2) {
            context.forward(o, o2);
        }

        @Override
        public void close() {
        }

        private void flushStore() {
            KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }
    }

    static class TestHeaders implements Headers {
        @Override
        public Headers add(Header header) throws IllegalStateException {
            return null;
        }

        @Override
        public Headers add(String s, byte[] bytes) throws IllegalStateException {
            return null;
        }

        @Override
        public Headers remove(String s) throws IllegalStateException {
            return null;
        }

        @Override
        public Header lastHeader(String s) {
            return null;
        }

        @Override
        public Iterable<Header> headers(String s) {
            return null;
        }

        @Override
        public Header[] toArray() {
            return new Header[0];
        }

        @Override
        public Iterator<Header> iterator() {
            return null;
        }
    }





    @Test
    public void processorTest() {
        processorUnderTest.process("key", "value");

        Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertEquals(forwarded.next().keyValue(), new KeyValue("key", "value"));
        assertFalse(forwarded.hasNext());

        context.resetForwards();
        assertEquals(context.forwarded().size(), 0);

        // Capture forwarded data which will be forward to childProcessor
        List<MockProcessorContext.CapturedForward> capturedForwards = context.forwarded("childProcessorName");

        context.commit();  // Commit processor's forward
        assertTrue(context.committed());
        context.resetCommit();
        assertFalse(context.committed());
    }

    @Test
    public void stateStoreTest() {
        MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        PunctuationType type = capturedPunctuator.getType();
        System.out.println(type);
        long intervalMs = capturedPunctuator.getIntervalMs();
        System.out.println(intervalMs);
        System.out.println(capturedPunctuator.cancelled());
    }


}
