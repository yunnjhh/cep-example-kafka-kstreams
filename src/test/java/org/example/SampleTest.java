package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.springframework.boot.test.context.SpringBootTest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SpringBootTest
public class SampleTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Long> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private KeyValueStore<String, Long> store;

  private final Serde<String> stringSerde = new Serdes.StringSerde();
  private final Serde<Long> longSerde = new Serdes.LongSerde();

  @BeforeEach
  public void setup() {
    final Topology topology = new Topology();
    topology.addSource("sourceProcessor", "input-topic");
    topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
    topology.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("aggStore"), Serdes.String(), Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
        "aggregator");
    topology.addSink("sinkProcessor", "result-topic", "aggregator");

    // setup test driver
    final Properties props = new Properties();
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    testDriver = new TopologyTestDriver(topology, props);

    // setup test topics
    inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
    outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

    // pre-populate store
    store = testDriver.getKeyValueStore("aggStore");
    store.put("a", 21L);
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }


  @Test
  public void shouldFlushStoreForFirstInput() {
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void shouldNotUpdateStoreForSmallerValue() {
    inputTopic.pipeInput("a", 1L);
    assertThat(store.get("a"), equalTo(21L));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void shouldNotUpdateStoreForLargerValue() {
    inputTopic.pipeInput("a", 42L);
    assertThat(store.get("a"), equalTo(42L));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 42L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void shouldUpdateStoreForNewKey() {
    inputTopic.pipeInput("b", 21L);
    assertThat(store.get("b"), equalTo(21L));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("b", 21L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void shouldPunctuateIfEvenTimeAdvances() {
    final Instant recordTime = Instant.now();
    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));

    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void shouldPunctuateIfWallClockTimeAdvances() {
    testDriver.advanceWallClockTime(Duration.ofSeconds(60));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }

  public static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {

    @Override
    public Processor<String, Long, String, Long> get() {
      return new CustomMaxAggregator();
    }
  }

  public static class CustomMaxAggregator implements Processor<String, Long, String, Long> {

    ProcessorContext<String, Long> context;
    private KeyValueStore<String, Long> store;

    @Override
    public void init(final ProcessorContext<String, Long> context) {
      this.context = context;
      context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, this::flushStore);
      context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, this::flushStore);
      store = context.getStateStore("aggStore");
    }

    @Override
    public void process(final Record<String, Long> record) {
      final Long oldValue = store.get(record.key());
      if (oldValue == null || record.value() > oldValue) {
        store.put(record.key(), record.value());
      }
    }

    private void flushStore(final long timestamp) {
      try (final KeyValueIterator<String, Long> it = store.all()) {
        while (it.hasNext()) {
          final KeyValue<String, Long> next = it.next();
          context.forward(new Record<>(next.key, next.value, timestamp));
        }
      }
    }
  }
}
