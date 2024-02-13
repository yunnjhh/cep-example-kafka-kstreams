package org.example.service.count.simple;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@SpringBootTest()
class WordCountProcessorDemoTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @Autowired
  KafkaStreamsConfiguration kafkaConfig;

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    //Create Actual Stream Processing pipeline
    new WordCountProcessorDemo(builder);
    testDriver = new TopologyTestDriver(builder.build(), kafkaConfig.asProperties());
    inputTopic = testDriver.createInputTopic("streams-app-processor-input", new StringSerializer(), new StringSerializer());
    outputTopic = testDriver.createOutputTopic("streams-app-processor-output", new StringDeserializer(), new LongDeserializer());
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testOneWord() {
    inputTopic.pipeInput("A A A A A A A A A A");
    inputTopic.pipeInput("A A A A A A A A A A");
    inputTopic.pipeInput("A A A A A A A A A A");
    inputTopic.pipeInput("A A A A A A A A A A");
    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A");
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 51L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }
}