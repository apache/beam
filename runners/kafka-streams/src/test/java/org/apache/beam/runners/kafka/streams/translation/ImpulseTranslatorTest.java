/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/**
 * Behavioural tests for {@link ImpulseTranslator} using {@link TopologyTestDriver}.
 *
 * <p>The translator builds a topology with a real source + processor pair. The tests sit a {@link
 * CapturingProcessor} downstream so emitted {@link KStreamsPayload} elements can be inspected
 * directly without going through a Kafka sink topic (the runner does not produce one because no
 * downstream PCollections exist yet).
 */
public class ImpulseTranslatorTest {

  @Test
  public void impulseEmitsDataElementFollowedByTerminalWatermark() {
    KafkaStreamsTranslationContext context = KafkaStreamsPipelineTranslatorTest.newContext();
    new KafkaStreamsPipelineTranslator()
        .translate(context, KafkaStreamsPipelineTranslatorTest.singleImpulsePipeline());

    CapturingProcessor capture = new CapturingProcessor();
    Topology topology = context.getTopology();
    topology.addProcessor("capture", capture, "impulse");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, baseProps())) {
      driver.advanceWallClockTime(Duration.ofSeconds(1));
      driver.advanceWallClockTime(Duration.ofSeconds(1));
    }

    assertThat(capture.received.size(), is(2));

    KStreamsPayload<byte[]> dataPayload = capture.received.get(0);
    assertThat(dataPayload, is(notNullValue()));
    assertThat(dataPayload.isData(), is(true));
    WindowedValue<byte[]> data = dataPayload.getData();
    assertThat(data.getValue().length, is(0));
    assertThat(data.getWindows().size(), is(1));
    assertThat(data.getTimestamp().getMillis(), is(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));

    KStreamsPayload<byte[]> watermarkPayload = capture.received.get(1);
    assertThat(watermarkPayload.isWatermark(), is(true));
    assertThat(
        watermarkPayload.getWatermarkMillis(), is(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
  }

  @Test
  public void impulseDoesNotReEmitDataOnRepeatedPunctuation() {
    KafkaStreamsTranslationContext context = KafkaStreamsPipelineTranslatorTest.newContext();
    new KafkaStreamsPipelineTranslator()
        .translate(context, KafkaStreamsPipelineTranslatorTest.singleImpulsePipeline());

    CapturingProcessor capture = new CapturingProcessor();
    Topology topology = context.getTopology();
    topology.addProcessor("capture", capture, "impulse");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, baseProps())) {
      driver.advanceWallClockTime(Duration.ofSeconds(1));
      // Trigger again — the data element is gated by the state store; the punctuator should also
      // have been cancelled after the first emission, so no further events should be captured.
      driver.advanceWallClockTime(Duration.ofSeconds(5));
    }

    assertThat(capture.received.size(), is(2));
    assertThat(capture.received.get(0).isData(), is(true));
    assertThat(capture.received.get(1).isWatermark(), is(true));
  }

  private static Properties baseProps() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-translator-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    props.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    return props;
  }

  /**
   * Captures {@link KStreamsPayload} records forwarded by {@link ImpulseProcessor}. The supplier
   * returns a fresh forwarder each call (required by Kafka Streams) but all forwarders write into
   * the shared {@link #received} list so the test can read the captured elements after the topology
   * is closed.
   */
  private static class CapturingProcessor
      implements ProcessorSupplier<
          byte[], KStreamsPayload<byte[]>, byte[], KStreamsPayload<byte[]>> {

    final List<KStreamsPayload<byte[]>> received = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Processor<byte[], KStreamsPayload<byte[]>, byte[], KStreamsPayload<byte[]>> get() {
      return new Processor<byte[], KStreamsPayload<byte[]>, byte[], KStreamsPayload<byte[]>>() {
        @Override
        public void init(@Nullable ProcessorContext<byte[], KStreamsPayload<byte[]>> context) {
          // no-op
        }

        @Override
        public void process(Record<byte[], KStreamsPayload<byte[]>> record) {
          received.add(record.value());
        }
      };
    }
  }
}
