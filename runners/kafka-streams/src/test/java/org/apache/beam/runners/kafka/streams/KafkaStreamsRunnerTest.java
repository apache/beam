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
package org.apache.beam.runners.kafka.streams;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.kafka.streams.translation.KStreamsPayload;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/**
 * Pipeline-level integration tests that build a Beam {@link Pipeline} via the high-level Java SDK
 * ({@code Pipeline.create().apply(Impulse.create())}) and run it through {@link
 * KafkaStreamsTestRunner}.
 *
 * <p>This is the test layer Jan requested on PR #38689: rather than building hand-rolled {@link
 * RunnerApi.Pipeline} protos, drive translation from the same surface a user would write. The tests
 * stop short of calling {@code pipeline.run()} because that would require a real Kafka broker —
 * {@code TopologyTestDriver} replaces the broker for unit-test purposes.
 */
public class KafkaStreamsRunnerTest {

  @Test
  public void impulseOnlyPipelineEmitsDataAndTerminalWatermark() {
    Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
    pipeline.apply("impulse", Impulse.create());

    CapturingProcessor capture = new CapturingProcessor();
    Topology topology = KafkaStreamsTestRunner.translate(pipeline).getTopology();
    // Impulse is the only transform, so it is the topology leaf; capture what it forwards.
    topology.addProcessor(
        "capture", capture, KafkaStreamsTestRunner.findAnyLeafProcessorName(topology));

    try (TopologyTestDriver driver =
        new TopologyTestDriver(topology, KafkaStreamsTestRunner.streamsConfig(pipeline))) {
      driver.advanceWallClockTime(Duration.ofSeconds(1));
      driver.advanceWallClockTime(Duration.ofSeconds(1));
    }

    assertThat(capture.received.size(), is(2));
    assertThat(capture.received.get(0).isData(), is(true));
    assertThat(capture.received.get(0).getData().getValue().length, is(0));
    assertThat(capture.received.get(1).isWatermark(), is(true));
    assertThat(
        capture.received.get(1).asWatermark().getWatermarkMillis(),
        is(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
  }

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
