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
package org.apache.beam.runners.samza.adapter;

import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.createElementMessage;
import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.createEndOfStreamMessage;
import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.createWatermarkMessage;
import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.expectWrappedException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link BoundedSourceSystem}. */
public class BoundedSourceSystemTest {
  private static final SystemStreamPartition DEFAULT_SSP =
      new SystemStreamPartition("default-system", "default-system", new Partition(0));

  // A reasonable time to wait to get all messages from the bounded source assuming no blocking.
  private static final long DEFAULT_TIMEOUT_MILLIS = 1000;
  private static final String NULL_STRING = null;

  @Test
  public void testConsumerStartStop() throws IOException, InterruptedException {
    final TestBoundedSource<String> source = TestBoundedSource.<String>createBuilder().build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    assertEquals(
        Arrays.asList(
            createWatermarkMessage(DEFAULT_SSP, BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(DEFAULT_SSP)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testConsumeOneMessage() throws IOException, InterruptedException {
    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder().addElements("test").build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, "0", "test", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(DEFAULT_SSP, BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(DEFAULT_SSP)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testAdvanceTimestamp() throws InterruptedException {
    final Instant timestamp = Instant.now();

    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder()
            .addElements("before")
            .setTimestamp(timestamp)
            .addElements("after")
            .build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, "0", "before", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createElementMessage(DEFAULT_SSP, "1", "after", timestamp),
            createWatermarkMessage(DEFAULT_SSP, BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(DEFAULT_SSP)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testConsumeMultipleMessages() throws IOException, InterruptedException {
    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder()
            .addElements("test", "a", "few", "messages")
            .build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, "0", "test", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createElementMessage(DEFAULT_SSP, "1", "a", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createElementMessage(DEFAULT_SSP, "2", "few", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createElementMessage(DEFAULT_SSP, "3", "messages", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(DEFAULT_SSP, BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(DEFAULT_SSP)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testReaderThrowsAtStart() throws Exception {
    final IOException exception = new IOException("Expected exception");

    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder().addException(exception).build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    expectWrappedException(
        exception, () -> consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testReaderThrowsAtAdvance() throws Exception {
    final IOException exception = new IOException("Expected exception");

    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder()
            .addElements("test", "a", "few", "good", "messages", "then", "...")
            .addException(exception)
            .build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    expectWrappedException(
        exception, () -> consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testTimeout() throws Exception {
    final CountDownLatch advanceLatch = new CountDownLatch(1);

    final TestBoundedSource<String> source =
        TestBoundedSource.<String>createBuilder()
            .addElements("before")
            .addLatch(advanceLatch)
            .addElements("after")
            .build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source);

    consumer.register(DEFAULT_SSP, "0");
    consumer.start();
    assertEquals(
        Collections.singletonList(
            createElementMessage(DEFAULT_SSP, "0", "before", BoundedWindow.TIMESTAMP_MIN_VALUE)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));

    advanceLatch.countDown();

    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, "1", "after", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(DEFAULT_SSP, BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(DEFAULT_SSP)),
        consumeUntilTimeoutOrEos(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testSplit() throws IOException, InterruptedException {
    final TestBoundedSource.SplittableBuilder<String> builder =
        TestBoundedSource.<String>createSplits(3);
    builder.forSplit(0).addElements("split-0");
    builder.forSplit(1).addElements("split-1");
    builder.forSplit(2).addElements("split-2");
    final TestBoundedSource<String> source = builder.build();

    final BoundedSourceSystem.Consumer<String> consumer = createConsumer(source, 3);

    consumer.register(ssp(0), NULL_STRING);
    consumer.register(ssp(1), NULL_STRING);
    consumer.register(ssp(2), NULL_STRING);
    consumer.start();

    final Set<String> offsets = new HashSet<>();

    // check split0
    List<IncomingMessageEnvelope> envelopes =
        consumeUntilTimeoutOrEos(consumer, ssp(0), DEFAULT_TIMEOUT_MILLIS);
    assertEquals(
        Arrays.asList(
            createElementMessage(
                ssp(0), envelopes.get(0).getOffset(), "split-0", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(ssp(0), BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(ssp(0))),
        envelopes);
    offsets.add(envelopes.get(0).getOffset());

    // check split1
    envelopes = consumeUntilTimeoutOrEos(consumer, ssp(1), DEFAULT_TIMEOUT_MILLIS);
    assertEquals(
        Arrays.asList(
            createElementMessage(
                ssp(1), envelopes.get(0).getOffset(), "split-1", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(ssp(1), BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(ssp(1))),
        envelopes);
    offsets.add(envelopes.get(0).getOffset());

    // check split2
    envelopes = consumeUntilTimeoutOrEos(consumer, ssp(2), DEFAULT_TIMEOUT_MILLIS);
    assertEquals(
        Arrays.asList(
            createElementMessage(
                ssp(2), envelopes.get(0).getOffset(), "split-2", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createWatermarkMessage(ssp(2), BoundedWindow.TIMESTAMP_MAX_VALUE),
            createEndOfStreamMessage(ssp(2))),
        envelopes);
    offsets.add(envelopes.get(0).getOffset());

    // check offsets
    assertEquals(Sets.newHashSet("0", "1", "2"), offsets);
    consumer.stop();
  }

  private static List<IncomingMessageEnvelope> consumeUntilTimeoutOrEos(
      SystemConsumer consumer, SystemStreamPartition ssp, long timeoutMillis)
      throws InterruptedException {
    assertTrue("Expected timeoutMillis (" + timeoutMillis + ") >= 0", timeoutMillis >= 0);

    final List<IncomingMessageEnvelope> accumulator = new ArrayList<>();
    final long start = System.currentTimeMillis();
    long now = start;
    while (timeoutMillis + start >= now) {
      accumulator.addAll(pollOnce(consumer, ssp, now - start - timeoutMillis));
      if (!accumulator.isEmpty() && accumulator.get(accumulator.size() - 1).isEndOfStream()) {
        break;
      }
      now = System.currentTimeMillis();
    }
    return accumulator;
  }

  private static List<IncomingMessageEnvelope> pollOnce(
      SystemConsumer consumer, SystemStreamPartition ssp, long timeoutMillis)
      throws InterruptedException {
    final Set<SystemStreamPartition> sspSet = Collections.singleton(ssp);
    final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResult =
        consumer.poll(sspSet, timeoutMillis);
    assertEquals(sspSet, pollResult.keySet());
    assertNotNull(pollResult.get(ssp));
    return pollResult.get(ssp);
  }

  private static <T> BoundedSourceSystem.Consumer<String> createConsumer(
      BoundedSource<String> source) {
    return createConsumer(source, 1);
  }

  private static BoundedSourceSystem.Consumer<String> createConsumer(
      BoundedSource<String> source, int splitNum) {
    SamzaPipelineOptions pipelineOptions = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    pipelineOptions.setMaxSourceParallelism(splitNum);
    return new BoundedSourceSystem.Consumer<>(
        source, pipelineOptions, new SamzaMetricsContainer(new MetricsRegistryMap()), "test-step");
  }

  private static SystemStreamPartition ssp(int partition) {
    return new SystemStreamPartition("default-system", "default-system", new Partition(partition));
  }
}
