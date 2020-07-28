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
import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.createWatermarkMessage;
import static org.apache.beam.runners.samza.adapter.TestSourceHelpers.expectWrappedException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.adapter.TestUnboundedSource.SplittableBuilder;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

/** Tests for {@link UnboundedSourceSystem}. */
public class UnboundedSourceSystemTest {

  // A reasonable time to wait to get all messages from the source assuming no blocking.
  private static final long DEFAULT_TIMEOUT_MILLIS = 1000;
  private static final long DEFAULT_WATERMARK_TIMEOUT_MILLIS = 1000;
  private static final String NULL_STRING = null;

  private static final SystemStreamPartition DEFAULT_SSP =
      new SystemStreamPartition("default-system", "default-system", new Partition(0));

  private static final Coder<TestCheckpointMark> CHECKPOINT_MARK_CODER =
      TestUnboundedSource.createBuilder().build().getCheckpointMarkCoder();

  @Test
  public void testConsumerStartStop() throws IOException, InterruptedException {
    final TestUnboundedSource<String> source = TestUnboundedSource.<String>createBuilder().build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, offset(0));
    consumer.start();
    assertEquals(
        Collections.EMPTY_LIST,
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testConsumeOneMessage() throws IOException, InterruptedException {
    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder().addElements("test").build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(
                DEFAULT_SSP, offset(0), "test", BoundedWindow.TIMESTAMP_MIN_VALUE)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testAdvanceTimestamp() throws IOException, InterruptedException {
    final Instant timestamp = Instant.now();

    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .addElements("before")
            .setTimestamp(timestamp)
            .addElements("after")
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(
                DEFAULT_SSP, offset(0), "before", BoundedWindow.TIMESTAMP_MIN_VALUE),
            createElementMessage(DEFAULT_SSP, offset(1), "after", timestamp)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testConsumeMultipleMessages() throws IOException, InterruptedException {
    final Instant timestamp = Instant.now();
    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .setTimestamp(timestamp)
            .addElements("test", "a", "few", "messages")
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, offset(0), "test", timestamp),
            createElementMessage(DEFAULT_SSP, offset(1), "a", timestamp),
            createElementMessage(DEFAULT_SSP, offset(2), "few", timestamp),
            createElementMessage(DEFAULT_SSP, offset(3), "messages", timestamp)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testAdvanceWatermark() throws IOException, InterruptedException {
    final Instant now = Instant.now();
    final Instant nowPlusOne = now.plus(1L);
    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .setTimestamp(now)
            .addElements("first")
            .setTimestamp(nowPlusOne)
            .addElements("second")
            .advanceWatermarkTo(now)
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, offset(0), "first", now),
            createElementMessage(DEFAULT_SSP, offset(1), "second", nowPlusOne),
            createWatermarkMessage(DEFAULT_SSP, now)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_WATERMARK_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-10521")
  public void testMultipleAdvanceWatermark() throws IOException, InterruptedException {
    final Instant now = Instant.now();
    final Instant nowPlusOne = now.plus(1L);
    final Instant nowPlusTwo = now.plus(2L);
    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .setTimestamp(now)
            .addElements("first")
            .advanceWatermarkTo(now)
            .noElements() // will output the first watermark
            .setTimestamp(nowPlusOne)
            .addElements("second")
            .setTimestamp(nowPlusTwo)
            .addElements("third")
            .advanceWatermarkTo(nowPlusOne)
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    // consume to the first watermark
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, offset(0), "first", now),
            createWatermarkMessage(DEFAULT_SSP, now)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_WATERMARK_TIMEOUT_MILLIS));

    // consume to the second watermark
    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, offset(1), "second", nowPlusOne),
            createElementMessage(DEFAULT_SSP, offset(2), "third", nowPlusTwo),
            createWatermarkMessage(DEFAULT_SSP, nowPlusOne)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_WATERMARK_TIMEOUT_MILLIS));

    consumer.stop();
  }

  @Test
  public void testReaderThrowsAtStart() throws Exception {
    final IOException exception = new IOException("Expected exception");

    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder().addException(exception).build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    expectWrappedException(
        exception,
        () -> consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testReaderThrowsAtAdvance() throws Exception {
    final IOException exception = new IOException("Expected exception");

    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .addElements("test", "a", "few", "good", "messages", "then", "...")
            .addException(exception)
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, offset(0));
    consumer.start();
    expectWrappedException(
        exception,
        () -> consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testTimeout() throws Exception {
    final CountDownLatch advanceLatch = new CountDownLatch(1);
    final Instant now = Instant.now();
    final Instant nowPlusOne = now.plus(1);

    final TestUnboundedSource<String> source =
        TestUnboundedSource.<String>createBuilder()
            .setTimestamp(now)
            .addElements("before")
            .addLatch(advanceLatch)
            .setTimestamp(nowPlusOne)
            .addElements("after")
            .advanceWatermarkTo(nowPlusOne)
            .build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source);

    consumer.register(DEFAULT_SSP, NULL_STRING);
    consumer.start();
    assertEquals(
        Collections.singletonList(createElementMessage(DEFAULT_SSP, offset(0), "before", now)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));

    advanceLatch.countDown();

    assertEquals(
        Arrays.asList(
            createElementMessage(DEFAULT_SSP, offset(1), "after", nowPlusOne),
            createWatermarkMessage(DEFAULT_SSP, nowPlusOne)),
        consumeUntilTimeoutOrWatermark(consumer, DEFAULT_SSP, DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  @Test
  public void testRestartFromCheckpoint() throws IOException, InterruptedException {
    final SplittableBuilder<String> builder = TestUnboundedSource.<String>createSplits(3);
    builder.forSplit(0).addElements("split-0");
    builder.forSplit(1).addElements("split-1");
    builder.forSplit(2).addElements("split-2");
    final TestUnboundedSource<String> source = builder.build();

    final UnboundedSourceSystem.Consumer<String, TestCheckpointMark> consumer =
        createConsumer(source, 3);

    consumer.register(ssp(0), offset(10));
    consumer.register(ssp(1), offset(5));
    consumer.register(ssp(2), offset(8));
    consumer.start();
    assertEquals(
        Arrays.asList(
            createElementMessage(ssp(0), offset(11), "split-0", BoundedWindow.TIMESTAMP_MIN_VALUE)),
        consumeUntilTimeoutOrWatermark(consumer, ssp(0), DEFAULT_TIMEOUT_MILLIS));
    assertEquals(
        Arrays.asList(
            createElementMessage(ssp(1), offset(6), "split-1", BoundedWindow.TIMESTAMP_MIN_VALUE)),
        consumeUntilTimeoutOrWatermark(consumer, ssp(1), DEFAULT_TIMEOUT_MILLIS));
    assertEquals(
        Arrays.asList(
            createElementMessage(ssp(2), offset(9), "split-2", BoundedWindow.TIMESTAMP_MIN_VALUE)),
        consumeUntilTimeoutOrWatermark(consumer, ssp(2), DEFAULT_TIMEOUT_MILLIS));
    consumer.stop();
  }

  private static UnboundedSourceSystem.Consumer<String, TestCheckpointMark> createConsumer(
      TestUnboundedSource<String> source) {
    return createConsumer(source, 1);
  }

  private static UnboundedSourceSystem.Consumer<String, TestCheckpointMark> createConsumer(
      TestUnboundedSource<String> source, int splitNum) {
    SamzaPipelineOptions pipelineOptions = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    pipelineOptions.setWatermarkInterval(0L); // emit immediately
    pipelineOptions.setMaxSourceParallelism(splitNum);
    return new UnboundedSourceSystem.Consumer<>(
        source, pipelineOptions, new SamzaMetricsContainer(new MetricsRegistryMap()), "test-step");
  }

  private static List<IncomingMessageEnvelope> consumeUntilTimeoutOrWatermark(
      SystemConsumer consumer, SystemStreamPartition ssp, long timeoutMillis)
      throws InterruptedException {
    assertTrue("Expected timeoutMillis (" + timeoutMillis + ") >= 0", timeoutMillis >= 0);

    final List<IncomingMessageEnvelope> accumulator = new ArrayList<>();
    final long start = System.currentTimeMillis();
    long now = start;
    while (timeoutMillis + start >= now) {
      accumulator.addAll(pollOnce(consumer, ssp, now - start - timeoutMillis));
      if (!accumulator.isEmpty()
          && MessageType.of(accumulator.get(accumulator.size() - 1).getMessage())
              == MessageType.WATERMARK) {
        break;
      }
      now = System.currentTimeMillis();
    }
    return accumulator;
  }

  private static OpMessage.Type getMessageType(IncomingMessageEnvelope envelope) {
    return ((OpMessage) envelope.getMessage()).getType();
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

  private static String offset(int offset) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CHECKPOINT_MARK_CODER.encode(TestCheckpointMark.of(offset), baos);
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  private static SystemStreamPartition ssp(int partition) {
    return new SystemStreamPartition("default-system", "default-system", new Partition(partition));
  }
}
