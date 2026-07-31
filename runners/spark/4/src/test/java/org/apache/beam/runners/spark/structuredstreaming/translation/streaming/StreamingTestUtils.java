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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.rules.TemporaryFolder;

/**
 * Shared test scaffolding for the Spark 4 streaming translators: an {@link UnboundedSource} over a
 * fixed, in-memory list of elements, a driver-side static collector {@link DoFn}, and factories for
 * the {@link SparkStructuredStreamingPipelineOptions} / {@link TestPipeline} every streaming test
 * in this package needs.
 *
 * <h2>Why streaming tests in this suite look the way they do</h2>
 *
 * <p>Two of the usual Beam testing tools do not work here, on purpose:
 *
 * <ul>
 *   <li><b>{@code PAssert} on an unbounded {@code PCollection}</b> never fires: {@code PAssert}
 *       needs a final, +infinity watermark to know a window's contents are complete, and this
 *       runner's sources never produce one (see below).
 *   <li><b>{@code StreamingQuery#processAllAvailable()}</b> hangs forever: {@link
 *       org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamMicroBatchStream} (WS-B)
 *       reports progress with opaque epoch offsets, not byte/row counts, so Spark can never decide
 *       that "all available" input has been consumed.
 * </ul>
 *
 * <p>Instead, every test in this package follows the same recipe:
 *
 * <ol>
 *   <li>Build a {@link ListBackedUnboundedSource} from a finite list of elements. The source is
 *       typed {@code UnboundedSource}, so the pipeline is genuinely a streaming pipeline, but it
 *       naturally runs out of input and Spark starts reporting empty micro-batches.
 *   <li>Set {@link SparkStructuredStreamingPipelineOptions#setStreamingStopAfterIdleBatches} (via
 *       {@link #streamingOptions}, already set to {@code 3}) so the runner's idle-stop listener
 *       gracefully stops the query a few empty micro-batches after the source is exhausted, instead
 *       of running forever.
 *   <li>Call {@code pipelineResult.waitUntilFinish()} and only then assert against the {@link
 *       #getCollected} static collector, never against the {@code PCollection} itself.
 * </ol>
 *
 * <h2>The watermark rule</h2>
 *
 * <p><b>A window only fires once the watermark has passed its end, and the watermark only ever
 * advances on new data.</b> Concretely, the watermark Spark computes for the source's event
 * timestamp column is {@code max(eventTimestamp seen so far) - watermarkDelay}, with no idle-time
 * advance: if the source stops producing elements, the watermark freezes where it was, forever. It
 * does <b>not</b> jump to +infinity when the source is exhausted, unlike a bounded pipeline's final
 * watermark.
 *
 * <p>The consequence for test authors: <b>every window (or event-time timer) you want to assert on
 * must be followed, in the input list, by at least one element timestamped after that window's end
 * (or the timer's deadline)</b>. Without such a trailing element the watermark never crosses the
 * threshold and the window or timer never fires, and the test will simply see nothing rather than
 * failing loudly.
 *
 * <p>A second, related wrinkle carried over from WS-C's state bridge tests: the watermark visible
 * inside a stateful operator (a {@code transformWithState} query) is the watermark as of the
 * <em>start</em> of the current micro-batch, not the one just computed from the current batch's own
 * rows. An end-of-window timer whose deadline the data has already crossed therefore fires one
 * micro-batch later than the batch that carried the crossing data, not in that same batch. Tests
 * that assert on timer firings should expect this one micro-batch latency floor and provide enough
 * trailing elements (i.e. enough separate micro-batches) for it.
 *
 * <h2>Local mode only</h2>
 *
 * <p>{@link #getCollected} works only because these tests run Spark in local mode inside the same
 * JVM as the test itself: {@link CollectDoFn} appends to a plain static, synchronized, in-process
 * map. It would not see anything written by executors of a real (multi-JVM) Spark cluster.
 */
public final class StreamingTestUtils {

  private StreamingTestUtils() {}

  // ---------------------------------------------------------------------------------------------
  // Static, in-process collector.
  // ---------------------------------------------------------------------------------------------

  /** Driver-side, per-collector-id accumulation of every element a {@link CollectDoFn} has seen. */
  private static final Map<String, List<Object>> COLLECTORS = new ConcurrentHashMap<>();

  /**
   * A {@link DoFn} that appends every element it sees to the static, in-process collector named
   * {@code collectorId}, then passes the element through unchanged. Safe to use concurrently from
   * multiple bundles/threads; see the class javadoc for why this only works in Spark local mode.
   */
  public static final class CollectDoFn<T> extends DoFn<T, T> {
    private final String collectorId;

    public CollectDoFn(String collectorId) {
      this.collectorId = Preconditions.checkNotNull(collectorId);
    }

    public String getCollectorId() {
      return collectorId;
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      append(collectorId, element);
      out.output(element);
    }
  }

  private static void append(String collectorId, Object value) {
    COLLECTORS
        .computeIfAbsent(collectorId, unused -> Collections.synchronizedList(new ArrayList<>()))
        .add(value);
  }

  /** Returns a snapshot of everything collected so far under {@code collectorId}. */
  @SuppressWarnings("unchecked")
  public static <T> List<T> getCollected(String collectorId) {
    List<Object> values = COLLECTORS.get(collectorId);
    if (values == null) {
      return Collections.emptyList();
    }
    synchronized (values) {
      return (List<T>) new ArrayList<>(values);
    }
  }

  /** Discards everything collected so far under {@code collectorId}. */
  public static void clear(String collectorId) {
    COLLECTORS.remove(collectorId);
  }

  /** Convenience for a collector id that will not collide with other tests or other test runs. */
  public static String newCollectorId(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  // ---------------------------------------------------------------------------------------------
  // Pipeline options / TestPipeline factories.
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns {@link SparkStructuredStreamingPipelineOptions} configured for a streaming test: the
   * {@link SparkStructuredStreamingRunner}, test mode, streaming mode, a 3-idle-batch stop, a 200ms
   * micro-batch trigger, and a checkpoint directory carved out of {@code checkpointDir}.
   *
   * <p>Callers that need a specific {@code SparkSession} (for example the relaxed-Kryo {@code
   * SparkSessionRule} pattern required by any query that hosts a {@code transformWithState}
   * operator, see {@code BeamStatefulProcessorTest}) should additionally call {@code
   * SparkSessionRule#configure} on the returned options, which sets {@code useActiveSparkSession}.
   */
  public static SparkStructuredStreamingPipelineOptions streamingOptions(
      TemporaryFolder checkpointDir) throws IOException {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setTestMode(true);
    options.setStreaming(true);
    options.setStreamingStopAfterIdleBatches(3);
    options.setMaxBatchDurationMillis(200);
    options.setCheckpointDir(checkpointDir.newFolder("checkpoint").getAbsolutePath());
    return options;
  }

  /** Builds a {@link TestPipeline} from {@link #streamingOptions}. */
  public static TestPipeline streamingPipeline(TemporaryFolder checkpointDir) throws IOException {
    return TestPipeline.fromOptions(streamingOptions(checkpointDir));
  }

  // ---------------------------------------------------------------------------------------------
  // ListBackedUnboundedSource.
  // ---------------------------------------------------------------------------------------------

  /**
   * An {@link UnboundedSource} over a fixed, finite {@link List} of {@link TimestampedValue}s.
   *
   * <p>Typed unbounded (so the pipeline it feeds is genuinely a streaming pipeline) but backed by a
   * finite list (so it naturally goes idle once exhausted, which is how tests in this package
   * terminate, see the {@link StreamingTestUtils} class javadoc). Supports explicit, possibly
   * out-of-order event timestamps so tests can inject late data.
   *
   * <p>Uses {@link org.apache.beam.sdk.io.UnboundedSource.CheckpointMark#NOOP_CHECKPOINT_MARK}:
   * this source never needs to resume a partially read split from a durable checkpoint, so there is
   * no state worth persisting between micro-batches beyond the in-memory read position.
   *
   * <p>Elements are stored pre-encoded (via {@code coder}) as {@code byte[]} plus a {@code long}
   * timestamp rather than kept as {@link TimestampedValue} objects, because this source (like any
   * {@link UnboundedSource}) is shipped to executors with plain Java serialization, and {@link
   * TimestampedValue} does not implement {@link java.io.Serializable}.
   *
   * <p>Splitting is round robin: split {@code i} of {@code n} gets every {@code n}-th element
   * starting at offset {@code i}. A single split is returned unchanged if there are not enough
   * elements to make splitting worthwhile; nothing about this source requires more than one split
   * for correctness, round robin merely spreads elements across splits close to evenly.
   */
  public static final class ListBackedUnboundedSource<T>
      extends UnboundedSource<T, UnboundedSource.CheckpointMark.NoopCheckpointMark> {

    private final List<byte[]> encodedElements;
    private final List<Long> timestampsMillis;
    private final Coder<T> coder;

    public ListBackedUnboundedSource(List<TimestampedValue<T>> elements, Coder<T> coder) {
      this.coder = Preconditions.checkNotNull(coder);
      List<byte[]> encoded = new ArrayList<>(elements.size());
      List<Long> timestamps = new ArrayList<>(elements.size());
      for (TimestampedValue<T> element : elements) {
        encoded.add(encode(coder, element.getValue()));
        timestamps.add(element.getTimestamp().getMillis());
      }
      this.encodedElements = encoded;
      this.timestampsMillis = timestamps;
    }

    private ListBackedUnboundedSource(
        List<byte[]> encodedElements, List<Long> timestampsMillis, Coder<T> coder) {
      this.encodedElements = encodedElements;
      this.timestampsMillis = timestampsMillis;
      this.coder = coder;
    }

    @Override
    public List<? extends UnboundedSource<T, UnboundedSource.CheckpointMark.NoopCheckpointMark>>
        split(int desiredNumSplits, PipelineOptions options) {
      int numElements = encodedElements.size();
      if (numElements == 0 || desiredNumSplits <= 1) {
        return Collections.singletonList(this);
      }
      int numSplits = Math.min(desiredNumSplits, numElements);
      List<List<byte[]>> bucketedElements = new ArrayList<>(numSplits);
      List<List<Long>> bucketedTimestamps = new ArrayList<>(numSplits);
      for (int i = 0; i < numSplits; i++) {
        bucketedElements.add(new ArrayList<>());
        bucketedTimestamps.add(new ArrayList<>());
      }
      for (int i = 0; i < numElements; i++) {
        int bucket = i % numSplits;
        bucketedElements.get(bucket).add(encodedElements.get(i));
        bucketedTimestamps.get(bucket).add(timestampsMillis.get(i));
      }
      List<ListBackedUnboundedSource<T>> splits = new ArrayList<>(numSplits);
      for (int i = 0; i < numSplits; i++) {
        splits.add(
            new ListBackedUnboundedSource<>(
                bucketedElements.get(i), bucketedTimestamps.get(i), coder));
      }
      return splits;
    }

    @Override
    public UnboundedReader<T> createReader(
        PipelineOptions options,
        UnboundedSource.CheckpointMark.@Nullable NoopCheckpointMark checkpointMark) {
      return new ListBackedUnboundedReader<>(this);
    }

    @Override
    public Coder<UnboundedSource.CheckpointMark.NoopCheckpointMark> getCheckpointMarkCoder() {
      return new NoopCheckpointMarkCoder();
    }

    @Override
    public Coder<T> getOutputCoder() {
      return coder;
    }

    private static <T> byte[] encode(Coder<T> coder, T value) {
      try {
        return CoderUtils.encodeToByteArray(coder, value);
      } catch (CoderException e) {
        throw new RuntimeException("Failed to encode a ListBackedUnboundedSource element", e);
      }
    }

    private static <T> T decode(Coder<T> coder, byte[] bytes) {
      try {
        return CoderUtils.decodeFromByteArray(coder, bytes);
      } catch (CoderException e) {
        throw new RuntimeException("Failed to decode a ListBackedUnboundedSource element", e);
      }
    }

    private static final class ListBackedUnboundedReader<T> extends UnboundedReader<T> {
      private final ListBackedUnboundedSource<T> source;
      private int index = -1;
      private Instant maxTimestampSeen = BoundedWindow.TIMESTAMP_MIN_VALUE;

      ListBackedUnboundedReader(ListBackedUnboundedSource<T> source) {
        this.source = source;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        int next = index + 1;
        if (next >= source.encodedElements.size()) {
          // Exhausted: report no more data, permanently. The source never produces more once past
          // the end of the backing list.
          return false;
        }
        index = next;
        Instant timestamp = currentTimestamp();
        if (timestamp.isAfter(maxTimestampSeen)) {
          maxTimestampSeen = timestamp;
        }
        return true;
      }

      private Instant currentTimestamp() {
        return new Instant(source.timestampsMillis.get(index));
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        if (index < 0) {
          throw new NoSuchElementException();
        }
        return decode(source.coder, source.encodedElements.get(index));
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (index < 0) {
          throw new NoSuchElementException();
        }
        return currentTimestamp();
      }

      @Override
      public Instant getWatermark() {
        // No idle advance, deliberately: once the list is exhausted this simply stops moving,
        // rather than jumping to +infinity. See the StreamingTestUtils class javadoc.
        return maxTimestampSeen;
      }

      @Override
      public UnboundedSource.CheckpointMark.NoopCheckpointMark getCheckpointMark() {
        return UnboundedSource.CheckpointMark.NOOP_CHECKPOINT_MARK;
      }

      @Override
      public UnboundedSource<T, ?> getCurrentSource() {
        return source;
      }

      @Override
      public void close() throws IOException {}
    }
  }

  /**
   * A trivial coder for the stateless {@link UnboundedSource.CheckpointMark.NoopCheckpointMark}.
   */
  private static final class NoopCheckpointMarkCoder
      extends org.apache.beam.sdk.coders.AtomicCoder<
          UnboundedSource.CheckpointMark.NoopCheckpointMark> {
    @Override
    public void encode(
        UnboundedSource.CheckpointMark.NoopCheckpointMark value, java.io.OutputStream outStream) {
      // Nothing to persist.
    }

    @Override
    public UnboundedSource.CheckpointMark.NoopCheckpointMark decode(java.io.InputStream inStream) {
      return UnboundedSource.CheckpointMark.NOOP_CHECKPOINT_MARK;
    }
  }
}
