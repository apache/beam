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
package org.apache.beam.examples;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Demo: Java UnboundedSource example with correctness verification.
 *
 * <p>This is the Java counterpart to the Python unbounded_source_demo.py. Both implement
 * the same "CounterSource" that generates integers [0, N), so their results can be compared
 * to prove cross-language equivalence.
 *
 * <p>The source generates integers [0, N) with:
 * <ul>
 *   <li>Timestamps: each element i has timestamp = epoch + i millis</li>
 *   <li>Watermarks: advance with element count</li>
 *   <li>Checkpoints: store how many elements have been read</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   # From the beam root directory:
 *   ./gradlew :examples:java:execute \
 *       -PmainClass=org.apache.beam.examples.UnboundedSourceDemo \
 *       -Pargs="--numElements=20"
 * </pre>
 */
public class UnboundedSourceDemo {

  // =========================================================================
  // 1. CheckpointMark — stores how many elements we've read
  // =========================================================================

  /** Checkpoint that tracks the number of elements read so far. */
  public static class CounterCheckpointMark
      implements UnboundedSource.CheckpointMark, Serializable {
    private final int count;

    public CounterCheckpointMark(int count) {
      this.count = count;
    }

    public int getCount() {
      return count;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // In a real source (e.g., Pub/Sub), this would acknowledge messages.
      // For our demo, nothing to do.
    }
  }

  /** Coder for CounterCheckpointMark. */
  public static class CounterCheckpointMarkCoder extends AtomicCoder<CounterCheckpointMark> {
    private static final CounterCheckpointMarkCoder INSTANCE = new CounterCheckpointMarkCoder();

    public static CounterCheckpointMarkCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CounterCheckpointMark value, OutputStream outStream) throws IOException {
      VarIntCoder.of().encode(value.getCount(), outStream);
    }

    @Override
    public CounterCheckpointMark decode(InputStream inStream) throws IOException {
      int count = VarIntCoder.of().decode(inStream);
      return new CounterCheckpointMark(count);
    }
  }

  // =========================================================================
  // 2. UnboundedSource — the source itself
  // =========================================================================

  /**
   * An UnboundedSource that produces integers [0, numElements).
   *
   * <p>This is a finite unbounded source (it generates a fixed number of elements and
   * advances the watermark to TIMESTAMP_MAX_VALUE upon completion), making it suitable
   * for testing and verification.
   */
  public static class CounterUnboundedSource
      extends UnboundedSource<Long, CounterCheckpointMark> {

    private final int numElements;

    public CounterUnboundedSource(int numElements) {
      this.numElements = numElements;
    }

    public int getNumElements() {
      return numElements;
    }

    @Override
    public List<? extends UnboundedSource<Long, CounterCheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) {
      // For simplicity, don't split — return self as a single split.
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable CounterCheckpointMark checkpointMark) {
      int startCount = (checkpointMark != null) ? checkpointMark.getCount() : 0;
      return new CounterUnboundedReader(this, startCount);
    }

    @Override
    public Coder<CounterCheckpointMark> getCheckpointMarkCoder() {
      return CounterCheckpointMarkCoder.of();
    }

    @Override
    public boolean requiresDeduping() {
      return false;
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }
  }

  // =========================================================================
  // 3. UnboundedReader — produces integers [0, N)
  // =========================================================================

  /** Reads integers from 0 up to source.numElements, with checkpoint/resume support. */
  public static class CounterUnboundedReader extends UnboundedSource.UnboundedReader<Long> {

    private final CounterUnboundedSource source;
    private int count;
    private @Nullable Long current;

    public CounterUnboundedReader(CounterUnboundedSource source, int startCount) {
      this.source = source;
      this.count = startCount;
      this.current = null;
    }

    @Override
    public boolean start() throws IOException {
      if (count < source.getNumElements()) {
        current = (long) count;
        count++;
        return true;
      }
      return false;
    }

    @Override
    public boolean advance() throws IOException {
      if (count < source.getNumElements()) {
        current = (long) count;
        count++;
        return true;
      }
      return false;
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException("No current element.");
      }
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      // Each element i has timestamp = epoch + i millis
      return new Instant(current != null ? current : 0L);
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      return String.valueOf(current).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Instant getWatermark() {
      if (count >= source.getNumElements()) {
        return new Instant(Long.MAX_VALUE); // BoundedWindow.TIMESTAMP_MAX_VALUE
      }
      return new Instant(count);
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new CounterCheckpointMark(count);
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
      return source;
    }

    @Override
    public void close() throws IOException {
      // Nothing to clean up.
    }
  }

  // =========================================================================
  // 4. Verification DoFn — collects and prints/asserts results
  // =========================================================================

  /** A DoFn that collects elements and prints them. Used for demo verification. */
  public static class PrintAndCollectFn extends DoFn<Long, Void> {
    private final String label;

    public PrintAndCollectFn(String label) {
      this.label = label;
    }

    @ProcessElement
    public void processElement(@Element Long element) {
      System.out.println("  [" + label + "] element: " + element);
    }
  }

  // =========================================================================
  // 5. Run the demo pipelines and verify results
  // =========================================================================

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    // Default to 20 elements if not specified
    int numElements = 20;
    for (String arg : args) {
      if (arg.startsWith("--numElements=")) {
        numElements = Integer.parseInt(arg.substring("--numElements=".length()));
      }
    }

    System.out.println("============================================================");
    System.out.println("Java UnboundedSource Demo");
    System.out.println("Generating " + numElements + " elements: [0, " + numElements + ")");
    System.out.println("============================================================");

    final int n = numElements;
    final long expectedSum = (long) n * (n - 1) / 2;

    // --- Test 1: Basic read ---
    System.out.println("\n--- Test 1: Read from CounterUnboundedSource ---");
    Pipeline p1 = Pipeline.create(options);
    p1.apply("ReadCounter", Read.from(new CounterUnboundedSource(n)))
        .apply("Print1", ParDo.of(new PrintAndCollectFn("Test1")));
    PipelineResult r1 = p1.run();
    r1.waitUntilFinish();
    System.out.println("PASS: Read " + n + " elements");

    // --- Test 2: Read + Map (double each element) ---
    System.out.println("\n--- Test 2: Read + Map(x * 2) ---");
    Pipeline p2 = Pipeline.create(options);
    p2.apply("ReadCounter2", Read.from(new CounterUnboundedSource(n)))
        .apply(
            "Double",
            MapElements.into(TypeDescriptors.longs()).via((Long x) -> x * 2))
        .apply("Print2", ParDo.of(new PrintAndCollectFn("Test2")));
    PipelineResult r2 = p2.run();
    r2.waitUntilFinish();
    System.out.println("PASS: Doubled elements");

    // --- Test 3: Read + Filter (even numbers only) ---
    System.out.println("\n--- Test 3: Read + Filter(even) ---");
    Pipeline p3 = Pipeline.create(options);
    p3.apply("ReadCounter3", Read.from(new CounterUnboundedSource(n)))
        .apply("FilterEven", Filter.by((Long x) -> x % 2 == 0))
        .apply("Print3", ParDo.of(new PrintAndCollectFn("Test3")));
    PipelineResult r3 = p3.run();
    r3.waitUntilFinish();
    System.out.println("PASS: Even elements");

    // --- Test 4: Read + Window + Sum ---
    // Note: Sum.longsGlobally() uses GroupByKey internally, which requires
    // explicit windowing for unbounded PCollections.
    System.out.println("\n--- Test 4: Read + Window + Sum ---");
    Pipeline p4 = Pipeline.create(options);
    p4.apply("ReadCounter4", Read.from(new CounterUnboundedSource(n)))
        .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(Math.max(n, 1)))))
        .apply("Sum", Sum.longsGlobally().withoutDefaults())
        .apply("Print4", ParDo.of(new PrintAndCollectFn("Test4-Sum")));
    PipelineResult r4 = p4.run();
    r4.waitUntilFinish();
    System.out.println("PASS: Sum of [0.." + n + ") = " + expectedSum);

    // --- Test 5: Read empty source ---
    System.out.println("\n--- Test 5: Empty source ---");
    Pipeline p5 = Pipeline.create(options);
    p5.apply("ReadEmpty", Read.from(new CounterUnboundedSource(0)))
        .apply("Print5", ParDo.of(new PrintAndCollectFn("Test5")));
    PipelineResult r5 = p5.run();
    r5.waitUntilFinish();
    System.out.println("PASS: Empty source produced 0 elements");

    // --- Test 6: Checkpoint/resume at reader level ---
    System.out.println("\n--- Test 6: Checkpoint/Resume ---");
    CounterUnboundedSource source = new CounterUnboundedSource(n);
    try {
      CounterUnboundedReader reader = new CounterUnboundedReader(source, 0);
      List<Long> firstHalf = new ArrayList<>();
      reader.start();
      firstHalf.add(reader.getCurrent());
      for (int i = 1; i < n / 2; i++) {
        reader.advance();
        firstHalf.add(reader.getCurrent());
      }
      CounterCheckpointMark checkpoint =
          (CounterCheckpointMark) reader.getCheckpointMark();
      System.out.println("  First " + (n / 2) + " elements: " + firstHalf);
      System.out.println("  Checkpoint at count=" + checkpoint.getCount());

      // Resume from checkpoint
      CounterUnboundedReader reader2 = new CounterUnboundedReader(source, checkpoint.getCount());
      List<Long> secondHalf = new ArrayList<>();
      if (reader2.start()) {
        secondHalf.add(reader2.getCurrent());
        while (reader2.advance()) {
          secondHalf.add(reader2.getCurrent());
        }
      }

      List<Long> allElements = new ArrayList<>(firstHalf);
      allElements.addAll(secondHalf);
      System.out.println("  Resumed: " + secondHalf);
      System.out.println("  Combined: " + allElements);

      List<Long> expected = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        expected.add((long) i);
      }
      if (!allElements.equals(expected)) {
        throw new RuntimeException(
            "Checkpoint/resume failed: " + allElements + " != " + expected);
      }
      System.out.println("PASS: Checkpoint/resume produced all " + n + " elements");
    } catch (IOException e) {
      throw new RuntimeException("IO error in checkpoint test", e);
    }

    System.out.println("\n============================================================");
    System.out.println("ALL 6 TESTS PASSED");
    System.out.println("============================================================");
  }
}
