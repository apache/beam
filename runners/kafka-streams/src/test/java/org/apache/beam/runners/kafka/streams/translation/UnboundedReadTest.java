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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.CountingSource.CounterMark;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.streams.TopologyTestDriver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

/**
 * Runs a pipeline whose source is unbounded, which is the shape the runner exists for: a Kafka
 * Streams application is a long-running stream processor, and until now the runner could only read
 * sources that finish.
 *
 * <p>Two things separate this from the bounded read. The source is polled repeatedly rather than
 * drained once, so elements arrive over several turns of the wall clock; and the watermark comes
 * from the reader's own progress rather than jumping to the end of time when the input runs out,
 * which is what lets downstream windows close on a stream that never ends.
 */
public class UnboundedReadTest {

  /** How many elements one poll of the source may take. */
  private static final int ELEMENTS_PER_POLL = 5;

  /** Elements the finite variant of the source produces before ending time. */
  private static final int ELEMENTS = 12;

  /** Elements the pipeline has seen, recorded in order. */
  private static final List<Long> RECEIVED = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void reset() {
    RECEIVED.clear();
  }

  private static class RecordFn extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<Long> out) {
      RECEIVED.add(element);
      out.output(element);
    }
  }

  /**
   * A genuinely unbounded pipeline. Nothing caps the source — capping it with {@code
   * withMaxNumRecords} would turn it back into a bounded read and test the wrong path — so the work
   * is bounded instead by how many elements a single poll may take and how many turns the test
   * drives.
   */
  private static Pipeline unboundedPipeline() {
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setMaxBundleSize(ELEMENTS_PER_POLL);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("read", Read.from(CountingSource.unbounded()))
        .apply("record", ParDo.of(new RecordFn()));
    return pipeline;
  }

  @Test
  public void anUnboundedSourceIsPolledAndItsElementsReachTheHarness() {
    Pipeline pipeline = unboundedPipeline();
    KafkaStreamsTranslationContext context = KafkaStreamsTestRunner.translate(pipeline);

    try (TopologyTestDriver driver =
        new TopologyTestDriver(
            context.getTopology(), KafkaStreamsTestRunner.streamsConfig(pipeline))) {
      // Several turns, because an unbounded read yields what is available now rather than
      // everything at once.
      for (int turn = 0; turn < 4; turn++) {
        driver.advanceWallClockTime(Duration.ofMillis(100));
      }
    }

    // More than a single poll's worth, which is the point: a bounded read drains once, whereas
    // this one has to be asked again on each turn of the clock and keep going from where it was.
    assertThat(
        "expected several polls' worth of elements, got " + RECEIVED.size(),
        RECEIVED.size(),
        is(greaterThan(ELEMENTS_PER_POLL)));
    // The source counts from zero, so what arrived has to start there and be contiguous — no gap
    // and no repeat, which is what the checkpoint mark between polls is for.
    for (int i = 0; i < RECEIVED.size(); i++) {
      assertThat(RECEIVED.get(i), is((long) i));
    }
  }

  /** A pipeline whose source may read {@code elementsPerPoll} elements and run for {@code ms}. */
  private static Pipeline pipelineWithPollBounds(int elementsPerPoll, int maxPollTimeMs) {
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setReadMaxElementsPerPoll(elementsPerPoll);
    options.setReadMaxPollTimeMs(maxPollTimeMs);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("read", Read.from(CountingSource.unbounded()))
        .apply("record", ParDo.of(new RecordFn()));
    return pipeline;
  }

  /** Drives one turn of the wall clock and reports how many elements the source produced. */
  private static int elementsInOneTurn(Pipeline pipeline) {
    KafkaStreamsTranslationContext context = KafkaStreamsTestRunner.translate(pipeline);
    try (TopologyTestDriver driver =
        new TopologyTestDriver(
            context.getTopology(), KafkaStreamsTestRunner.streamsConfig(pipeline))) {
      driver.advanceWallClockTime(Duration.ofMillis(100));
    }
    return RECEIVED.size();
  }

  /**
   * The element bound cannot bound the time a turn takes, because how long an element takes is
   * decided by the pipeline below the source. Left on the count alone, a source with data always
   * available runs the full count every turn, overruns the punctuation interval, and is due again
   * the moment it returns — so it keeps the thread and the rest of the topology never runs.
   */
  @Test
  public void aPollOutOfTimeYieldsBeforeReachingItsElementBound() {
    // A turn that is out of time before it starts, so what stops it can only be the time bound.
    int elements = elementsInOneTurn(pipelineWithPollBounds(1_000, 0));

    assertThat(
        "the source should have yielded, not run to its element bound",
        elements,
        is(lessThan(1_000)));
    assertThat("the source should still have made progress", elements, is(greaterThan(0)));
  }

  /** The time bound only cuts a turn short; with time to spare the element bound still applies. */
  @Test
  public void aPollWithTimeToSpareReachesItsElementBound() {
    int elements = elementsInOneTurn(pipelineWithPollBounds(100, 60_000));

    assertThat(
        "a turn with time to spare should read at least a full batch",
        elements,
        is(greaterThan(99)));
  }

  @Test
  public void aSourceThatReachesTheEndOfTimeStopsBeingPolled() {
    // CountingSource.unbounded() with a limit reports the terminal watermark once it has produced
    // its elements, which is a source saying it will yield nothing further. Polling must stop
    // there rather than spinning on a reader that can only return false.
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setMaxBundleSize(ELEMENTS_PER_POLL);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("read", Read.from(CountingSource.unbounded()).withMaxNumRecords(ELEMENTS))
        .apply("record", ParDo.of(new RecordFn()));

    KafkaStreamsTranslationContext context = KafkaStreamsTestRunner.translate(pipeline);
    try (TopologyTestDriver driver =
        new TopologyTestDriver(
            context.getTopology(), KafkaStreamsTestRunner.streamsConfig(pipeline))) {
      for (int turn = 0; turn < 10; turn++) {
        driver.advanceWallClockTime(Duration.ofMillis(100));
      }
    }

    // Every element exactly once: the source finished, and the turns after it finished added
    // nothing.
    assertThat(RECEIVED.size(), is(ELEMENTS));
  }

  /** A source that ignores the requested split count and always returns two parts. */
  private static class TwoSplitSource extends UnboundedSource<Long, CounterMark> {
    private final UnboundedSource<Long, CounterMark> delegate = CountingSource.unbounded();

    @Override
    public List<? extends UnboundedSource<Long, CounterMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return delegate.split(2, options);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable CounterMark checkpointMark) throws IOException {
      return delegate.createReader(options, checkpointMark);
    }

    @Override
    public Coder<CounterMark> getCheckpointMarkCoder() {
      return delegate.getCheckpointMarkCoder();
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return delegate.getOutputCoder();
    }
  }

  @Test
  public void aSourceThatSplitsIntoSeveralPartsIsRejectedRatherThanTruncated() {
    // The count passed to split() is only a hint. Reading the first part of several and ignoring
    // the rest would silently drop their data, so translation has to fail instead.
    Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
    pipeline
        .apply("read", Read.from(new TwoSplitSource()))
        .apply("record", ParDo.of(new RecordFn()));

    try {
      KafkaStreamsTestRunner.translate(pipeline);
      throw new AssertionError("expected a multi-split source to be rejected");
    } catch (UnsupportedOperationException e) {
      assertThat(e.getMessage(), containsString("split into 2 parts"));
      assertThat(e.getMessage(), containsString("drop the data"));
    }
  }

  @Test
  public void theSourceIsTranslatedAsAnUnboundedRead() {
    // The bounded and unbounded reads share a URN and are told apart by the payload, so this pins
    // down that the pipeline really did take the unbounded path.
    KafkaStreamsTranslationContext context = KafkaStreamsTestRunner.translate(unboundedPipeline());

    boolean hasReadProcessor =
        context.getTopology().describe().subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .anyMatch(node -> node.name().contains("read"));
    assertThat(hasReadProcessor, is(true));
  }
}
