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
package org.apache.beam.sdk.extensions.ordered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.Reason;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Ordered Processing tests use the same testing scenario. Events are sent in or out of sequence.
 * Each event is a string for a particular key. The output is a concatenation of all strings.
 */
@RunWith(JUnit4.class)
public class OrderedEventProcessorTest {

  public static final boolean LAST_EVENT_RECEIVED = true;
  public static final int EMISSION_FREQUENCY_ON_EVERY_ELEMENT = 1;
  public static final int INITIAL_SEQUENCE_OF_0 = 0;
  public static final boolean DONT_PRODUCE_STATUS_ON_EVERY_EVENT = false;
  public static final int LARGE_MAX_RESULTS_PER_OUTPUT = 1000;
  public static final int EMISSION_FREQUENCY_ON_EVERY_OTHER_EVENT = 2;
  public static final boolean PRODUCE_STATUS_ON_EVERY_EVENT = true;
  public static final boolean STREAMING = true;
  public static final boolean BATCH = false;
  public static final Set<KV<String, KV<Long, UnprocessedEvent<String>>>> NO_EXPECTED_DLQ_EVENTS =
      Collections.emptySet();
  @Rule public final transient TestPipeline streamingPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline batchPipeline = TestPipeline.create();

  static class MapEventsToKV extends DoFn<Event, KV<String, KV<Long, String>>> {

    @ProcessElement
    public void convert(
        @Element Event event, OutputReceiver<KV<String, KV<Long, String>>> outputReceiver) {
      outputReceiver.output(KV.of(event.getKey(), KV.of(event.getSequence(), event.getValue())));
    }
  }

  static class MapStringBufferStateToString
      extends DoFn<KV<String, StringBuilderState>, KV<String, String>> {

    @ProcessElement
    public void map(
        @Element KV<String, StringBuilderState> element,
        OutputReceiver<KV<String, String>> outputReceiver) {
      outputReceiver.output(KV.of(element.getKey(), element.getValue().toString()));
    }
  }

  @Test
  public void testPerfectOrderingProcessing() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(0, "id-1", "a"),
      Event.create(1, "id-1", "b"),
      Event.create(2, "id-1", "c"),
      Event.create(3, "id-1", "d"),
      Event.create(0, "id-2", "a"),
      Event.create(1, "id-2", "b")
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of(
            "id-1",
            OrderedProcessingStatus.create(
                3L,
                0,
                null,
                null,
                4,
                Arrays.stream(events).filter(e -> e.getKey().equals("id-1")).count(),
                0,
                false)));
    expectedStatuses.add(
        KV.of(
            "id-2",
            OrderedProcessingStatus.create(
                1L,
                0,
                null,
                null,
                2,
                Arrays.stream(events).filter(e -> e.getKey().equals("id-2")).count(),
                0,
                false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));
    expectedOutput.add(KV.of("id-1", "abcd"));
    expectedOutput.add(KV.of("id-2", "a"));
    expectedOutput.add(KV.of("id-2", "ab"));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testOutOfSequenceProcessing() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(2, "id-1", "c"),
      Event.create(1, "id-1", "b"),
      Event.create(0, "id-1", "a"),
      Event.create(3, "id-1", "d"),
      Event.create(1, "id-2", "b"),
      Event.create(2, "id-2", "c"),
      Event.create(4, "id-2", "e"),
      Event.create(0, "id-2", "a"),
      Event.create(3, "id-2", "d")
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of(
            "id-1",
            OrderedProcessingStatus.create(
                3L,
                0,
                null,
                null,
                4,
                Arrays.stream(events).filter(e -> e.getKey().equals("id-1")).count(),
                0,
                false)));
    expectedStatuses.add(
        KV.of(
            "id-2",
            OrderedProcessingStatus.create(
                4L,
                0,
                null,
                null,
                5,
                Arrays.stream(events).filter(e -> e.getKey().equals("id-2")).count(),
                0,
                false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));
    expectedOutput.add(KV.of("id-1", "abcd"));
    expectedOutput.add(KV.of("id-2", "a"));
    expectedOutput.add(KV.of("id-2", "ab"));
    expectedOutput.add(KV.of("id-2", "abc"));
    expectedOutput.add(KV.of("id-2", "abcd"));
    expectedOutput.add(KV.of("id-2", "abcde"));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testUnfinishedProcessing() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(2, "id-1", "c"),
      //   Excluded                     Event.create(1, "id-1", "b"),
      Event.create(0, "id-1", "a"),
      Event.create(3, "id-1", "d"),
      Event.create(0, "id-2", "a"),
      Event.create(1, "id-2", "b"),
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of("id-1", OrderedProcessingStatus.create(0L, 2, 2L, 3L, 3, 1L, 0, false)));
    expectedStatuses.add(
        KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2L, 0, false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-2", "a"));
    expectedOutput.add(KV.of("id-2", "ab"));

    testProcessing(events, expectedStatuses, expectedOutput, 1, 0, 1000, false);
  }

  @Test
  public void testHandlingOfDuplicateSequences() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(3, "id-1", "d"),
      Event.create(2, "id-1", "c"),
      // Duplicates to be buffered
      Event.create(3, "id-1", "d"),
      Event.create(3, "id-1", "d"),
      Event.create(0, "id-1", "a"),
      Event.create(1, "id-1", "b"),

      // Duplicates after the events are processed
      Event.create(1, "id-1", "b"),
      Event.create(3, "id-1", "d"),
    };
    int resultCount = 4;
    int duplicateCount = 4;

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of(
            "id-1",
            OrderedProcessingStatus.create(
                3L, 0, null, null, events.length, resultCount, duplicateCount, false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));
    expectedOutput.add(KV.of("id-1", "abcd"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> duplicates = new ArrayList<>();
    duplicates.add(KV.of("id-1", KV.of(3L, UnprocessedEvent.create("d", Reason.duplicate))));
    duplicates.add(KV.of("id-1", KV.of(3L, UnprocessedEvent.create("d", Reason.duplicate))));
    duplicates.add(KV.of("id-1", KV.of(1L, UnprocessedEvent.create("b", Reason.duplicate))));
    duplicates.add(KV.of("id-1", KV.of(3L, UnprocessedEvent.create("d", Reason.duplicate))));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        duplicates,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testHandlingOfCheckedExceptions() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(0, "id-1", "a"),
      Event.create(1, "id-1", "b"),
      Event.create(2, "id-1", StringBuilderState.BAD_VALUE),
      Event.create(3, "id-1", "c"),
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of("id-1", OrderedProcessingStatus.create(1L, 1, 3L, 3L, events.length, 2, 0, false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> failedEvents = new ArrayList<>();
    failedEvents.add(
        KV.of(
            "id-1",
            KV.of(
                2L,
                UnprocessedEvent.create(StringBuilderState.BAD_VALUE, Reason.exception_thrown))));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        failedEvents,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testProcessingWithEveryOtherResultEmission() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(2, "id-1", "c"),
      Event.create(1, "id-1", "b"),
      Event.create(0, "id-1", "a"),
      Event.create(3, "id-1", "d"),
      Event.create(0, "id-2", "a"),
      Event.create(1, "id-2", "b"),
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of("id-1", OrderedProcessingStatus.create(3L, 0, null, null, 4, 2L, 0, false)));
    expectedStatuses.add(
        KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 1L, 0, false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    //  Skipped        KV.of("id-1", "ab"),
    expectedOutput.add(KV.of("id-1", "abc"));
    //  Skipped        KV.of("id-1", "abcd"),
    expectedOutput.add(KV.of("id-2", "a"));
    //  Skipped        KV.of("id-2", "ab")
    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_OTHER_EVENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testLargeBufferedOutputInTimer() throws CannotProvideCoderException {
    int maxResultsPerOutput = 100;

    // Array of sequences starting with 2 and the last element - 1.
    // Output will be buffered until the last event arrives
    long[] sequences = new long[maxResultsPerOutput * 3];
    for (int i = 0; i < sequences.length - 1; i++) {
      sequences[i] = i + 2L;
    }
    sequences[sequences.length - 1] = 1;

    List<Event> events = new ArrayList<>(sequences.length);
    Collection<KV<String, String>> expectedOutput = new ArrayList<>(sequences.length);
    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses =
        new ArrayList<>(sequences.length + 10);

    StringBuilder output = new StringBuilder();
    String outputPerElement = ".";
    String key = "id-1";

    int bufferedEventCount = 0;

    for (long sequence : sequences) {
      ++bufferedEventCount;

      events.add(Event.create(sequence, key, outputPerElement));
      output.append(outputPerElement);
      expectedOutput.add(KV.of(key, output.toString()));

      if (bufferedEventCount < sequences.length) {
        // Last event will result in a batch of events being produced. That's why it's excluded
        // here.
        expectedStatuses.add(
            KV.of(
                key,
                OrderedProcessingStatus.create(
                    null, bufferedEventCount, 2L, sequence, bufferedEventCount, 0L, 0, false)));
      }
    }

    // Statuses produced by the batched processing
    for (int i = maxResultsPerOutput; i < sequences.length; i += maxResultsPerOutput) {
      long lastOutputSequence = i;
      expectedStatuses.add(
          KV.of(
              key,
              OrderedProcessingStatus.create(
                  lastOutputSequence,
                  sequences.length - lastOutputSequence,
                  lastOutputSequence + 1,
                  (long) sequences.length,
                  sequences.length,
                  lastOutputSequence,
                  0,
                  false)));
    }

    // -- Final status - indicates that everything has been fully processed
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                (long) sequences.length,
                0,
                null,
                null,
                sequences.length,
                sequences.length,
                0,
                false)));

    testProcessing(
        events.toArray(new Event[events.size()]),
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        1L /* This dataset assumes 1 as the starting sequence */,
        maxResultsPerOutput,
        PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testSequenceGapProcessingInBufferedOutput() throws CannotProvideCoderException {
    int maxResultsPerOutput = 3;

    long[] sequences = new long[] {2, 3, 7, 8, 9, 10, 1, 4, 5, 6};

    List<Event> events = new ArrayList<>(sequences.length);
    List<KV<String, String>> expectedOutput = new ArrayList<>(sequences.length);

    StringBuilder output = new StringBuilder();
    String outputPerElement = ".";
    String key = "id-1";

    for (long sequence : sequences) {
      events.add(Event.create(sequence, key, outputPerElement));
      output.append(outputPerElement);
      expectedOutput.add(KV.of(key, output.toString()));
    }

    int numberOfReceivedEvents = 0;
    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();

    // First elements are out-of-sequence and they just get buffered. Earliest and latest sequence
    // numbers keep changing.
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 1, 2L, 2L, ++numberOfReceivedEvents, 0L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 2, 2L, 3L, ++numberOfReceivedEvents, 0L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 3, 2L, 7L, ++numberOfReceivedEvents, 0L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 4, 2L, 8L, ++numberOfReceivedEvents, 0L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 5, 2L, 9L, ++numberOfReceivedEvents, 0L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                null, 6, 2L, 10L, ++numberOfReceivedEvents, 0L, 0, false)));
    // --- 1 has appeared and caused the batch to be sent out.
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                3L, 4, 7L, 10L, ++numberOfReceivedEvents, 3L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                4L, 4, 7L, 10L, ++numberOfReceivedEvents, 4L, 0, false)));
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                5L, 4, 7L, 10L, ++numberOfReceivedEvents, 5L, 0, false)));
    // --- 6 came and 6, 7, and 8 got output
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                8L, 2, 9L, 10L, ++numberOfReceivedEvents, 8L, 0, false)));
    // Last timer run produces the final status. Number of received events doesn't
    // increase,
    // this is the result of a timer processing
    expectedStatuses.add(
        KV.of(
            key,
            OrderedProcessingStatus.create(
                10L, 0, null, null, numberOfReceivedEvents, 10L, 0, false)));

    testProcessing(
        events.toArray(new Event[events.size()]),
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        1L /* This dataset assumes 1 as the starting sequence */,
        maxResultsPerOutput,
        PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testHandlingOfMaxSequenceNumber() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(0, "id-1", "a"),
      Event.create(1, "id-1", "b"),
      Event.create(Long.MAX_VALUE, "id-1", "c")
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of("id-1", OrderedProcessingStatus.create(1L, 0, null, null, 3, 2, 0, false)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> unprocessedEvents =
        new ArrayList<>();
    unprocessedEvents.add(
        KV.of(
            "id-1",
            KV.of(
                Long.MAX_VALUE,
                UnprocessedEvent.create("c", Reason.sequence_id_outside_valid_range))));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        unprocessedEvents,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testProcessingOfTheLastInput() throws CannotProvideCoderException {
    Event[] events = {
      Event.create(0, "id-1", "a"),
      Event.create(1, "id-1", "b"),
      Event.create(2, "id-1", StringEventExaminer.LAST_INPUT)
    };

    Collection<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>();
    expectedStatuses.add(
        KV.of(
            "id-1",
            OrderedProcessingStatus.create(
                2L, 0, null, null, events.length, events.length, 0, LAST_EVENT_RECEIVED)));

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "ab" + StringEventExaminer.LAST_INPUT));

    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  @Test
  public void testWindowedProcessing() throws CannotProvideCoderException {

    Instant base = new Instant(0);
    TestStream<Event> values =
        TestStream.create(streamingPipeline.getCoderRegistry().getCoder(Event.class))
            .advanceWatermarkTo(base)
            .addElements(
                // Start of first window
                TimestampedValue.of(
                    Event.create(0, "id-1", "a"), base.plus(Duration.standardSeconds(1))),
                TimestampedValue.of(
                    Event.create(1, "id-1", "b"), base.plus(Duration.standardSeconds(2))),
                TimestampedValue.of(
                    Event.create(0, "id-2", "x"), base.plus(Duration.standardSeconds(1))),
                TimestampedValue.of(
                    Event.create(1, "id-2", "y"), base.plus(Duration.standardSeconds(2))),
                TimestampedValue.of(
                    Event.create(2, "id-2", "z"), base.plus(Duration.standardSeconds(2))),

                // Start of second window. Numbering must start with 0 again.
                TimestampedValue.of(
                    Event.create(0, "id-1", "c"), base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of(
                    Event.create(1, "id-1", "d"), base.plus(Duration.standardSeconds(11))))
            .advanceWatermarkToInfinity();

    Pipeline pipeline = streamingPipeline;

    PCollection<Event> rawInput = pipeline.apply("Create Streaming Events", values);
    PCollection<KV<String, KV<Long, String>>> input =
        rawInput.apply("To KV", ParDo.of(new MapEventsToKV()));

    input = input.apply("Window input", Window.into(FixedWindows.of(Duration.standardSeconds(5))));

    StringBufferOrderedProcessingHandler handler =
        new StringBufferOrderedProcessingHandler(
            EMISSION_FREQUENCY_ON_EVERY_ELEMENT, INITIAL_SEQUENCE_OF_0);
    handler.setMaxOutputElementsPerBundle(LARGE_MAX_RESULTS_PER_OUTPUT);
    handler.setStatusUpdateFrequency(null);
    handler.setProduceStatusUpdateOnEveryEvent(true);

    OrderedEventProcessor<String, String, String, StringBuilderState> orderedEventProcessor =
        OrderedEventProcessor.create(handler);

    OrderedEventProcessorResult<String, String, String> processingResult =
        input.apply("Process Events", orderedEventProcessor);

    IntervalWindow window1 = new IntervalWindow(base, base.plus(Duration.standardSeconds(5)));
    PAssert.that("Output matches in window 1", processingResult.output())
        .inWindow(window1)
        .containsInAnyOrder(
            KV.of("id-1", "a"),
            KV.of("id-1", "ab"),
            KV.of("id-2", "x"),
            KV.of("id-2", "xy"),
            KV.of("id-2", "xyz"));

    IntervalWindow window2 =
        new IntervalWindow(
            base.plus(Duration.standardSeconds(10)), base.plus(Duration.standardSeconds(15)));
    PAssert.that("Output matches in window 2", processingResult.output())
        .inWindow(window2)
        .containsInAnyOrder(KV.of("id-1", "c"), KV.of("id-1", "cd"));

    PAssert.that("Statuses match in window 1", processingResult.processingStatuses())
        .inWindow(window1)
        .containsInAnyOrder(
            KV.of("id-1", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
            KV.of("id-1", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(2L, 0, null, null, 3, 3, 0, false)));

    PAssert.that("Statuses match in window 2", processingResult.processingStatuses())
        .inWindow(window2)
        .containsInAnyOrder(
            KV.of("id-1", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
            KV.of("id-1", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)));

    PAssert.that("Unprocessed events match", processingResult.unprocessedEvents())
        .containsInAnyOrder(NO_EXPECTED_DLQ_EVENTS);

    pipeline.run();
  }

  private void testProcessing(
      Event[] events,
      Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent)
      throws CannotProvideCoderException {
    testProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        NO_EXPECTED_DLQ_EVENTS,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent);
  }

  private void testProcessing(
      Event[] events,
      Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> expectedUnprocessedEvents,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent)
      throws CannotProvideCoderException {
    doTest(
        events,
        expectedStatuses,
        expectedOutput,
        expectedUnprocessedEvents,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent,
        STREAMING);
    doTest(
        events,
        expectedStatuses,
        expectedOutput,
        expectedUnprocessedEvents,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent,
        BATCH);
  }

  /**
   * The majority of the tests use this method. Testing is done in the global window.
   *
   * @param events
   * @param expectedStatuses
   * @param expectedOutput
   * @param expectedUnprocessedEvents
   * @param emissionFrequency
   * @param initialSequence
   * @param maxResultsPerOutput
   * @param produceStatusOnEveryEvent
   * @param streaming
   * @throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException
   */
  private void doTest(
      Event[] events,
      Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> expectedUnprocessedEvents,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent,
      boolean streaming)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {

    Pipeline pipeline = streaming ? streamingPipeline : batchPipeline;

    PCollection<Event> rawInput =
        streaming
            ? createStreamingPCollection(pipeline, events)
            : createBatchPCollection(pipeline, events);
    PCollection<KV<String, KV<Long, String>>> input =
        rawInput.apply("To KV", ParDo.of(new MapEventsToKV()));

    StringBufferOrderedProcessingHandler handler =
        new StringBufferOrderedProcessingHandler(emissionFrequency, initialSequence);
    handler.setMaxOutputElementsPerBundle(maxResultsPerOutput);
    if (produceStatusOnEveryEvent) {
      handler.setProduceStatusUpdateOnEveryEvent(true);
      // This disables status updates emitted on timers.
      handler.setStatusUpdateFrequency(null);
    } else {
      handler.setStatusUpdateFrequency(
          streaming ? Duration.standardMinutes(5) : Duration.standardSeconds(1));
    }
    OrderedEventProcessor<String, String, String, StringBuilderState> orderedEventProcessor =
        OrderedEventProcessor.create(handler);

    OrderedEventProcessorResult<String, String, String> processingResult =
        input.apply("Process Events", orderedEventProcessor);

    PAssert.that("Output matches", processingResult.output()).containsInAnyOrder(expectedOutput);

    if (streaming) {
      // Only in streaming the events will arrive in a pre-determined order and the statuses
      // will be deterministic. In batch pipelines events can be processed in any order,
      // so we skip status verification and rely on the output and unprocessed event matches.
      PAssert.that("Statuses match", processingResult.processingStatuses())
          .containsInAnyOrder(expectedStatuses);
    }

    // This is a temporary workaround until PAssert changes.
    boolean unprocessedEventsHaveExceptionStackTrace = false;
    for (KV<String, KV<Long, UnprocessedEvent<String>>> event : expectedUnprocessedEvents) {
      if (event.getValue().getValue().getReason() == Reason.exception_thrown) {
        unprocessedEventsHaveExceptionStackTrace = true;
        break;
      }
    }

    if (unprocessedEventsHaveExceptionStackTrace) {
      PAssert.thatSingleton(
              "Unprocessed event count",
              processingResult
                  .unprocessedEvents()
                  .apply(
                      "Window",
                      Window.<KV<String, KV<Long, UnprocessedEvent<String>>>>into(
                              new GlobalWindows())
                          .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                          .discardingFiredPanes())
                  .apply("Count", Count.globally()))
          .isEqualTo((long) expectedUnprocessedEvents.size());
    } else {
      PAssert.that("Unprocessed events match", processingResult.unprocessedEvents())
          .containsInAnyOrder(expectedUnprocessedEvents);
    }
    pipeline.run();
  }

  private @UnknownKeyFor @NonNull @Initialized PCollection<Event> createBatchPCollection(
      Pipeline pipeline, Event[] events) {
    return pipeline
        .apply("Create Batch Events", Create.of(Arrays.asList(events)))
        .apply("Reshuffle", Reshuffle.viaRandomKey());
  }

  private @UnknownKeyFor @NonNull @Initialized PCollection<Event> createStreamingPCollection(
      Pipeline pipeline, Event[] events)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
    Instant now = Instant.now().minus(Duration.standardMinutes(20));
    TestStream.Builder<Event> messageFlow =
        TestStream.create(pipeline.getCoderRegistry().getCoder(Event.class))
            .advanceWatermarkTo(now);

    int delayInMilliseconds = 0;
    for (Event e : events) {
      messageFlow =
          messageFlow
              .advanceWatermarkTo(now.plus(Duration.millis(++delayInMilliseconds)))
              .addElements(e);
    }

    // Needed to force the processing time based timers.
    messageFlow = messageFlow.advanceProcessingTime(Duration.standardMinutes(15));
    return pipeline.apply("Create Streaming Events", messageFlow.advanceWatermarkToInfinity());
  }

  /**
   * Unprocessed event's explanation contains stacktraces which makes tests very brittle because it
   * requires hardcoding the line numbers in the code. We use this matcher to only compare on the
   * first line of the explanation.
   */
  static class UnprocessedEventMatcher
      extends BaseMatcher<KV<String, KV<Long, UnprocessedEvent<String>>>>
      implements SerializableMatcher<KV<String, KV<Long, UnprocessedEvent<String>>>> {

    private KV<String, KV<Long, UnprocessedEvent<String>>> element;

    public UnprocessedEventMatcher(KV<String, KV<Long, UnprocessedEvent<String>>> element) {
      this.element = element;
    }

    @Override
    public boolean matches(Object actual) {
      KV<String, KV<Long, UnprocessedEvent<String>>> toMatch =
          (KV<String, KV<Long, UnprocessedEvent<String>>>) actual;

      UnprocessedEvent<String> originalEvent = element.getValue().getValue();
      UnprocessedEvent<String> eventToMatch = toMatch.getValue().getValue();

      return element.getKey().equals(toMatch.getKey())
          && element.getValue().getKey().equals(toMatch.getValue().getKey())
          && originalEvent.getEvent().equals(eventToMatch.getEvent())
          && originalEvent.getReason() == eventToMatch.getReason()
          && normalizeExplanation(originalEvent.getExplanation())
              .equals(normalizeExplanation(eventToMatch.getExplanation()));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Just some text...");
    }

    static String normalizeExplanation(String value) {
      if (value == null) {
        return "";
      }
      String firstLine = value.split("\n", 1)[0];
      if (firstLine.contains("Exception")) {
        return firstLine;
      }
      return value;
    }
  }
}
