package org.apache.beam.sdk.extensions.ordered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.Reason;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class OrderedEventProcessorPerKeySequenceTest extends OrderedEventProcessorTestBase {

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

    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(events, expectedStatuses, expectedOutput, 1, 0, 1000, false);
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

    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(
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
    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(
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

    long[] sequences = new long[]{2, 3, 7, 8, 9, 10, 1, 4, 5, 6};

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

    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(
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

    testPerKeySequenceProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        DONT_PRODUCE_STATUS_ON_EVERY_EVENT);
  }

  protected void testPerKeySequenceProcessing(
      Event[] events,
      Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent)
      throws CannotProvideCoderException {
    testPerKeySequenceProcessing(
        events,
        expectedStatuses,
        expectedOutput,
        NO_EXPECTED_DLQ_EVENTS,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent);
  }

  protected void testPerKeySequenceProcessing(
      Event[] events,
      Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> expectedUnprocessedEvents,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent)
      throws CannotProvideCoderException {
    // Test a streaming pipeline
    doTest(
        events,
        expectedStatuses,
        expectedOutput,
        expectedUnprocessedEvents,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent,
        STREAMING,
        false, CompletedSequenceRange.EMPTY);

    // Test a batch pipeline
    doTest(
        events,
        expectedStatuses,
        expectedOutput,
        expectedUnprocessedEvents,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput,
        produceStatusOnEveryEvent,
        BATCH,
        false, CompletedSequenceRange.EMPTY);
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
}
