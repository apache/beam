package org.apache.beam.sdk.extensions.ordered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.ordered.StringBufferOrderedProcessingHandler.StringBufferOrderedProcessingWithGlobalSequenceHandler;
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

public class OrderedEventProcessorGlobalSequenceTest extends OrderedEventProcessorTestBase {

  public static final boolean GLOBAL_SEQUENCE = true;

  static {
    Logger logger = Logger.getLogger(GlobalSequencesProcessorDoFn.class.getName());
    logger.setLevel(Level.FINEST);
  }

  @org.junit.Test
  public void testPerfectOrderingProcessing() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(0, "id-1", "a"),
        Event.create(1, "id-1", "b"),
        Event.create(2, "id-1", "c"),
        Event.create(3, "id-1", "d"),
        Event.create(4, "id-2", "a"),
        Event.create(5, "id-2", "b")
    };

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));
    expectedOutput.add(KV.of("id-1", "abcd"));
    expectedOutput.add(KV.of("id-2", "a"));
    expectedOutput.add(KV.of("id-2", "ab"));

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 5, new Instant()));
  }

  @Test
  public void testOutOfSequenceProcessing() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(2, "id-1", "c"),
        Event.create(1, "id-1", "b"),
        Event.create(0, "id-1", "a"),
        Event.create(3, "id-1", "d"),
        Event.create(5, "id-2", "b"),
        Event.create(6, "id-2", "c"),
        Event.create(8, "id-2", "e"),
        Event.create(4, "id-2", "a"),
        Event.create(7, "id-2", "d")
    };

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

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 8, new Instant()));
  }

  @Test
  public void testHandlingOfDuplicateSequences() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(3, "id-1", "d"),
        Event.create(2, "id-1", "c"),

        // Duplicates
        Event.create(3, "id-1", "d"),
        Event.create(3, "id-1", "d"),

        Event.create(0, "id-1", "a"),
        Event.create(1, "id-1", "b"),

        // Additional duplicates
        Event.create(1, "id-1", "b"),
        Event.create(3, "id-1", "d"),
    };

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

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        duplicates,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 3, new Instant()));
  }

  @Test
  public void testTreatingSequencesBelowInitialAsDuplicates() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(3, "id-1", "d"),
        Event.create(2, "id-1", "c"),

        // Earlier events
        Event.create(-1, "id-1", "early-1"),
        Event.create(-2, "id-1", "early-2"),

        Event.create(0, "id-1", "a"),
        Event.create(1, "id-1", "b")
    };

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));
    expectedOutput.add(KV.of("id-1", "abcd"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> duplicates = new ArrayList<>();
    duplicates.add(KV.of("id-1", KV.of(-1L, UnprocessedEvent.create("early-1", Reason.duplicate))));
    duplicates.add(KV.of("id-1", KV.of(-2L, UnprocessedEvent.create("early-2", Reason.duplicate))));

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        duplicates,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 3, new Instant()));
  }

  @Test
  public void testHandlingOfCheckedExceptions() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(0, "id-1", "a"),
        Event.create(1, "id-1", "b"),
        Event.create(2, "id-1", StringBuilderState.BAD_VALUE),
        Event.create(3, "id-1", "c"),
    };

    // This is an interesting case - even though event #2 is not processed it doesn't affect
    // the global sequence calculations. It is not considered a gap, and all the subsequent
    // events will be processed.
    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> failedEvents = new ArrayList<>();
    failedEvents.add(
        KV.of(
            "id-1",
            KV.of(
                2L,
                UnprocessedEvent.create(StringBuilderState.BAD_VALUE, Reason.exception_thrown))));

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        failedEvents,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        // Sequence matcher doesn't know if the element is valid or not.
        // That's why the elements that are get rejected in the processor still count  when
        // calculating the global sequence
        ContiguousSequenceRange.of(0, 3, new Instant()));
  }

  @Test
  public void testProcessingWithEveryOtherResultEmission() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(2, "id-1", "c"),
        Event.create(1, "id-1", "b"),
        Event.create(0, "id-1", "a"),
        Event.create(3, "id-1", "d"),
        Event.create(4, "id-2", "a"),
        Event.create(5, "id-2", "b"),
    };

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    //  Skipped        KV.of("id-1", "ab"),
    expectedOutput.add(KV.of("id-1", "abc"));
    //  Skipped        KV.of("id-1", "abcd"),
    expectedOutput.add(KV.of("id-2", "a"));
    //  Skipped        KV.of("id-2", "ab")
    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_OTHER_EVENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 5, new Instant()));
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

    StringBuilder output = new StringBuilder();
    String outputPerElement = ".";
    String key = "id-1";

    for (long sequence : sequences) {
      events.add(Event.create(sequence, key, outputPerElement));
      output.append(outputPerElement);
      expectedOutput.add(KV.of(key, output.toString()));

    }

    testGlobalSequenceProcessing(
        events.toArray(new Event[events.size()]),
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        1L /* This dataset assumes 1 as the starting sequence */,
        maxResultsPerOutput,
        ContiguousSequenceRange.of(1, sequences.length, new Instant()));
  }

  @Test
  public void testSequenceGapProcessingInBufferedOutput() throws CannotProvideCoderException {
    int maxResultsPerOutput = 3;

    long[] sequences = new long[]{2, 3, 7, 8, 9, 10, 1, 4, 5, 6};

    List<Event> events = new ArrayList<>(sequences.length);
    List<KV<String, String>> expectedOutput = new ArrayList<>(sequences.length);

    String key = "id-1";

    for (long sequence : sequences) {
      events.add(Event.create(sequence, key, sequence + "-"));
    }

    StringBuilder output = new StringBuilder();
    Arrays.stream(sequences).sorted().forEach(sequence -> {
          output.append(sequence + "-");
          expectedOutput.add(KV.of(key, output.toString()));
        }
    );

    testGlobalSequenceProcessing(
        events.toArray(new Event[events.size()]),
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        1L /* This dataset assumes 1 as the starting sequence */,
        maxResultsPerOutput,
        ContiguousSequenceRange.of(1, 10, new Instant()));
  }

  @Test
  public void testHandlingOfMaxSequenceNumber() throws CannotProvideCoderException {
    Event[] events = {
        Event.create(1, "id-1", "b"),
        Event.create(0, "id-1", "a"),
        Event.create(Long.MAX_VALUE, "id-1", "d"),
        Event.create(2, "id-1", "c")
    };

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "abc"));

    Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> unprocessedEvents =
        new ArrayList<>();
    unprocessedEvents.add(
        KV.of(
            "id-1",
            KV.of(
                Long.MAX_VALUE,
                UnprocessedEvent.create("d", Reason.sequence_id_outside_valid_range))));

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        unprocessedEvents,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 2, Instant.now()));
  }

  @Test
  public void testProcessingOfTheLastInput() throws CannotProvideCoderException {
    // TODO: fix the test. Need to see that the resulting status reflects the last input
    Event[] events = {
        Event.create(0, "id-1", "a"),
        Event.create(1, "id-1", "b"),
        Event.create(2, "id-1", StringEventExaminer.LAST_INPUT)
    };

    Collection<KV<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(KV.of("id-1", "a"));
    expectedOutput.add(KV.of("id-1", "ab"));
    expectedOutput.add(KV.of("id-1", "ab" + StringEventExaminer.LAST_INPUT));

    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        EMISSION_FREQUENCY_ON_EVERY_ELEMENT,
        INITIAL_SEQUENCE_OF_0,
        LARGE_MAX_RESULTS_PER_OUTPUT,
        ContiguousSequenceRange.of(0, 2, new Instant()));
  }


  private void testGlobalSequenceProcessing(
      Event[] events,
      Collection<KV<String, String>> expectedOutput,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      ContiguousSequenceRange expectedLastCompleteRange)
      throws CannotProvideCoderException {
    testGlobalSequenceProcessing(
        events,
        expectedOutput,
        NO_EXPECTED_DLQ_EVENTS,
        emissionFrequency,
        initialSequence,
        maxResultsPerOutput, expectedLastCompleteRange);
  }

  private void testGlobalSequenceProcessing(
      Event[] events,
      Collection<KV<String, String>> expectedOutput,
      Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> expectedUnprocessedEvents,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      ContiguousSequenceRange expectedLastCompleteRange)
      throws CannotProvideCoderException {
    // Test a streaming pipeline
    if (false) {
      doTest(
          events,
          null /* expectedStatuses */,
          expectedOutput,
          expectedUnprocessedEvents,
          emissionFrequency,
          initialSequence,
          maxResultsPerOutput,
          false /* produceStatusOnEveryEvent */,
          STREAMING,
          GLOBAL_SEQUENCE, expectedLastCompleteRange);
    }

    // Test a batch pipeline
    if (runTestsOnDataflowRunner()) {
      doTest(
          events,
          null /* expectedStatuses */,
          expectedOutput,
          expectedUnprocessedEvents,
          emissionFrequency,
          initialSequence,
          maxResultsPerOutput,
          false /* produceStatusOnEveryEvent */,
          BATCH,
          GLOBAL_SEQUENCE,
          expectedLastCompleteRange);
    } else {
      System.err.println(
          "Warning - batch tests didn't run. "
              + "DirectRunner doesn't work correctly with this transform in batch mode."
              + "Run the tests using Dataflow runner to validate.");
    }
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
            .advanceProcessingTime(Duration.standardMinutes(15))
            .advanceWatermarkToInfinity();

    Pipeline pipeline = streamingPipeline;

    PCollection<Event> rawInput = pipeline.apply("Create Streaming Events", values);
    PCollection<KV<String, KV<Long, String>>> input =
        rawInput.apply("To KV", ParDo.of(new MapEventsToKV()));

    input = input.apply("Window input", Window.into(FixedWindows.of(Duration.standardSeconds(5))));

    StringBufferOrderedProcessingWithGlobalSequenceHandler handler =
        new StringBufferOrderedProcessingWithGlobalSequenceHandler(
            EMISSION_FREQUENCY_ON_EVERY_ELEMENT, INITIAL_SEQUENCE_OF_0);
    handler.setMaxOutputElementsPerBundle(LARGE_MAX_RESULTS_PER_OUTPUT);
    handler.setStatusUpdateFrequency(null);
    handler.setProduceStatusUpdateOnEveryEvent(false);

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

    // TODO: can we make the status assertions work?
//    PAssert.that("Statuses match in window 1", processingResult.processingStatuses())
//        .inWindow(window1)
//        .containsInAnyOrder(
////            KV.of("id-1", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
//            KV.of("id-1", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)),
////            KV.of("id-2", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
////            KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)),
//            KV.of("id-2", OrderedProcessingStatus.create(2L, 0, null, null, 3, 3, 0, false))
//        );

//    PAssert.that("Statuses match in window 2", processingResult.processingStatuses())
//        .inWindow(window2)
//        .containsInAnyOrder(
//            KV.of("id-1", OrderedProcessingStatus.create(0L, 0, null, null, 1, 1, 0, false)),
//            KV.of("id-1", OrderedProcessingStatus.create(1L, 0, null, null, 2, 2, 0, false)));

    PAssert.that("Unprocessed events match", processingResult.unprocessedEvents())
        .containsInAnyOrder(NO_EXPECTED_DLQ_EVENTS);

    pipeline.run();
  }
}
