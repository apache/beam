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

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.ordered.StringBufferOrderedProcessingHandler.StringBufferOrderedProcessingWithGlobalSequenceHandler;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.Reason;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;

/**
 * Ordered Processing tests use the same testing scenario. Events are sent in or out of sequence.
 * Each event is a string for a particular key. The output is a concatenation of all strings.
 */
public class OrderedEventProcessorTestBase {

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

  protected boolean runTestsOnDataflowRunner() {
    return Boolean.getBoolean("run-tests-on-dataflow");
  }

  protected String getSystemProperty(String name) {
    String property = System.getProperty(name);
    if (property == null) {
      throw new IllegalStateException("Unable to find system property '" + name + "'");
    }
    return property;
  }

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

  /**
   * The majority of the tests use this method. Testing is done in the global window.
   *
   * @throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException
   */
  protected void doTest(
      Event[] events,
      @Nullable Collection<KV<String, OrderedProcessingStatus>> expectedStatuses,
      Collection<KV<String, String>> expectedOutput,
      Collection<KV<String, KV<Long, UnprocessedEvent<String>>>> expectedUnprocessedEvents,
      int emissionFrequency,
      long initialSequence,
      int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent,
      boolean streaming,
      boolean isGlobalSequence,
      @Nullable ContiguousSequenceRange expectedLastCompletedSequence)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {

    Pipeline pipeline = streaming ? streamingPipeline : batchPipeline;
    if (runTestsOnDataflowRunner()) {
      pipeline.getOptions().setRunner(TestDataflowRunner.class);
      TestDataflowPipelineOptions options =
          pipeline.getOptions().as(TestDataflowPipelineOptions.class);
      options.setExperiments(Arrays.asList("disable_runner_v2"));
      options.setTempRoot("gs://" + getSystemProperty("temp_dataflow_bucket"));
    }
    PCollection<Event> rawInput =
        streaming
            ? createStreamingPCollection(pipeline, events)
            : createBatchPCollection(pipeline, events);
    PCollection<KV<String, KV<Long, String>>> input =
        rawInput.apply("To KV", ParDo.of(new MapEventsToKV()));

    OrderedProcessingHandler<String, String, StringBuilderState, String> handler =
        isGlobalSequence
            ? new StringBufferOrderedProcessingWithGlobalSequenceHandler(
                emissionFrequency, initialSequence)
            : new StringBufferOrderedProcessingHandler(emissionFrequency, initialSequence);
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

    if (streaming && expectedStatuses != null) {
      // Only in a streaming pipeline the events will arrive in a pre-determined order and the
      // statuses
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

    if (expectedLastCompletedSequence != null && processingResult.latestContiguousRange() != null) {
      PCollection<ContiguousSequenceRange> globalSequences =
          rawInput.apply(
              "Publish Global Sequences",
              new GlobalSequenceRangePublisher(
                  processingResult.latestContiguousRange(),
                  handler.getKeyCoder(pipeline, input.getCoder()),
                  handler.getEventCoder(pipeline, input.getCoder())));
      PAssert.that("CompletedSequenceRange verification", globalSequences)
          .satisfies(new LastExpectedGlobalSequenceRangeMatcher(expectedLastCompletedSequence));
    }
    pipeline.run();
  }

  static class LastExpectedGlobalSequenceRangeMatcher
      implements SerializableFunction<Iterable<ContiguousSequenceRange>, Void> {

    private final long expectedStart;
    private final long expectedEnd;

    LastExpectedGlobalSequenceRangeMatcher(ContiguousSequenceRange expected) {
      this.expectedStart = expected.getStart();
      this.expectedEnd = expected.getEnd();
    }

    @Override
    public Void apply(Iterable<ContiguousSequenceRange> input) {
      StringBuilder listOfRanges = new StringBuilder("[");
      Iterator<ContiguousSequenceRange> iterator = input.iterator();
      ContiguousSequenceRange lastRange = null;
      while (iterator.hasNext()) {
        lastRange = iterator.next();

        if (listOfRanges.length() > 1) {
          listOfRanges.append(", ");
        }
        listOfRanges.append(lastRange);
      }
      listOfRanges.append(']');
      boolean foundExpectedRange =
          lastRange != null
              && lastRange.getStart() == expectedStart
              && lastRange.getEnd() == expectedEnd;

      assertThat(
          "Expected range not found: ["
              + expectedStart
              + '-'
              + expectedEnd
              + "], received ranges: "
              + listOfRanges,
          foundExpectedRange);
      return null;
    }
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

  static class GlobalSequenceRangePublisher
      extends PTransform<PCollection<Event>, PCollection<ContiguousSequenceRange>> {

    private final PCollectionView<ContiguousSequenceRange> lastCompletedSequenceRangeView;
    private final Coder<String> keyCoder;
    private final Coder<String> eventCoder;

    public GlobalSequenceRangePublisher(
        PCollectionView<ContiguousSequenceRange> latestCompletedSequenceRange,
        Coder<String> keyCoder,
        Coder<String> eventCoder) {
      this.lastCompletedSequenceRangeView = latestCompletedSequenceRange;
      this.keyCoder = keyCoder;
      this.eventCoder = eventCoder;
    }

    @Override
    public PCollection<ContiguousSequenceRange> expand(PCollection<Event> input) {
      PCollection<KV<String, KV<Long, String>>> events =
          input
              // In production pipelines the global sequence will typically be obtained
              // by using GenerateSequence. But GenerateSequence doesn't work well with TestStream,
              // That's why we use the input events here.
              //              .apply("Create Ticker",
              //                  GenerateSequence.from(0).to(2).withRate(1,
              // Duration.standardSeconds(5)))
              .apply("To KV", ParDo.of(new MapEventsToKV()));
      if (input.isBounded() == IsBounded.BOUNDED) {
        return events.apply(
            "Emit SideInput",
            ParDo.of(new SideInputEmitter())
                .withSideInput("lastCompletedSequence", lastCompletedSequenceRangeView));
      } else {
        PCollection<KV<String, KV<Long, String>>> tickers =
            events.apply(
                "Create Tickers",
                new PerKeyTickerGenerator<>(keyCoder, eventCoder, Duration.standardSeconds(1)));
        return tickers.apply(
            "Emit SideInput",
            ParDo.of(new SideInputEmitter())
                .withSideInput("lastCompletedSequence", lastCompletedSequenceRangeView));
      }
    }

    static class SideInputEmitter
        extends DoFn<KV<String, KV<Long, String>>, ContiguousSequenceRange> {

      @ProcessElement
      public void produceCompletedRange(
          @SideInput("lastCompletedSequence") ContiguousSequenceRange sideInput,
          OutputReceiver<ContiguousSequenceRange> outputReceiver) {
        outputReceiver.output(sideInput);
      }
    }
  }
}
