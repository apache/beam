package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main DoFn for processing ordered events.
 *
 * @param <EventTypeT>
 * @param <EventKeyTypeT>
 * @param <StateTypeT>
 */
abstract class ProcessorDoFn<
    EventTypeT,
    EventKeyTypeT,
    ResultTypeT,
    StateTypeT extends MutableState<EventTypeT, ResultTypeT>>
    extends DoFn<KV<EventKeyTypeT, KV<Long, EventTypeT>>, KV<EventKeyTypeT, ResultTypeT>> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessorDoFn.class);

  protected static final String PROCESSING_STATE = "processingState";
  protected static final String MUTABLE_STATE = "mutableState";

  protected static final String STATUS_EMISSION_TIMER = "statusTimer";
  protected static final String WINDOW_CLOSED = "windowClosed";
  protected final EventExaminer<EventTypeT, StateTypeT> eventExaminer;

  private final TupleTag<KV<EventKeyTypeT, OrderedProcessingStatus>> statusTupleTag;
  protected final Duration statusUpdateFrequency;

  protected final TupleTag<KV<EventKeyTypeT, ResultTypeT>> mainOutputTupleTag;
  protected final TupleTag<KV<EventKeyTypeT, KV<Long, UnprocessedEvent<EventTypeT>>>>
      unprocessedEventsTupleTag;
  private final boolean produceStatusUpdateOnEveryEvent;

  private final long maxNumberOfResultsToProduce;


  protected @Nullable Long numberOfResultsBeforeBundleStart = Long.valueOf(0);

  /**
   * Stateful DoFn to do the bulk of processing.
   *
   * @param eventExaminer
   * @param mainOutputTupleTag
   * @param statusTupleTag
   * @param statusUpdateFrequency
   * @param unprocessedEventTupleTag
   * @param produceStatusUpdateOnEveryEvent
   * @param maxNumberOfResultsToProduce
   */
  ProcessorDoFn(
      EventExaminer<EventTypeT, StateTypeT> eventExaminer,
      TupleTag<KV<EventKeyTypeT, ResultTypeT>> mainOutputTupleTag,
      TupleTag<KV<EventKeyTypeT, OrderedProcessingStatus>> statusTupleTag,
      Duration statusUpdateFrequency,
      TupleTag<KV<EventKeyTypeT, KV<Long, UnprocessedEvent<EventTypeT>>>>
          unprocessedEventTupleTag,
      boolean produceStatusUpdateOnEveryEvent,
      long maxNumberOfResultsToProduce) {
    this.eventExaminer = eventExaminer;

    this.mainOutputTupleTag = mainOutputTupleTag;
    this.statusTupleTag = statusTupleTag;
    this.unprocessedEventsTupleTag = unprocessedEventTupleTag;
    this.statusUpdateFrequency = statusUpdateFrequency;
    this.produceStatusUpdateOnEveryEvent = produceStatusUpdateOnEveryEvent;
    this.maxNumberOfResultsToProduce = maxNumberOfResultsToProduce;
  }

  @StartBundle
  public void onBundleStart() {
    numberOfResultsBeforeBundleStart = null;
  }

  @FinishBundle
  public void onBundleFinish() {
    // This might be necessary because this field is also used in a Timer
    numberOfResultsBeforeBundleStart = null;
  }


  protected void saveStates(
      ValueState<ProcessingState<EventKeyTypeT>> processingStatusState,
      ProcessingState<EventKeyTypeT> processingStatus,
      ValueState<StateTypeT> currentStateState,
      @Nullable StateTypeT state,
      MultiOutputReceiver outputReceiver,
      Instant windowTimestamp) {
    // There is always a change to the processing status
    processingStatusState.write(processingStatus);

    // Stored state may not have changes if the element was out of sequence.
    if (state != null) {
      currentStateState.write(state);
    }

    if (produceStatusUpdateOnEveryEvent) {
      // During pipeline draining the window timestamp is set to a large value in the future.
      // Producing an event before that results in error, that's why this logic exist.
      Instant statusTimestamp = windowTimestamp;

      emitProcessingStatus(processingStatus, outputReceiver, statusTimestamp);
    }
  }

  protected void emitProcessingStatus(
      ProcessingState<EventKeyTypeT> processingState,
      MultiOutputReceiver outputReceiver,
      Instant statusTimestamp) {
    outputReceiver
        .get(statusTupleTag)
        .outputWithTimestamp(
            KV.of(
                processingState.getKey(),
                OrderedProcessingStatus.create(
                    processingState.getLastOutputSequence(),
                    processingState.getBufferedEventCount(),
                    processingState.getEarliestBufferedSequence(),
                    processingState.getLatestBufferedSequence(),
                    processingState.getEventsReceived(),
                    processingState.getResultCount(),
                    processingState.getDuplicates(),
                    processingState.isLastEventReceived())),
            statusTimestamp);
  }


  protected boolean reachedMaxResultCountForBundle(
      ProcessingState<EventKeyTypeT> processingState, Timer largeBatchEmissionTimer) {
    boolean exceeded =
        processingState.resultsProducedInBundle(
            numberOfResultsBeforeBundleStart == null ? 0
                : numberOfResultsBeforeBundleStart.longValue())
            >= maxNumberOfResultsToProduce;
    if (exceeded) {
      LOG.info(
          "Setting the timer to output next batch of events for key '"
              + processingState.getKey()
              + "'");
      // See GroupIntoBatches for examples on how to hold the timestamp.
      // TODO: test that on draining the pipeline all the results are still produced correctly.
      // See: https://github.com/apache/beam/issues/30781
      largeBatchEmissionTimer.offset(Duration.millis(1)).setRelative();
    }
    return exceeded;
  }
}
