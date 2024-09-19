package org.apache.beam.sdk.extensions.ordered;

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.ordered.ProcessingState.ProcessingStateCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SequencePerKeyProcessorDoFn<EventTypeT, EventKeyTypeT, ResultTypeT,
    StateTypeT extends MutableState<EventTypeT, ResultTypeT>>
    extends ProcessorDoFn<EventTypeT, EventKeyTypeT, ResultTypeT, StateTypeT> {

  private static final Logger LOG = LoggerFactory.getLogger(SequencePerKeyProcessorDoFn.class);

  private static final String LARGE_BATCH_EMISSION_TIMER = "largeBatchTimer";
  protected static final String BUFFERED_EVENTS = "bufferedEvents";
  @TimerId(LARGE_BATCH_EMISSION_TIMER)
  @SuppressWarnings("unused")
  private final TimerSpec largeBatchEmissionTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);
  @StateId(BUFFERED_EVENTS)
  @SuppressWarnings("unused")
  private final StateSpec<OrderedListState<EventTypeT>> bufferedEventsSpec;
  @SuppressWarnings("unused")
  @StateId(MUTABLE_STATE)
  private final StateSpec<ValueState<StateTypeT>> mutableStateSpec;

  @StateId(WINDOW_CLOSED)
  @SuppressWarnings("unused")
  private final StateSpec<ValueState<Boolean>> windowClosedSpec;

  @TimerId(STATUS_EMISSION_TIMER)
  @SuppressWarnings("unused")
  private final TimerSpec statusEmissionTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
  @StateId(PROCESSING_STATE)
  @SuppressWarnings("unused")
  private final StateSpec<ValueState<ProcessingState<EventKeyTypeT>>> processingStateSpec;

  /**
   * Stateful DoFn to do the bulk of processing.
   *
   * @param eventExaminer
   * @param eventCoder
   * @param stateCoder
   * @param keyCoder
   * @param mainOutputTupleTag
   * @param statusTupleTag
   * @param statusUpdateFrequency
   * @param unprocessedEventTupleTag
   * @param produceStatusUpdateOnEveryEvent
   * @param maxNumberOfResultsToProduce
   */
  SequencePerKeyProcessorDoFn(
      EventExaminer<EventTypeT, StateTypeT> eventExaminer,
      Coder<EventTypeT> eventCoder,
      Coder<StateTypeT> stateCoder,
      Coder<EventKeyTypeT> keyCoder,
      TupleTag<KV<EventKeyTypeT, ResultTypeT>> mainOutputTupleTag,
      TupleTag<KV<EventKeyTypeT, OrderedProcessingStatus>> statusTupleTag,
      Duration statusUpdateFrequency,
      TupleTag<KV<EventKeyTypeT, KV<Long, UnprocessedEvent<EventTypeT>>>>
          unprocessedEventTupleTag,
      boolean produceStatusUpdateOnEveryEvent, long maxNumberOfResultsToProduce) {
    super(eventExaminer, mainOutputTupleTag, statusTupleTag,
        statusUpdateFrequency, unprocessedEventTupleTag, produceStatusUpdateOnEveryEvent,
        maxNumberOfResultsToProduce);
    this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
    this.processingStateSpec = StateSpecs.value(ProcessingStateCoder.of(keyCoder));
    this.mutableStateSpec = StateSpecs.value(stateCoder);
    this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
  }

  @Override
  boolean checkForFirstOrLastEvent() {
    return true;
  }

  @Override
  boolean checkForSequenceGapInBufferedEvents() {
    return true;
  }

  @ProcessElement
  public void processElement(
      @StateId(BUFFERED_EVENTS) OrderedListState<EventTypeT> bufferedEventsState,
      @AlwaysFetched @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyTypeT>> processingStateState,
      @StateId(MUTABLE_STATE) ValueState<StateTypeT> mutableStateState,
      @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
      @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
      @Element KV<EventKeyTypeT, KV<Long, EventTypeT>> eventAndSequence,
      MultiOutputReceiver outputReceiver,
      BoundedWindow window,
      ProcessContext context) {
    EventKeyTypeT key = eventAndSequence.getKey();
    long sequence = eventAndSequence.getValue().getKey();
    EventTypeT event = eventAndSequence.getValue().getValue();

    ProcessingState<EventKeyTypeT> processingState = processingStateState.read();

    if (processingState == null) {
      // This is the first time we see this key/window pair
      processingState = new ProcessingState<>(key);
      if (statusUpdateFrequency != null) {
        // Set up the timer to produce the status of the processing on a regular basis
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    if (numberOfResultsBeforeBundleStart == null) {
      // Per key processing is synchronized by Beam. There is no need to have it here.
      numberOfResultsBeforeBundleStart = processingState.getResultCount();
    }

    processingState.eventReceived();

    StateTypeT state =
        processNewEvent(
            sequence,
            event,
            processingState,
            mutableStateState,
            bufferedEventsState,
            outputReceiver);

    processBufferedEvents(
        processingState, state, bufferedEventsState, outputReceiver, largeBatchEmissionTimer);

    saveStates(
        processingStateState,
        processingState,
        mutableStateState,
        state,
        outputReceiver,
        window.maxTimestamp());

    checkIfProcessingIsCompleted(processingState);
  }

  private boolean checkIfProcessingIsCompleted(ProcessingState<EventKeyTypeT> processingState) {
    boolean result = processingState.isProcessingCompleted();
    if (result && LOG.isTraceEnabled()) {
      LOG.trace("Processing for key '" + processingState.getKey() + "' is completed.");
    }
    return result;
  }

  /**
   * Process buffered events.
   */
  private void processBufferedEvents(
      ProcessingState<EventKeyTypeT> processingState,
      @Nullable StateTypeT state,
      OrderedListState<EventTypeT> bufferedEventsState,
      MultiOutputReceiver outputReceiver,
      Timer largeBatchEmissionTimer) {
    if (state == null) {
      // Only when the current event caused a state mutation and the state is passed to this
      // method should we attempt to process buffered events
      return;
    }

    if (!processingState.readyToProcessBufferedEvents()) {
      return;
    }

    if (reachedMaxResultCountForBundle(processingState, largeBatchEmissionTimer)) {
      // No point in trying to process buffered events
      return;
    }

    // Technically this block is not needed because these preconditions are checked
    // earlier. Included to keep the linter happy.
    Long earliestBufferedSequence = processingState.getEarliestBufferedSequence();
    if (earliestBufferedSequence == null) {
      return;
    }
    Long latestBufferedSequence = processingState.getLatestBufferedSequence();
    if (latestBufferedSequence == null) {
      return;
    }

    processBufferedEventRange(processingState, state, bufferedEventsState, outputReceiver,
        largeBatchEmissionTimer, CompletedSequenceRange.EMPTY);
  }

  @OnTimer(LARGE_BATCH_EMISSION_TIMER)
  public void onBatchEmission(
      OnTimerContext context,
      @StateId(BUFFERED_EVENTS) OrderedListState<EventTypeT> bufferedEventsState,
      @AlwaysFetched @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyTypeT>> processingStatusState,
      @AlwaysFetched @StateId(MUTABLE_STATE) ValueState<StateTypeT> currentStateState,
      @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
      MultiOutputReceiver outputReceiver) {
    ProcessingState<EventKeyTypeT> processingState = processingStatusState.read();
    if (processingState == null) {
      LOG.warn("Processing state is empty. Ignore it if the pipeline is being cancelled.");
      return;
    }
    StateTypeT state = currentStateState.read();
    if (state == null) {
      LOG.warn("Mutable state is empty. Ignore it if the pipeline is being cancelled.");
      return;
    }

    LOG.debug("Starting to process batch for key '" + processingState.getKey() + "'");

    this.numberOfResultsBeforeBundleStart = processingState.getResultCount();

    processBufferedEvents(
        processingState, state, bufferedEventsState, outputReceiver, largeBatchEmissionTimer);

    saveStates(
        processingStatusState,
        processingState,
        currentStateState,
        state,
        outputReceiver,
        // TODO: validate that this is correct.
        context.window().maxTimestamp());

    checkIfProcessingIsCompleted(processingState);
  }

  @OnTimer(STATUS_EMISSION_TIMER)
  @SuppressWarnings("unused")
  public void onStatusEmission(
      MultiOutputReceiver outputReceiver,
      @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
      @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState,
      @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyTypeT>> processingStateState) {

    processStatusTimerEvent(outputReceiver, statusEmissionTimer, windowClosedState,
        processingStateState);
  }

  @OnWindowExpiration
  public void onWindowExpiration(@StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState) {
    windowClosedState.write(true);
  }
}
