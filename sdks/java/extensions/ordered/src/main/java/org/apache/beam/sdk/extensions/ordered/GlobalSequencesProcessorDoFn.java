package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.ordered.GlobalSequenceTracker.SequenceAndTimestamp;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GlobalSequencesProcessorDoFn<EventT, EventKeyT, ResultT,
    StateT extends MutableState<EventT, ResultT>>
    extends ProcessorDoFn<EventT, EventKeyT, ResultT, StateT> {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalSequencesProcessorDoFn.class);

  private static final String BATCH_EMISSION_TIMER = "batchTimer";

  @TimerId(BATCH_EMISSION_TIMER)
  @SuppressWarnings("unused")
  private final TimerSpec batchTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  private static final String BUFFERED_EVENTS = "bufferedEvents";
  @StateId(BUFFERED_EVENTS)
  @SuppressWarnings("unused")
  private final StateSpec<OrderedListState<EventT>> bufferedEventsSpec;

  @StateId(PROCESSING_STATE)
  @SuppressWarnings("unused")
  private final StateSpec<ValueState<ProcessingState<EventKeyT>>> processingStateSpec;

  @StateId(MUTABLE_STATE)
  @SuppressWarnings("unused")
  private final StateSpec<ValueState<StateT>> mutableStateSpec;

  @StateId(WINDOW_CLOSED)
  @SuppressWarnings("unused")
  private final StateSpec<ValueState<Boolean>> windowClosedSpec;

  @TimerId(STATUS_EMISSION_TIMER)
  @SuppressWarnings("unused")
  private final TimerSpec statusEmissionTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final PCollectionView<SequenceAndTimestamp> latestContinuousSequenceSideInput;

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
   * @param latestContinuousSequenceSideInput
   */
  GlobalSequencesProcessorDoFn(EventExaminer<EventT, StateT> eventExaminer,
      Coder<EventT> eventCoder,
      Coder<StateT> stateCoder,
      Coder<EventKeyT> keyCoder,
      TupleTag<KV<EventKeyT, ResultT>> mainOutputTupleTag,
      TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusTupleTag,
      Duration statusUpdateFrequency,
      TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>>
          unprocessedEventTupleTag,
      boolean produceStatusUpdateOnEveryEvent, long maxNumberOfResultsToProduce,
      PCollectionView<SequenceAndTimestamp> latestContinuousSequenceSideInput) {
    super(eventExaminer, mainOutputTupleTag, statusTupleTag,
        statusUpdateFrequency, unprocessedEventTupleTag, produceStatusUpdateOnEveryEvent,
        maxNumberOfResultsToProduce);

    this.latestContinuousSequenceSideInput = latestContinuousSequenceSideInput;
    this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
    this.processingStateSpec = StateSpecs.value(ProcessingStateCoder.of(keyCoder));
    this.mutableStateSpec = StateSpecs.value(stateCoder);
    this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
  }

  @Override
  boolean checkForInitialEvent() {
    return false;
  }

  @Override
  boolean checkForSequenceGapInBufferedEvents() {
    return false;
  }

  @ProcessElement
  public void processElement(ProcessContext context,
      @Element KV<EventKeyT, KV<Long, EventT>> eventAndSequence,
      @StateId(BUFFERED_EVENTS) OrderedListState<EventT> bufferedEventsState,
      @AlwaysFetched @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyT>> processingStateState,
      @StateId(MUTABLE_STATE) ValueState<StateT> mutableStateState,
      @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
      @TimerId(BATCH_EMISSION_TIMER) Timer batchEmissionTimer,
      MultiOutputReceiver outputReceiver,
      BoundedWindow window) {

    SequenceAndTimestamp lastContinuousSequence = context.sideInput(
        latestContinuousSequenceSideInput);
    LOG.info("lastSequence: " + lastContinuousSequence);

    EventT event = eventAndSequence.getValue().getValue();

    EventKeyT key = eventAndSequence.getKey();
    long sequence = eventAndSequence.getValue().getKey();

    ProcessingState<EventKeyT> processingState = processingStateState.read();

    if (processingState == null) {
      // This is the first time we see this key/window pair
      processingState = new ProcessingState<>(key);
      if (statusUpdateFrequency != null) {
        // Set up the timer to produce the status of the processing on a regular basis
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    processingState.updateGlobalSequenceDetails(lastContinuousSequence);

    if (event == null) {
      // This is the ticker event. We only need to update the state as it relates to the global sequence.
      processingStateState.write(processingState);
      return;
    }

    if (numberOfResultsBeforeBundleStart == null) {
      // Per key processing is synchronized by Beam. There is no need to have it here.
      numberOfResultsBeforeBundleStart = processingState.getResultCount();
    }

    processingState.eventReceived();

    StateT state =
        processNewEvent(
            sequence,
            event,
            processingState,
            mutableStateState,
            bufferedEventsState,
            outputReceiver);

    saveStates(
        processingStateState,
        processingState,
        mutableStateState,
        state,
        outputReceiver,
        window.maxTimestamp());

    // TODO: we can keep resetting this into the future under heavy load.
    //  Need to add logic to avoid doing it.
    //
    batchEmissionTimer.set(lastContinuousSequence.getTimestamp());

    // Only if the record matches the sequence it can be output now
    // TODO: refactor the code from SequencePerKeyDoFn
  }

  @OnTimer(BATCH_EMISSION_TIMER)
  public void onBatchEmission(
      OnTimerContext context,
      @StateId(BUFFERED_EVENTS) OrderedListState<EventT> bufferedEventsState,
      @AlwaysFetched @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyT>> processingStatusState,
      @AlwaysFetched @StateId(MUTABLE_STATE) ValueState<StateT> mutableStateState,
      @TimerId(BATCH_EMISSION_TIMER) Timer batchEmissionTimer,
      MultiOutputReceiver outputReceiver) {

    // At this point everything in the buffered state is ready to be processed up to the latest global sequence.
    @Nullable ProcessingState<EventKeyT> processingState = processingStatusState.read();
    if (processingState == null) {
      LOG.warn("Missing the processing state. Probably occurred during pipeline drainage");
      return;
    }

    StateT state = mutableStateState.read();

    Long lastCompleteGlobalSequence = processingState.getLastCompleteGlobalSequence();
    if (lastCompleteGlobalSequence == null) {
      LOG.warn("Last complete global instance is null.");
      return;
    }

    Long earliestBufferedSequence = processingState.getEarliestBufferedSequence();
    if (earliestBufferedSequence == null) {
      LOG.warn("Earliest buffered sequence is null.");
      return;
    }
    Instant startRange = fromLong(earliestBufferedSequence);
    Instant endRange = fromLong(lastCompleteGlobalSequence + 1);

    LOG.info("Processing range " + processingState.getKey() + " " + startRange + " " + endRange);
    state = processBufferedEventRange(processingState, state, bufferedEventsState, outputReceiver,
        batchEmissionTimer, startRange, endRange);

    saveStates(
        processingStatusState,
        processingState,
        mutableStateState,
        state,
        outputReceiver,
        // TODO: validate that this is correct.
        context.window().maxTimestamp());
  }

  @OnTimer(STATUS_EMISSION_TIMER)
  @SuppressWarnings("unused")
  public void onStatusEmission(
      MultiOutputReceiver outputReceiver,
      @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
      @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState,
      @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyT>> processingStateState) {

    processStatusTimerEvent(outputReceiver, statusEmissionTimer, windowClosedState,
        processingStateState);
  }
}
