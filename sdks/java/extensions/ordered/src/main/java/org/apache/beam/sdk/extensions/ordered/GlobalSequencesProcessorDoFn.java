package org.apache.beam.sdk.extensions.ordered;

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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Stateful DoFn used to process events in the global sequence mode.
 *
 * @param <EventT>
 * @param <EventKeyT>
 * @param <ResultT>
 * @param <StateT>
 */
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

  private final PCollectionView<ContiguousSequenceRange> latestContiguousRangeSideInput;

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
      PCollectionView<ContiguousSequenceRange> latestContiguousRangeSideInput) {
    super(eventExaminer, mainOutputTupleTag, statusTupleTag,
        statusUpdateFrequency, unprocessedEventTupleTag, produceStatusUpdateOnEveryEvent,
        maxNumberOfResultsToProduce);

    this.latestContiguousRangeSideInput = latestContiguousRangeSideInput;
    this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
    this.processingStateSpec = StateSpecs.value(ProcessingStateCoder.of(keyCoder));
    this.mutableStateSpec = StateSpecs.value(stateCoder);
    this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
  }

  @Override
  boolean checkForFirstOrLastEvent() {
    return false;
  }

  @Override
  boolean checkForSequenceGapInBufferedEvents() {
    return false;
  }

  @ProcessElement
  public void processElement(ProcessContext context,
      @Element KV<EventKeyT, KV<Long, EventT>> eventAndSequence,
      @StateId(BUFFERED_EVENTS) OrderedListState<EventT> bufferedEventsProxy,
      @AlwaysFetched @StateId(PROCESSING_STATE)
      ValueState<ProcessingState<EventKeyT>> processingStateProxy,
      @StateId(MUTABLE_STATE) ValueState<StateT> mutableStateProxy,
      @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
      @TimerId(BATCH_EMISSION_TIMER) Timer batchEmissionTimer,
      MultiOutputReceiver outputReceiver,
      BoundedWindow window) {

    ContiguousSequenceRange lastContiguousRange = context.sideInput(
        latestContiguousRangeSideInput);

    EventT event = eventAndSequence.getValue().getValue();
    EventKeyT key = eventAndSequence.getKey();
    long sequence = eventAndSequence.getValue().getKey();

    if (LOG.isTraceEnabled()) {
      LOG.trace(key + ": " + sequence + " lastRange: " + lastContiguousRange);
    }

    ProcessingState<EventKeyT> processingState = processingStateProxy.read();

    if (processingState == null) {
      // This is the first time we see this key/window pair
      processingState = new ProcessingState<>(key);
      if (statusUpdateFrequency != null) {
        // Set up the timer to produce the status of the processing on a regular basis
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    processingState.updateGlobalSequenceDetails(lastContiguousRange);

    if (event == null) {
      // This is a ticker event. We only need to update the state as it relates to the global sequence.
      processingStateProxy.write(processingState);

      setBatchEmissionTimerIfNeeded(batchEmissionTimer, processingState);

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
            mutableStateProxy,
            bufferedEventsProxy,
            outputReceiver);

    saveStates(
        processingStateProxy,
        processingState,
        mutableStateProxy,
        state,
        outputReceiver,
        window.maxTimestamp());

    setBatchEmissionTimerIfNeeded(batchEmissionTimer, processingState);
  }

  private void setBatchEmissionTimerIfNeeded(Timer batchEmissionTimer,
      ProcessingState<EventKeyT> processingState) {
    ContiguousSequenceRange lastCompleteGlobalSequence = processingState.getLastContiguousRange();
    if (lastCompleteGlobalSequence != null &&
        processingState.thereAreGloballySequencedEventsToBeProcessed()) {
      batchEmissionTimer.set(lastCompleteGlobalSequence.getTimestamp());
    }
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

    ContiguousSequenceRange lastContiguousRange = processingState.getLastContiguousRange();
    if (lastContiguousRange == null) {
      LOG.warn("Last complete global instance is null.");
      return;
    }

    Long earliestBufferedSequence = processingState.getEarliestBufferedSequence();
    if (earliestBufferedSequence == null) {
      LOG.warn("Earliest buffered sequence is null.");
      return;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Emission timer: " + processingState);
    }

    this.numberOfResultsBeforeBundleStart = processingState.getResultCount();

    state = processBufferedEventRange(processingState, state, bufferedEventsState, outputReceiver,
        batchEmissionTimer, lastContiguousRange);

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
