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

import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.Reason;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base DoFn for processing ordered events.
 *
 * @param <EventT> type of the events to process
 * @param <EventKeyT> event key type
 * @param <StateT> state type
 */
abstract class ProcessorDoFn<
        EventT, EventKeyT, ResultT, StateT extends MutableState<EventT, ResultT>>
    extends DoFn<KV<EventKeyT, KV<Long, EventT>>, KV<EventKeyT, ResultT>> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessorDoFn.class);

  protected static final String PROCESSING_STATE = "processingState";
  protected static final String MUTABLE_STATE = "mutableState";

  protected static final String STATUS_EMISSION_TIMER = "statusTimer";
  protected static final String WINDOW_CLOSED = "windowClosed";
  protected final EventExaminer<EventT, StateT> eventExaminer;

  private final TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusTupleTag;
  protected final Duration statusUpdateFrequency;

  protected final TupleTag<KV<EventKeyT, ResultT>> mainOutputTupleTag;
  protected final TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>>
      unprocessedEventsTupleTag;
  private final boolean produceStatusUpdateOnEveryEvent;

  private final long maxNumberOfResultsToProduce;

  protected @Nullable Long numberOfResultsBeforeBundleStart = 0L;

  ProcessorDoFn(
      EventExaminer<EventT, StateT> eventExaminer,
      TupleTag<KV<EventKeyT, ResultT>> mainOutputTupleTag,
      TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusTupleTag,
      Duration statusUpdateFrequency,
      TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>> unprocessedEventTupleTag,
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

  /** @return true if each event needs to be examined. */
  abstract boolean checkForFirstOrLastEvent();

  /**
   * Process the just received event.
   *
   * @return newly created or updated State. If null is returned - the event wasn't processed.
   */
  protected @javax.annotation.Nullable StateT processNewEvent(
      long currentSequence,
      EventT currentEvent,
      ProcessingState<EventKeyT> processingState,
      ValueState<StateT> currentStateState,
      OrderedListState<EventT> bufferedEventsState,
      MultiOutputReceiver outputReceiver) {
    if (currentSequence == Long.MAX_VALUE) {
      // OrderedListState can't handle the timestamp based on MAX_VALUE.
      // To avoid exceptions, we DLQ this event.
      outputReceiver
          .get(unprocessedEventsTupleTag)
          .output(
              KV.of(
                  processingState.getKey(),
                  KV.of(
                      currentSequence,
                      UnprocessedEvent.create(
                          currentEvent, Reason.sequence_id_outside_valid_range))));
      return null;
    }

    if (processingState.hasAlreadyBeenProcessed(currentSequence)) {
      outputReceiver
          .get(unprocessedEventsTupleTag)
          .output(
              KV.of(
                  processingState.getKey(),
                  KV.of(currentSequence, UnprocessedEvent.create(currentEvent, Reason.duplicate))));
      return null;
    }

    StateT state;
    boolean thisIsTheLastEvent =
        checkForFirstOrLastEvent() && eventExaminer.isLastEvent(currentSequence, currentEvent);
    if (checkForFirstOrLastEvent() && eventExaminer.isInitialEvent(currentSequence, currentEvent)) {
      // First event of the key/window
      // What if it's a duplicate event - it will reset everything. Shall we drop/DLQ anything
      // that's before the processingState.lastOutputSequence?
      state = eventExaminer.createStateOnInitialEvent(currentEvent);

      processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

      ResultT result = state.produceResult();
      if (result != null) {
        outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
        processingState.resultProduced();
      }

      // Nothing else to do. We will attempt to process buffered events later.
      return state;
    }

    if (processingState.isNextEvent(currentSequence)) {
      // Event matches expected sequence
      state = currentStateState.read();
      if (state == null) {
        LOG.warn("Unexpectedly got an empty state. Most likely cause is pipeline drainage.");
        return null;
      }

      try {
        state.mutate(currentEvent);
      } catch (Exception e) {
        outputReceiver
            .get(unprocessedEventsTupleTag)
            .output(
                KV.of(
                    processingState.getKey(),
                    KV.of(currentSequence, UnprocessedEvent.create(currentEvent, e))));
        return null;
      }

      ResultT result = state.produceResult();
      if (result != null) {
        outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
        processingState.resultProduced();
      }
      processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

      return state;
    }

    // Event is not ready to be processed yet
    bufferEvent(
        currentSequence, currentEvent, processingState, bufferedEventsState, thisIsTheLastEvent);

    // This will signal that the state hasn't been mutated. We don't need to save it.
    return null;
  }

  protected void saveStates(
      ValueState<ProcessingState<EventKeyT>> processingStatusState,
      ProcessingState<EventKeyT> processingStatus,
      ValueState<StateT> currentStateState,
      @Nullable StateT state,
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

  void processStatusTimerEvent(
      MultiOutputReceiver outputReceiver,
      Timer statusEmissionTimer,
      ValueState<Boolean> windowClosedState,
      ValueState<ProcessingState<EventKeyT>> processingStateState) {
    ProcessingState<EventKeyT> currentState = processingStateState.read();
    if (currentState == null) {
      // This could happen if the state has been purged already during the draining.
      // It means that there is nothing that we can do.
      LOG.warn(
          "Current processing state is null in onStatusEmission() - most likely the pipeline is shutting down.");
      return;
    }

    emitProcessingStatus(currentState, outputReceiver, Instant.now());

    Boolean windowClosed = windowClosedState.read();
    if (!currentState.isProcessingCompleted()
        // Stop producing statuses if we are finished for a particular key
        && (windowClosed == null || !windowClosed)) {
      statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
    }
  }

  protected void emitProcessingStatus(
      ProcessingState<EventKeyT> processingState,
      MultiOutputReceiver outputReceiver,
      Instant statusTimestamp) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Emitting status for: " + processingState.getKey() + ", " + processingState);
    }
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
      ProcessingState<EventKeyT> processingState, Timer largeBatchEmissionTimer) {
    boolean exceeded =
        processingState.resultsProducedInBundle(
                numberOfResultsBeforeBundleStart == null ? 0 : numberOfResultsBeforeBundleStart)
            >= maxNumberOfResultsToProduce;
    if (exceeded) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Setting the timer to output next batch of events for key '"
                + processingState.getKey()
                + "'");
      }
      // See GroupIntoBatches for examples on how to hold the timestamp.
      // TODO: test that on draining the pipeline all the results are still produced correctly.
      // See: https://github.com/apache/beam/issues/30781
      largeBatchEmissionTimer.offset(Duration.millis(1)).setRelative();
    }
    return exceeded;
  }

  private void bufferEvent(
      long currentSequence,
      EventT currentEvent,
      ProcessingState<EventKeyT> processingState,
      OrderedListState<EventT> bufferedEventsState,
      boolean thisIsTheLastEvent) {
    Instant eventTimestamp = fromLong(currentSequence);
    bufferedEventsState.add(TimestampedValue.of(currentEvent, eventTimestamp));
    processingState.eventBuffered(currentSequence, thisIsTheLastEvent);
  }

  abstract boolean checkForSequenceGapInBufferedEvents();

  @Nullable
  StateT processBufferedEventRange(
      ProcessingState<EventKeyT> processingState,
      @Nullable StateT state,
      OrderedListState<EventT> bufferedEventsState,
      MultiOutputReceiver outputReceiver,
      Timer largeBatchEmissionTimer,
      ContiguousSequenceRange contiguousSequenceRange) {
    Long earliestBufferedSequence = processingState.getEarliestBufferedSequence();
    Long latestBufferedSequence = processingState.getLatestBufferedSequence();
    if (earliestBufferedSequence == null || latestBufferedSequence == null) {
      return state;
    }
    Instant startRange = fromLong(earliestBufferedSequence);
    Instant endRange = fromLong(latestBufferedSequence + 1);

    // readRange is efficiently implemented and will bring records in batches
    Iterable<TimestampedValue<EventT>> events = bufferedEventsState.readRange(startRange, endRange);

    Instant endClearRange = startRange; // it will get re-adjusted later.

    Iterator<TimestampedValue<EventT>> bufferedEventsIterator = events.iterator();
    while (bufferedEventsIterator.hasNext()) {
      TimestampedValue<EventT> timestampedEvent = bufferedEventsIterator.next();
      Instant eventTimestamp = timestampedEvent.getTimestamp();
      long eventSequence = eventTimestamp.getMillis();

      EventT bufferedEvent = timestampedEvent.getValue();
      boolean skipProcessing = false;
      boolean beforeInitialSequence = false;

      if (contiguousSequenceRange != null && eventSequence < contiguousSequenceRange.getStart()) {
        // In case of global sequence processing - remove the elements below the range start
        skipProcessing = true;
        beforeInitialSequence = true;
        endClearRange = fromLong(eventSequence);
      }
      if (processingState.checkForDuplicateBatchedEvent(eventSequence)) {
        // There could be multiple events under the same sequence number. Only the first one
        // will get processed. The rest are considered duplicates.
        skipProcessing = true;
      }

      if (skipProcessing) {
        outputReceiver
            .get(unprocessedEventsTupleTag)
            .output(
                KV.of(
                    processingState.getKey(),
                    KV.of(
                        eventSequence,
                        UnprocessedEvent.create(
                            bufferedEvent,
                            beforeInitialSequence
                                ? Reason.before_initial_sequence
                                : Reason.duplicate))));
        // TODO: When there is a large number of duplicates this can cause a situation where
        // we produce too much output and the runner will start throwing unrecoverable errors.
        // Need to add counting logic to accumulate both the normal and DLQ outputs.
        continue;
      }

      Long lastOutputSequence = processingState.getLastOutputSequence();
      boolean currentEventIsNextInSequence =
          lastOutputSequence != null && eventSequence == lastOutputSequence + 1;
      boolean continueProcessing =
          checkForSequenceGapInBufferedEvents()
              ? currentEventIsNextInSequence
              : (eventSequence < contiguousSequenceRange.getEnd() || currentEventIsNextInSequence);
      if (!continueProcessing) {
        processingState.foundSequenceGap(eventSequence);
        // Records will be cleared up to this element
        endClearRange = fromLong(eventSequence);
        break;
      }

      // This check needs to be done after we checked for sequence gap and before we
      // attempt to process the next element which can result in a new result.
      if (reachedMaxResultCountForBundle(processingState, largeBatchEmissionTimer)) {
        endClearRange = fromLong(eventSequence);
        break;
      }

      // Remove this record also
      endClearRange = fromLong(eventSequence + 1);

      try {
        if (state == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Creating a new state: " + processingState.getKey() + " " + bufferedEvent);
          }
          state = eventExaminer.createStateOnInitialEvent(bufferedEvent);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Mutating " + processingState.getKey() + " " + bufferedEvent);
          }
          state.mutate(bufferedEvent);
        }
      } catch (Exception e) {
        outputReceiver
            .get(unprocessedEventsTupleTag)
            .output(
                KV.of(
                    processingState.getKey(),
                    KV.of(eventSequence, UnprocessedEvent.create(bufferedEvent, e))));
        // There is a chance that the next event will have the same sequence number and will
        // process successfully.
        continue;
      }

      ResultT result = state.produceResult();
      if (result != null) {
        outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
        processingState.resultProduced();
      }
      processingState.processedBufferedEvent(eventSequence);
    }

    bufferedEventsState.clearRange(startRange, endClearRange);

    return state;
  }

  static Instant fromLong(long value) {
    return Instant.ofEpochMilli(value);
  }
}
