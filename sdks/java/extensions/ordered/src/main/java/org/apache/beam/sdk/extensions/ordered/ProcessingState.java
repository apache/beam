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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.initialization.qual.Initialized;

/**
 * Class used to store the status of processing for a particular key.
 *
 * @param <KeyT>
 */
class ProcessingState<KeyT> {

  @Nullable private Long lastOutputSequence;
  @Nullable private Long latestBufferedSequence;
  @Nullable private Long earliestBufferedSequence;
  private long bufferedEventCount;

  private boolean lastEventReceived;

  private long eventsReceived;

  private long duplicates;

  private long resultCount;

  @Nullable private ContiguousSequenceRange lastCompleteGlobalSequence;

  private KeyT key;

  public ProcessingState(KeyT key) {
    this.key = key;
    this.bufferedEventCount = 0;
    this.lastOutputSequence = null;
    this.earliestBufferedSequence = null;
    this.latestBufferedSequence = null;
    this.lastCompleteGlobalSequence = null;
  }

  /**
   * Only to be used by the coder.
   *
   * @param key
   * @param lastOutputSequence
   * @param earliestBufferedSequence
   * @param latestBufferedSequence
   * @param bufferedEventCount
   */
  ProcessingState(
      KeyT key,
      @Nullable Long lastOutputSequence,
      @Nullable Long earliestBufferedSequence,
      @Nullable Long latestBufferedSequence,
      long bufferedEventCount,
      long eventsReceived,
      long duplicates,
      long resultCount,
      boolean lastEventReceived) {
    this(key);
    this.lastOutputSequence = lastOutputSequence;
    this.earliestBufferedSequence = earliestBufferedSequence;
    this.latestBufferedSequence = latestBufferedSequence;
    this.bufferedEventCount = bufferedEventCount;
    this.eventsReceived = eventsReceived;
    this.duplicates = duplicates;
    this.resultCount = resultCount;
    this.lastEventReceived = lastEventReceived;
  }

  @Nullable
  public Long getLastOutputSequence() {
    return lastOutputSequence;
  }

  @Nullable
  public Long getLatestBufferedSequence() {
    return latestBufferedSequence;
  }

  @Nullable
  public Long getEarliestBufferedSequence() {
    return earliestBufferedSequence;
  }

  public long getBufferedEventCount() {
    return bufferedEventCount;
  }

  public long getEventsReceived() {
    return eventsReceived;
  }

  public boolean isLastEventReceived() {
    return lastEventReceived;
  }

  public long getResultCount() {
    return resultCount;
  }

  public long getDuplicates() {
    return duplicates;
  }

  public KeyT getKey() {
    return key;
  }

  public @Nullable ContiguousSequenceRange getLastContiguousRange() {
    return lastCompleteGlobalSequence;
  }

  public void setLastCompleteGlobalSequence(@Nullable ContiguousSequenceRange lastCompleteGlobalSequence) {
    this.lastCompleteGlobalSequence = lastCompleteGlobalSequence;
  }

  /**
   * Current event matched the sequence and was processed.
   *
   * @param sequence
   * @param lastEvent
   */
  public void eventAccepted(long sequence, boolean lastEvent) {
    this.lastOutputSequence = sequence;
    setLastEventReceived(lastEvent);
  }

  private void setLastEventReceived(boolean lastEvent) {
    // Only one last event can be received.
    this.lastEventReceived = this.lastEventReceived ? true : lastEvent;
  }

  /**
   * New event added to the buffer.
   *
   * @param sequenceNumber of the event
   * @param isLastEvent
   */
  void eventBuffered(long sequenceNumber, boolean isLastEvent) {
    bufferedEventCount++;
    latestBufferedSequence =
        Math.max(
            sequenceNumber,
            latestBufferedSequence == null ? Long.MIN_VALUE : latestBufferedSequence);
    earliestBufferedSequence =
        Math.min(
            sequenceNumber,
            earliestBufferedSequence == null ? Long.MAX_VALUE : earliestBufferedSequence);

    setLastEventReceived(isLastEvent);
  }

  /**
   * An event was processed and removed from the buffer.
   *
   * @param sequence of the processed event
   */
  public void processedBufferedEvent(long sequence) {
    bufferedEventCount--;
    lastOutputSequence = sequence;

    if (bufferedEventCount == 0) {
      earliestBufferedSequence = latestBufferedSequence = null;
    } else {
      // We don't know for sure that it's the earliest record yet, but OrderedEventProcessor will
      // read the next
      // buffered event and call foundSequenceGap() and adjust this value.
      earliestBufferedSequence = sequence + 1;
    }
  }

  /**
   * A set of records was pulled from the buffer, but it turned out that the element is not
   * sequential.
   *
   * @param newEarliestSequence
   */
  public void foundSequenceGap(long newEarliestSequence) {
    earliestBufferedSequence = newEarliestSequence;
  }

  @Override
  public boolean equals(@Nullable @Initialized Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProcessingState)) {
      return false;
    }
    ProcessingState<?> that = (ProcessingState<?>) o;
    return bufferedEventCount == that.bufferedEventCount
        && lastEventReceived == that.lastEventReceived
        && eventsReceived == that.eventsReceived
        && duplicates == that.duplicates
        && Objects.equals(lastOutputSequence, that.lastOutputSequence)
        && Objects.equals(latestBufferedSequence, that.latestBufferedSequence)
        && Objects.equals(earliestBufferedSequence, that.earliestBufferedSequence)
        && Objects.equals(key, that.key)
        && resultCount == that.resultCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        lastOutputSequence,
        latestBufferedSequence,
        earliestBufferedSequence,
        bufferedEventCount,
        lastEventReceived,
        eventsReceived,
        duplicates,
        resultCount,
        key);
  }

  @Override
  public String toString() {
    return "ProcessingState{" +
        "lastOutputSequence=" + lastOutputSequence +
        ", latestBufferedSequence=" + latestBufferedSequence +
        ", earliestBufferedSequence=" + earliestBufferedSequence +
        ", bufferedEventCount=" + bufferedEventCount +
        ", lastEventReceived=" + lastEventReceived +
        ", eventsReceived=" + eventsReceived +
        ", duplicates=" + duplicates +
        ", resultCount=" + resultCount +
        ", lastCompleteGlobalSequence=" + lastCompleteGlobalSequence +
        ", key=" + key +
        '}';
  }

  public boolean isProcessingCompleted() {
    return lastEventReceived && bufferedEventCount == 0;
  }

  public void eventReceived() {
    eventsReceived++;
  }

  public boolean isNextEvent(long sequence) {
    return lastOutputSequence != null && sequence == lastOutputSequence + 1;
  }

  public boolean hasAlreadyBeenProcessed(long currentSequence) {
    boolean result = lastOutputSequence != null && lastOutputSequence >= currentSequence;
    if (result) {
      duplicates++;
    }
    return result;
  }

  public boolean checkForDuplicateBatchedEvent(long currentSequence) {
    boolean result = lastOutputSequence != null && lastOutputSequence == currentSequence;
    if (result) {
      duplicates++;
      if (--bufferedEventCount == 0) {
        earliestBufferedSequence = latestBufferedSequence = null;
      }
    }
    return result;
  }

  public boolean readyToProcessBufferedEvents() {
    return earliestBufferedSequence != null
        && lastOutputSequence != null
        && earliestBufferedSequence == lastOutputSequence + 1;
  }

  public void resultProduced() {
    resultCount++;
  }

  public long resultsProducedInBundle(long numberOfResultsBeforeBundleStart) {
    return resultCount - numberOfResultsBeforeBundleStart;
  }

  public void updateGlobalSequenceDetails(ContiguousSequenceRange updated) {
    if(thereAreGloballySequencedEventsToBeProcessed()) {
      // We don't update the timer if we can already process events in the onTimer batch.
      // Otherwise, it's possible that we will be pushing the timer to later timestamps
      // without a chance to run and produce output.
      return;
    }
    this.lastCompleteGlobalSequence = updated;
  }

  public boolean thereAreGloballySequencedEventsToBeProcessed() {
    return bufferedEventCount > 0
        && lastCompleteGlobalSequence != null
        && earliestBufferedSequence != null &&
        earliestBufferedSequence <= lastCompleteGlobalSequence.getEnd();
  }

  /**
   * Coder for the processing status.
   *
   * @param <KeyT>
   */
  static class ProcessingStateCoder<KeyT> extends Coder<ProcessingState<KeyT>> {

    private static final NullableCoder<Long> NULLABLE_LONG_CODER =
        NullableCoder.of(VarLongCoder.of());
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final VarIntCoder INTEGER_CODER = VarIntCoder.of();
    private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

    private static final NullableCoder<ContiguousSequenceRange> SEQUENCE_AND_TIMESTAMP_CODER =
        NullableCoder.of(ContiguousSequenceRange.CompletedSequenceRangeCoder.of());

    private Coder<KeyT> keyCoder;

    private ProcessingStateCoder(Coder<KeyT> keyCoder) {
      this.keyCoder = keyCoder;
    }

    public static <KeyT> ProcessingStateCoder<KeyT> of(Coder<KeyT> keyCoder) {
      return new ProcessingStateCoder<>(keyCoder);
    }

    @Override
    public void encode(ProcessingState<KeyT> value, OutputStream outStream) throws IOException {
      NULLABLE_LONG_CODER.encode(value.getLastOutputSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getEarliestBufferedSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getLatestBufferedSequence(), outStream);
      LONG_CODER.encode(value.getBufferedEventCount(), outStream);
      LONG_CODER.encode(value.getEventsReceived(), outStream);
      LONG_CODER.encode(value.getDuplicates(), outStream);
      LONG_CODER.encode(value.getResultCount(), outStream);
      BOOLEAN_CODER.encode(value.isLastEventReceived(), outStream);
      keyCoder.encode(value.getKey(), outStream);
      SEQUENCE_AND_TIMESTAMP_CODER.encode(value.getLastContiguousRange(), outStream);
    }

    @Override
    public ProcessingState<KeyT> decode(InputStream inStream) throws IOException {
      Long lastOutputSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long earliestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long latestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      int bufferedRecordCount = INTEGER_CODER.decode(inStream);
      long recordsReceivedCount = LONG_CODER.decode(inStream);
      long duplicates = LONG_CODER.decode(inStream);
      long resultCount = LONG_CODER.decode(inStream);
      boolean isLastEventReceived = BOOLEAN_CODER.decode(inStream);
      KeyT key = keyCoder.decode(inStream);
      ContiguousSequenceRange lastCompleteGlobalSequence = SEQUENCE_AND_TIMESTAMP_CODER.decode(inStream);

      ProcessingState<KeyT> result = new ProcessingState<>(
          key,
          lastOutputSequence,
          earliestBufferedSequence,
          latestBufferedSequence,
          bufferedRecordCount,
          recordsReceivedCount,
          duplicates,
          resultCount,
          isLastEventReceived);
      result.setLastCompleteGlobalSequence(lastCompleteGlobalSequence);

      return result;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of();
    }

    @Override
    public void verifyDeterministic() {}
  }
}
