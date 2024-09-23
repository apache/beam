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

import com.google.auto.value.AutoValue;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

/** Indicates the status of ordered processing for a particular key. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class OrderedProcessingStatus {

  public static OrderedProcessingStatus create(
      @Nullable Long lastProcessedSequence,
      long numberOfBufferedEvents,
      @Nullable Long earliestBufferedSequence,
      @Nullable Long latestBufferedSequence,
      long numberOfReceivedEvents,
      long resultCount,
      long duplicateCount,
      boolean lastEventReceived) {
    return new AutoValue_OrderedProcessingStatus.Builder()
        .setLastProcessedSequence(lastProcessedSequence)
        .setNumberOfBufferedEvents(numberOfBufferedEvents)
        .setEarliestBufferedSequence(earliestBufferedSequence)
        .setLatestBufferedSequence(latestBufferedSequence)
        .setNumberOfReceivedEvents(numberOfReceivedEvents)
        .setLastEventReceived(lastEventReceived)
        .setDuplicateCount(duplicateCount)
        .setResultCount(resultCount)
        .setStatusDate(Instant.now())
        .build();
  }

  /**
   * @return Last sequence processed. If null is returned - no elements for the given key and window
   *     have been processed yet.
   */
  public abstract @Nullable Long getLastProcessedSequence();

  /** @return Number of events received out of sequence and buffered. */
  public abstract long getNumberOfBufferedEvents();

  /** @return Earliest buffered sequence. If null is returned - there are no buffered events. */
  @Nullable
  public abstract Long getEarliestBufferedSequence();

  /** @return Latest buffered sequence. If null is returned - there are no buffered events. */
  @Nullable
  public abstract Long getLatestBufferedSequence();

  /** @return Total number of events received for the given key and window. */
  public abstract long getNumberOfReceivedEvents();

  /**
   * @return Number of duplicate events which were output in {@link
   *     OrderedEventProcessorResult#unprocessedEvents()} PCollection
   */
  public abstract long getDuplicateCount();

  /** @return Number of output results produced. */
  public abstract long getResultCount();

  /**
   * @return Indicator that the last event for the given key and window has been received. It
   *     doesn't necessarily mean that all the events for the given key and window have been
   *     processed. Use {@link OrderedProcessingStatus#getNumberOfBufferedEvents()} == 0 and this
   *     indicator as the sign that the processing is complete.
   */
  public abstract boolean isLastEventReceived();

  /**
   * @return Timestamp of when the status was produced. It is not related to the event's timestamp.
   */
  public abstract Instant getStatusDate();

  @Override
  public final boolean equals(@Nullable Object obj) {
    if (obj == null) {
      return false;
    }
    if (!OrderedProcessingStatus.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    OrderedProcessingStatus that = (OrderedProcessingStatus) obj;
    boolean result =
        Objects.equals(this.getEarliestBufferedSequence(), that.getEarliestBufferedSequence())
            && Objects.equals(this.getLastProcessedSequence(), that.getLastProcessedSequence())
            && Objects.equals(this.getLatestBufferedSequence(), that.getLatestBufferedSequence())
            && this.getNumberOfBufferedEvents() == that.getNumberOfBufferedEvents()
            && this.getDuplicateCount() == that.getDuplicateCount()
            && this.getResultCount() == that.getResultCount()
            && this.getNumberOfReceivedEvents() == that.getNumberOfReceivedEvents();
    return result;
  }

  @Override
  public final int hashCode() {
    return Objects.hash(
        this.getEarliestBufferedSequence(),
        this.getLastProcessedSequence(),
        this.getLatestBufferedSequence(),
        this.getNumberOfBufferedEvents(),
        this.getNumberOfReceivedEvents(),
        this.getDuplicateCount(),
        this.getResultCount());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLastProcessedSequence(@Nullable Long value);

    public abstract Builder setNumberOfBufferedEvents(long value);

    public abstract Builder setEarliestBufferedSequence(@Nullable Long value);

    public abstract Builder setLatestBufferedSequence(@Nullable Long value);

    public abstract Builder setNumberOfReceivedEvents(long value);

    public abstract Builder setDuplicateCount(long value);

    public abstract Builder setResultCount(long value);

    public abstract Builder setLastEventReceived(boolean value);

    public abstract Builder setStatusDate(Instant value);

    public abstract OrderedProcessingStatus build();
  }
}
