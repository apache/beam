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

package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

/**
 * A testing input that generates an unbounded {@link PCollection} of elements, advancing the
 * watermark and processing time as elements are emitted. After all of the specified elements are
 * emitted, ceases to produce output.
 *
 * <p>Each call to a {@link TestStream.Builder} method will only be reflected in the state of the
 * {@link Pipeline} after each method before it has completed and no more progress can be made by
 * the {@link Pipeline}. A {@link PipelineRunner} must ensure that no more progress can be made in
 * the {@link Pipeline} before advancing the state of the {@link TestStream}.
 */
public final class TestStream<T> extends PTransform<PBegin, PCollection<T>> {
  private final List<Event<T>> events;
  private final Coder<T> coder;

  /**
   * Create a new {@link TestStream.Builder} with no elements and watermark equal to {@link
   * BoundedWindow#TIMESTAMP_MIN_VALUE}.
   */
  public static <T> Builder<T> create(Coder<T> coder) {
    return new Builder<>(coder);
  }

  private TestStream(Coder<T> coder, List<Event<T>> events) {
    this.coder = coder;
    this.events = checkNotNull(events);
  }

  /**
   * An incomplete {@link TestStream}. Elements added to this builder will be produced in sequence
   * when the pipeline created by the {@link TestStream} is run.
   */
  public static class Builder<T> {
    private final Coder<T> coder;
    private final ImmutableList<Event<T>> events;
    private final Instant currentWatermark;

    private Builder(Coder<T> coder) {
      this(coder, ImmutableList.<Event<T>>of(), BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    private Builder(Coder<T> coder, ImmutableList<Event<T>> events, Instant currentWatermark) {
      this.coder = coder;
      this.events = events;
      this.currentWatermark = currentWatermark;
    }

    /**
     * Adds the specified elements to the source with timestamp equal to the current watermark.
     *
     * @return A {@link TestStream.Builder} like this one that will add the provided elements
     *         after all earlier events have completed.
     */
    @SafeVarargs
    public final Builder<T> addElements(T element, T... elements) {
      TimestampedValue<T> firstElement = TimestampedValue.of(element, currentWatermark);
      @SuppressWarnings({"unchecked", "rawtypes"})
      TimestampedValue<T>[] remainingElements = new TimestampedValue[elements.length];
      for (int i = 0; i < elements.length; i++) {
        remainingElements[i] = TimestampedValue.of(elements[i], currentWatermark);
      }
      return addElements(firstElement, remainingElements);
    }

    /**
     * Adds the specified elements to the source with the provided timestamps.
     *
     * @return A {@link TestStream.Builder} like this one that will add the provided elements
     *         after all earlier events have completed.
     */
    @SafeVarargs
    public final Builder<T> addElements(
        TimestampedValue<T> element, TimestampedValue<T>... elements) {
      checkArgument(
          element.getTimestamp().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Elements must have timestamps before %s. Got: %s",
          BoundedWindow.TIMESTAMP_MAX_VALUE,
          element.getTimestamp());
      for (TimestampedValue<T> multiElement : elements) {
        checkArgument(
            multiElement.getTimestamp().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
            "Elements must have timestamps before %s. Got: %s",
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            multiElement.getTimestamp());
      }
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(ElementEvent.add(element, elements))
              .build();
      return new Builder<T>(coder, newEvents, currentWatermark);
    }

    /**
     * Advance the watermark of this source to the specified instant.
     *
     * <p>The watermark must advance monotonically and cannot advance to {@link
     * BoundedWindow#TIMESTAMP_MAX_VALUE} or beyond.
     *
     * @return A {@link TestStream.Builder} like this one that will advance the watermark to the
     *         specified point after all earlier events have completed.
     */
    public Builder<T> advanceWatermarkTo(Instant newWatermark) {
      checkArgument(
          newWatermark.isAfter(currentWatermark), "The watermark must monotonically advance");
      checkArgument(
          newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "The Watermark cannot progress beyond the maximum. Got: %s. Maximum: %s",
          newWatermark,
          BoundedWindow.TIMESTAMP_MAX_VALUE);
      ImmutableList<Event<T>> newEvents = ImmutableList.<Event<T>>builder()
          .addAll(events)
          .add(WatermarkEvent.<T>advanceTo(newWatermark))
          .build();
      return new Builder<T>(coder, newEvents, newWatermark);
    }

    /**
     * Advance the processing time by the specified amount.
     *
     * @return A {@link TestStream.Builder} like this one that will advance the processing time by
     *         the specified amount after all earlier events have completed.
     */
    public Builder<T> advanceProcessingTime(Duration amount) {
      checkArgument(
          amount.getMillis() > 0,
          "Must advance the processing time by a positive amount. Got: ",
          amount);
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(ProcessingTimeEvent.<T>advanceBy(amount))
              .build();
      return new Builder<T>(coder, newEvents, currentWatermark);
    }

    /**
     * Advance the watermark to infinity, completing this {@link TestStream}. Future calls to the
     * same builder will not affect the returned {@link TestStream}.
     */
    public TestStream<T> advanceWatermarkToInfinity() {
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(WatermarkEvent.<T>advanceTo(BoundedWindow.TIMESTAMP_MAX_VALUE))
              .build();
      return new TestStream<>(coder, newEvents);
    }
  }

  /**
   * An event in a {@link TestStream}. A marker interface for all events that happen while
   * evaluating a {@link TestStream}.
   */
  public interface Event<T> {
    EventType getType();
  }

  /**
   * The types of {@link Event} that are supported by {@link TestStream}.
   */
  public enum EventType {
    ELEMENT,
    WATERMARK,
    PROCESSING_TIME
  }

  /** A {@link Event} that produces elements. */
  @AutoValue
  public abstract static class ElementEvent<T> implements Event<T> {
    public abstract Iterable<TimestampedValue<T>> getElements();

    @SafeVarargs
    static <T> Event<T> add(TimestampedValue<T> element, TimestampedValue<T>... elements) {
      return add(ImmutableList.<TimestampedValue<T>>builder().add(element).add(elements).build());
    }

    static <T> Event<T> add(Iterable<TimestampedValue<T>> elements) {
      return new AutoValue_TestStream_ElementEvent<>(EventType.ELEMENT, elements);
    }
  }

  /** A {@link Event} that advances the watermark. */
  @AutoValue
  public abstract static class WatermarkEvent<T> implements Event<T> {
    public abstract Instant getWatermark();

    static <T> Event<T> advanceTo(Instant newWatermark) {
      return new AutoValue_TestStream_WatermarkEvent<>(EventType.WATERMARK, newWatermark);
    }
  }

  /** A {@link Event} that advances the processing time clock. */
  @AutoValue
  public abstract static class ProcessingTimeEvent<T> implements Event<T> {
    public abstract Duration getProcessingTimeAdvance();

    static <T> Event<T> advanceBy(Duration amount) {
      return new AutoValue_TestStream_ProcessingTimeEvent<>(EventType.PROCESSING_TIME, amount);
    }
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.<T>createPrimitiveOutputInternal(
            input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
        .setCoder(coder);
  }

  public Coder<T> getValueCoder() {
    return coder;
  }

  /**
   * Returns a coder suitable for encoding {@link TestStream.Event}.
   */
  public Coder<Event<T>> getEventCoder() {
    return EventCoder.of(coder);
  }

  /**
   * Returns the sequence of {@link Event Events} in this {@link TestStream}.
   *
   * <p>For use by {@link PipelineRunner} authors.
   */
  public List<Event<T>> getEvents() {
    return events;
  }

  /**
   * A {@link Coder} that encodes and decodes {@link TestStream.Event Events}.
   *
   * @param <T> the type of elements in {@link ElementEvent ElementEvents} encoded and decoded by
   *            this {@link EventCoder}
   */
  @VisibleForTesting
  static final class EventCoder<T> extends StandardCoder<Event<T>> {
    private static final Coder<ReadableDuration> DURATION_CODER = DurationCoder.of();
    private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
    private final Coder<T> valueCoder;
    private final Coder<Iterable<TimestampedValue<T>>> elementCoder;

    public static <T> EventCoder<T> of(Coder<T> valueCoder) {
      return new EventCoder<>(valueCoder);
    }

    @JsonCreator
    public static <T> EventCoder<T> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<? extends Coder<?>> components) {
      checkArgument(
          components.size() == 1,
          "Was expecting exactly one component coder, got %s",
          components.size());
      return new EventCoder<>((Coder<T>) components.get(0));
    }

    private EventCoder(Coder<T> valueCoder) {
      this.valueCoder = valueCoder;
      this.elementCoder = IterableCoder.of(TimestampedValueCoder.of(valueCoder));
    }

    @Override
    public void encode(
        Event<T> value, OutputStream outStream, Context context)
        throws IOException {
      VarInt.encode(value.getType().ordinal(), outStream);
      switch (value.getType()) {
        case ELEMENT:
          Iterable<TimestampedValue<T>> elems = ((ElementEvent<T>) value).getElements();
          elementCoder.encode(elems, outStream, context);
          break;
        case WATERMARK:
          Instant ts = ((WatermarkEvent<T>) value).getWatermark();
          INSTANT_CODER.encode(ts, outStream, context);
          break;
        case PROCESSING_TIME:
          Duration processingAdvance = ((ProcessingTimeEvent<T>) value).getProcessingTimeAdvance();
          DURATION_CODER.encode(processingAdvance, outStream, context);
          break;
        default:
          throw new AssertionError("Unreachable: Unsupported Event Type " + value.getType());
      }
    }

    @Override
    public Event<T> decode(
        InputStream inStream, Context context) throws IOException {
      EventType eventType = EventType.values()[VarInt.decodeInt(inStream)];
      switch (eventType) {
        case ELEMENT:
          Iterable<TimestampedValue<T>> elements = elementCoder.decode(inStream, context);
          return ElementEvent.add(elements);
        case WATERMARK:
          return WatermarkEvent.advanceTo(INSTANT_CODER.decode(inStream, context));
        case PROCESSING_TIME:
          return ProcessingTimeEvent.advanceBy(
              DURATION_CODER.decode(inStream, context).toDuration());
        default:
          throw new AssertionError("Unreachable: Unsupported Event Type " + eventType);
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      elementCoder.verifyDeterministic();
      DURATION_CODER.verifyDeterministic();
      INSTANT_CODER.verifyDeterministic();
    }

    @Override
    public TypeDescriptor<Event<T>> getEncodedTypeDescriptor() {
      return new TypeDescriptor<Event<T>>() {}.where(
          new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
    }
  }
}
