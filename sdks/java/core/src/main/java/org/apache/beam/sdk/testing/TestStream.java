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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

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

  @Experimental(Kind.SCHEMAS)
  public static Builder<Row> create(Schema schema) {
    return create(SchemaCoder.of(schema));
  }

  @Experimental(Kind.SCHEMAS)
  public static <T> Builder<T> create(
      Schema schema,
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {
    return create(SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction));
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
      this(coder, ImmutableList.of(), BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    private Builder(Coder<T> coder, ImmutableList<Event<T>> events, Instant currentWatermark) {
      this.coder = coder;
      this.events = events;
      this.currentWatermark = currentWatermark;
    }

    /**
     * Adds the specified elements to the source with timestamp equal to the current watermark.
     *
     * @return A {@link TestStream.Builder} like this one that will add the provided elements after
     *     all earlier events have completed.
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
     * @return A {@link TestStream.Builder} like this one that will add the provided elements after
     *     all earlier events have completed.
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
      return new Builder<>(coder, newEvents, currentWatermark);
    }

    /**
     * Advance the watermark of this source to the specified instant.
     *
     * <p>The watermark must advance monotonically and cannot advance to {@link
     * BoundedWindow#TIMESTAMP_MAX_VALUE} or beyond.
     *
     * @return A {@link TestStream.Builder} like this one that will advance the watermark to the
     *     specified point after all earlier events have completed.
     */
    public Builder<T> advanceWatermarkTo(Instant newWatermark) {
      checkArgument(
          !newWatermark.isBefore(currentWatermark), "The watermark must monotonically advance");
      checkArgument(
          newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "The Watermark cannot progress beyond the maximum. Got: %s. Maximum: %s",
          newWatermark,
          BoundedWindow.TIMESTAMP_MAX_VALUE);
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(WatermarkEvent.advanceTo(newWatermark))
              .build();
      return new Builder<>(coder, newEvents, newWatermark);
    }

    /**
     * Advance the processing time by the specified amount.
     *
     * @return A {@link TestStream.Builder} like this one that will advance the processing time by
     *     the specified amount after all earlier events have completed.
     */
    public Builder<T> advanceProcessingTime(Duration amount) {
      checkArgument(
          amount.getMillis() > 0,
          "Must advance the processing time by a positive amount. Got: ",
          amount);
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(ProcessingTimeEvent.advanceBy(amount))
              .build();
      return new Builder<>(coder, newEvents, currentWatermark);
    }

    /**
     * Advance the watermark to infinity, completing this {@link TestStream}. Future calls to the
     * same builder will not affect the returned {@link TestStream}.
     */
    public TestStream<T> advanceWatermarkToInfinity() {
      ImmutableList<Event<T>> newEvents =
          ImmutableList.<Event<T>>builder()
              .addAll(events)
              .add(WatermarkEvent.advanceTo(BoundedWindow.TIMESTAMP_MAX_VALUE))
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

  /** The types of {@link Event} that are supported by {@link TestStream}. */
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

    /** <b>For internal use only: no backwards compatibility guarantees.</b> */
    @Internal
    public static <T> Event<T> add(Iterable<TimestampedValue<T>> elements) {
      return new AutoValue_TestStream_ElementEvent<>(EventType.ELEMENT, elements);
    }
  }

  /** A {@link Event} that advances the watermark. */
  @AutoValue
  public abstract static class WatermarkEvent<T> implements Event<T> {
    public abstract Instant getWatermark();

    /** <b>For internal use only: no backwards compatibility guarantees.</b> */
    @Internal
    public static <T> Event<T> advanceTo(Instant newWatermark) {
      return new AutoValue_TestStream_WatermarkEvent<>(EventType.WATERMARK, newWatermark);
    }
  }

  /** A {@link Event} that advances the processing time clock. */
  @AutoValue
  public abstract static class ProcessingTimeEvent<T> implements Event<T> {
    public abstract Duration getProcessingTimeAdvance();

    /** <b>For internal use only: no backwards compatibility guarantees.</b> */
    @Internal
    public static <T> Event<T> advanceBy(Duration amount) {
      return new AutoValue_TestStream_ProcessingTimeEvent<>(EventType.PROCESSING_TIME, amount);
    }
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, coder);
  }

  public Coder<T> getValueCoder() {
    return coder;
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
   * <b>For internal use only. No backwards-compatibility guarantees.</b>
   *
   * <p>Builder a test stream directly from events. No validation is performed on watermark
   * monotonicity, etc. This is assumed to be a previously-serialized {@link TestStream} transform
   * that is correct by construction.
   */
  @Internal
  public static <T> TestStream<T> fromRawEvents(Coder<T> coder, List<Event<T>> events) {
    return new TestStream<>(coder, events);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof TestStream)) {
      return false;
    }
    TestStream<?> that = (TestStream<?>) other;

    return getValueCoder().equals(that.getValueCoder()) && getEvents().equals(that.getEvents());
  }

  @Override
  public int hashCode() {
    return Objects.hash(TestStream.class, getValueCoder(), getEvents());
  }

  /** Coder for {@link TestStream}. */
  public static class TestStreamCoder<T> extends StructuredCoder<TestStream<T>> {

    private final TimestampedValue.TimestampedValueCoder<T> elementCoder;

    public static <T> TestStreamCoder<T> of(Coder<T> valueCoder) {
      return new TestStreamCoder<>(valueCoder);
    }

    private TestStreamCoder(Coder<T> valueCoder) {
      this.elementCoder = TimestampedValue.TimestampedValueCoder.of(valueCoder);
    }

    @Override
    public void encode(TestStream<T> value, OutputStream outStream) throws IOException {
      List<Event<T>> events = value.getEvents();
      VarIntCoder.of().encode(events.size(), outStream);

      for (Event event : events) {
        if (event instanceof ElementEvent) {
          outStream.write(event.getType().ordinal());
          Iterable<TimestampedValue<T>> elements = ((ElementEvent) event).getElements();
          VarIntCoder.of().encode(Iterables.size(elements), outStream);
          for (TimestampedValue<T> element : elements) {
            elementCoder.encode(element, outStream);
          }
        } else if (event instanceof WatermarkEvent) {
          outStream.write(event.getType().ordinal());
          Instant watermark = ((WatermarkEvent) event).getWatermark();
          InstantCoder.of().encode(watermark, outStream);
        } else if (event instanceof ProcessingTimeEvent) {
          outStream.write(event.getType().ordinal());
          Duration processingTimeAdvance = ((ProcessingTimeEvent) event).getProcessingTimeAdvance();
          DurationCoder.of().encode(processingTimeAdvance, outStream);
        }
      }
    }

    @Override
    public TestStream<T> decode(InputStream inStream) throws IOException {
      Integer numberOfEvents = VarIntCoder.of().decode(inStream);
      List<Event<T>> events = new ArrayList<>(numberOfEvents);

      for (int i = 0; i < numberOfEvents; i++) {
        EventType eventType = EventType.values()[inStream.read()];
        switch (eventType) {
          case ELEMENT:
            int numElements = VarIntCoder.of().decode(inStream);
            List<TimestampedValue<T>> elements = new ArrayList<>(numElements);
            for (int j = 0; j < numElements; j++) {
              elements.add(elementCoder.decode(inStream));
            }
            events.add(ElementEvent.add(elements));
            break;
          case WATERMARK:
            Instant watermark = InstantCoder.of().decode(inStream);
            events.add(WatermarkEvent.advanceTo(watermark));
            break;
          case PROCESSING_TIME:
            Duration duration = DurationCoder.of().decode(inStream).toDuration();
            events.add(ProcessingTimeEvent.advanceBy(duration));
            break;
          default:
            throw new IllegalStateException("Unknown event type + " + eventType);
        }
      }
      return TestStream.fromRawEvents(elementCoder.getValueCoder(), events);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(elementCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
