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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} that produces longs starting from the given value, and either up to the
 * given limit or until {@link Long#MAX_VALUE} / until the given time elapses.
 *
 * <p>The bounded {@link GenerateSequence} is implemented based on {@link OffsetBasedSource} and
 * {@link OffsetBasedSource.OffsetBasedReader}, so it performs efficient initial splitting and it
 * supports dynamic work rebalancing.
 *
 * <p>To produce a bounded {@code PCollection<Long>}:
 *
 * <pre>{@code
 * Pipeline p = ...
 * PCollection<Long> bounded = p.apply(GenerateSequence.from(0).to(1000));
 * }</pre>
 *
 * <p>To produce an unbounded {@code PCollection<Long>}, simply do not specify {@link #to(long)},
 * calling {@link #withTimestampFn(SerializableFunction)} to provide values with timestamps other
 * than {@link Instant#now}.
 *
 * <pre>{@code
 * Pipeline p = ...
 *
 * // To use processing time as the element timestamp.
 * PCollection<Long> unbounded = p.apply(GenerateSequence.from(0));
 * // Or, to use a provided function to set the element timestamp.
 * PCollection<Long> unboundedWithTimestamps =
 *     p.apply(GenerateSequence.from(0).withTimestampFn(someFn));
 * }</pre>
 *
 * <p>In all cases, the sequence of numbers is generated in parallel, so there is no inherent
 * ordering between the generated values - it is only guaranteed that all values in the given range
 * will be present in the resulting {@link PCollection}.
 */
@AutoValue
public abstract class GenerateSequence extends PTransform<PBegin, PCollection<Long>> {
  abstract long getFrom();

  abstract long getTo();

  abstract @Nullable SerializableFunction<Long, Instant> getTimestampFn();

  abstract long getElementsPerPeriod();

  abstract @Nullable Duration getPeriod();

  abstract @Nullable Duration getMaxReadTime();

  abstract Builder toBuilder();

  @AutoValue.Builder
  @Experimental
  abstract static class Builder
      implements ExternalTransformBuilder<
          External.ExternalConfiguration, PBegin, PCollection<Long>> {
    abstract Builder setFrom(long from);

    abstract Builder setTo(long to);

    abstract Builder setTimestampFn(SerializableFunction<Long, Instant> timestampFn);

    abstract Builder setElementsPerPeriod(long elementsPerPeriod);

    abstract Builder setPeriod(Duration period);

    abstract Builder setMaxReadTime(Duration maxReadTime);

    abstract GenerateSequence build();

    @Override
    public GenerateSequence buildExternal(External.ExternalConfiguration config) {
      Preconditions.checkNotNull(config.start, "Parameters 'from' must not be null.");
      setFrom(config.start);
      setTo(-1);
      setElementsPerPeriod(0);
      if (config.stop != null) {
        setTo(config.stop);
      }
      if (config.period != null) {
        setPeriod(Duration.millis(config.period));
      }
      if (config.maxReadTime != null) {
        setMaxReadTime(Duration.millis(config.maxReadTime));
      }
      if (config.elementsPerPeriod != null) {
        setElementsPerPeriod(config.elementsPerPeriod);
      }
      return build();
    }
  }

  /** Exposes GenerateSequence as an external transform for cross-language usage. */
  @Experimental
  @AutoService(ExternalTransformRegistrar.class)
  public static class External implements ExternalTransformRegistrar {

    public static final String URN = "beam:external:java:generate_sequence:v1";

    @Override
    public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
      return ImmutableMap.of(URN, AutoValue_GenerateSequence.Builder.class);
    }

    /** Parameters class to expose the transform to an external SDK. */
    @Experimental
    public static class ExternalConfiguration {
      private Long start;
      private @Nullable Long stop;
      private @Nullable Long period;
      private @Nullable Long maxReadTime;
      private @Nullable Long elementsPerPeriod;

      public void setStart(Long start) {
        this.start = start;
      }

      public void setStop(@Nullable Long stop) {
        this.stop = stop;
      }

      public void setPeriod(@Nullable Long period) {
        this.period = period;
      }

      public void setMaxReadTime(@Nullable Long maxReadTime) {
        this.maxReadTime = maxReadTime;
      }

      public void setElementsPerPeriod(@Nullable Long elementsPerPeriod) {
        this.elementsPerPeriod = elementsPerPeriod;
      }
    }
  }

  /** Specifies the minimum number to generate (inclusive). */
  public static GenerateSequence from(long from) {
    checkArgument(from >= 0, "Value of from must be non-negative, but was: %s", from);
    return new AutoValue_GenerateSequence.Builder()
        .setFrom(from)
        .setTo(-1)
        .setElementsPerPeriod(0)
        .build();
  }

  /** Specifies the maximum number to generate (exclusive). */
  public GenerateSequence to(long to) {
    checkArgument(
        getTo() == -1 || getTo() >= getFrom(), "Degenerate range [%s, %s)", getFrom(), getTo());
    return toBuilder().setTo(to).build();
  }

  /** Specifies the function to use to assign timestamps to the elements. */
  public GenerateSequence withTimestampFn(SerializableFunction<Long, Instant> timestampFn) {
    return toBuilder().setTimestampFn(timestampFn).build();
  }

  /** Specifies to generate at most a given number of elements per a given period. */
  public GenerateSequence withRate(long numElements, Duration periodLength) {
    checkArgument(
        numElements > 0,
        "Number of elements in withRate must be positive, but was: %s",
        numElements);
    checkArgument(periodLength != null, "periodLength can not be null");
    return toBuilder().setElementsPerPeriod(numElements).setPeriod(periodLength).build();
  }

  /** Specifies to stop generating elements after the given time. */
  public GenerateSequence withMaxReadTime(Duration maxReadTime) {
    return toBuilder().setMaxReadTime(maxReadTime).build();
  }

  @Override
  public PCollection<Long> expand(PBegin input) {
    boolean isRangeUnbounded = getTo() < 0;
    boolean usesUnboundedFeatures =
        getTimestampFn() != null || getElementsPerPeriod() > 0 || getMaxReadTime() != null;
    if (!isRangeUnbounded && !usesUnboundedFeatures) {
      // This is the only case when we can use the bounded CountingSource.
      return input.apply(Read.from(CountingSource.createSourceForSubrange(getFrom(), getTo())));
    }

    CountingSource.UnboundedCountingSource source = CountingSource.createUnboundedFrom(getFrom());
    if (getTimestampFn() != null) {
      source = source.withTimestampFn(getTimestampFn());
    }
    if (getElementsPerPeriod() > 0) {
      source = source.withRate(getElementsPerPeriod(), getPeriod());
    }

    Read.Unbounded<Long> readUnbounded = Read.from(source);

    if (getMaxReadTime() == null) {
      if (isRangeUnbounded) {
        return input.apply(readUnbounded);
      } else {
        return input.apply(readUnbounded.withMaxNumRecords(getTo() - getFrom()));
      }
    } else {
      BoundedReadFromUnboundedSource<Long> withMaxReadTime =
          readUnbounded.withMaxReadTime(getMaxReadTime());
      if (isRangeUnbounded) {
        return input.apply(withMaxReadTime);
      } else {
        return input.apply(withMaxReadTime.withMaxNumRecords(getTo() - getFrom()));
      }
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("from", getFrom()).withLabel("Generate sequence from"));
    builder.addIfNotDefault(
        DisplayData.item("to", getTo()).withLabel("Generate sequence to (exclusive)"), -1L);
    builder.addIfNotNull(
        DisplayData.item(
                "timestampFn", getTimestampFn() == null ? null : getTimestampFn().getClass())
            .withLabel("Timestamp Function"));
    builder.addIfNotNull(
        DisplayData.item("maxReadTime", getMaxReadTime()).withLabel("Maximum Read Time"));
    if (getElementsPerPeriod() > 0) {
      builder.add(
          DisplayData.item("elementsPerPeriod", getElementsPerPeriod())
              .withLabel("Elements per period"));
      builder.add(DisplayData.item("period", getPeriod()).withLabel("Period"));
    }
  }
}
