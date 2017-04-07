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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import org.apache.beam.sdk.io.CountingSource.NowTimestampFn;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} that produces longs. When used to produce a
 * {@link IsBounded#BOUNDED bounded} {@link PCollection}, {@link CountingInput} starts at {@code 0}
 * or starting value, and counts up to a specified maximum. When used to produce an
 * {@link IsBounded#UNBOUNDED unbounded} {@link PCollection}, it counts up to {@link Long#MAX_VALUE}
 * and then never produces more output. (In practice, this limit should never be reached.)
 *
 * <p>The bounded {@link CountingInput} is implemented based on {@link OffsetBasedSource} and
 * {@link OffsetBasedSource.OffsetBasedReader}, so it performs efficient initial splitting and it
 * supports dynamic work rebalancing.
 *
 * <p>To produce a bounded {@code PCollection<Long>} starting from {@code 0},
 * use {@link CountingInput#upTo(long)}:
 *
 * <pre>{@code
 * Pipeline p = ...
 * PTransform<PBegin, PCollection<Long>> producer = CountingInput.upTo(1000);
 * PCollection<Long> bounded = p.apply(producer);
 * }</pre>
 *
 * <p>To produce a bounded {@code PCollection<Long>} starting from {@code startOffset},
 * use {@link CountingInput#forSubrange(long, long)} instead.
 *
 * <p>To produce an unbounded {@code PCollection<Long>}, use {@link CountingInput#unbounded()},
 * calling {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to provide values
 * with timestamps other than {@link Instant#now}.
 *
 * <pre>{@code
 * Pipeline p = ...
 *
 * // To create an unbounded producer that uses processing time as the element timestamp.
 * PCollection<Long> unbounded = p.apply(CountingInput.unbounded());
 * // Or, to create an unbounded source that uses a provided function to set the element timestamp.
 * PCollection<Long> unboundedWithTimestamps =
 *     p.apply(CountingInput.unbounded().withTimestampFn(someFn));
 * }</pre>
 */
public class CountingInput {
  /**
   * Creates a {@link BoundedCountingInput} that will produce the specified number of elements,
   * from {@code 0} to {@code numElements - 1}.
   */
  public static BoundedCountingInput upTo(long numElements) {
    checkArgument(numElements >= 0,
        "numElements (%s) must be greater than or equal to 0",
        numElements);
    return new BoundedCountingInput(numElements);
  }

  /**
   * Creates a {@link BoundedCountingInput} that will produce elements
   * starting from {@code startIndex} (inclusive) to {@code endIndex} (exclusive).
   * If {@code startIndex == endIndex}, then no elements will be produced.
   */
  public static BoundedCountingInput forSubrange(long startIndex, long endIndex) {
    checkArgument(endIndex >= startIndex,
        "endIndex (%s) must be greater than or equal to startIndex (%s)",
        endIndex, startIndex);
    return new BoundedCountingInput(startIndex, endIndex);
  }

  /**
   * Creates an {@link UnboundedCountingInput} that will produce numbers starting from {@code 0} up
   * to {@link Long#MAX_VALUE}.
   *
   * <p>After {@link Long#MAX_VALUE}, the transform never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Elements in the resulting {@link PCollection PCollection&lt;Long&gt;} will by default have
   * timestamps corresponding to processing time at element generation, provided by
   * {@link Instant#now}. Use the transform returned by
   * {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to control the output
   * timestamps.
   */
  public static UnboundedCountingInput unbounded() {
    return new UnboundedCountingInput(
        new NowTimestampFn(),
        1L /* Elements per period */,
        Duration.ZERO /* Period length */,
        Optional.<Long>absent() /* Maximum number of records */,
        Optional.<Duration>absent() /* Maximum read duration */);
  }

  /**
   * A {@link PTransform} that will produce a specified number of {@link Long Longs} starting from
   * 0.
   *
   * <pre>{@code
   * PCollection<Long> bounded = p.apply(CountingInput.upTo(10L));
   * }</pre>
   */
  public static class BoundedCountingInput extends PTransform<PBegin, PCollection<Long>> {
    private final long startIndex;
    private final long endIndex;

    private BoundedCountingInput(long numElements) {
      this.endIndex = numElements;
      this.startIndex = 0;
    }

    private BoundedCountingInput(long startIndex, long endIndex) {
      this.endIndex = endIndex;
      this.startIndex = startIndex;
    }

    @Override
    public PCollection<Long> expand(PBegin begin) {
      return begin.apply(Read.from(CountingSource.createSourceForSubrange(startIndex, endIndex)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      if (startIndex == 0) {
            builder.add(DisplayData.item("upTo", endIndex)
                .withLabel("Count Up To"));
      } else {
            builder.add(DisplayData.item("startAt", startIndex).withLabel("Count Starting At"))
                    .add(DisplayData.item("upTo", endIndex).withLabel("Count Up To"));
        }
    }
  }

  /**
   * A {@link PTransform} that will produce numbers starting from {@code 0} up to
   * {@link Long#MAX_VALUE}.
   *
   * <p>After {@link Long#MAX_VALUE}, the transform never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Elements in the resulting {@link PCollection PCollection&lt;Long&gt;} will by default have
   * timestamps corresponding to processing time at element generation, provided by
   * {@link Instant#now}. Use the transform returned by
   * {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to control the output
   * timestamps.
   */
  public static class UnboundedCountingInput extends PTransform<PBegin, PCollection<Long>> {
    private final SerializableFunction<Long, Instant> timestampFn;
    private final long elementsPerPeriod;
    private final Duration period;
    private final Optional<Long> maxNumRecords;
    private final Optional<Duration> maxReadTime;

    private UnboundedCountingInput(
        SerializableFunction<Long, Instant> timestampFn,
        long elementsPerPeriod,
        Duration period,
        Optional<Long> maxNumRecords,
        Optional<Duration> maxReadTime) {
      this.timestampFn = timestampFn;
      this.elementsPerPeriod = elementsPerPeriod;
      this.period = period;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
    }

    /**
     * Returns an {@link UnboundedCountingInput} like this one, but where output elements have the
     * timestamp specified by the timestampFn.
     *
     * <p>Note that the timestamps produced by {@code timestampFn} may not decrease.
     */
    public UnboundedCountingInput withTimestampFn(SerializableFunction<Long, Instant> timestampFn) {
      return new UnboundedCountingInput(
          timestampFn, elementsPerPeriod, period, maxNumRecords, maxReadTime);
    }

    /**
     * Returns an {@link UnboundedCountingInput} like this one, but that will read at most the
     * specified number of elements.
     *
     * <p>A bounded amount of elements will be produced by the result transform, and the result
     * {@link PCollection} will be {@link IsBounded#BOUNDED bounded}.
     */
    public UnboundedCountingInput withMaxNumRecords(long maxRecords) {
      checkArgument(
          maxRecords > 0, "MaxRecords must be a positive (nonzero) value. Got %s", maxRecords);
      return new UnboundedCountingInput(
          timestampFn, elementsPerPeriod, period, Optional.of(maxRecords), maxReadTime);
    }

    /**
     * Returns an {@link UnboundedCountingInput} like this one, but with output production limited
     * to an aggregate rate of no more than the number of elements per the period length.
     *
     * <p>Note that when there are multiple splits, each split outputs independently. This may lead
     * to elements not being produced evenly across time, though the aggregate rate will still
     * approach the specified rate.
     *
     * <p>A duration of {@link Duration#ZERO} will produce output as fast as possible.
     */
    public UnboundedCountingInput withRate(long numElements, Duration periodLength) {
      return new UnboundedCountingInput(
          timestampFn, numElements, periodLength, maxNumRecords, maxReadTime);
    }

    /**
     * Returns an {@link UnboundedCountingInput} like this one, but that will read for at most the
     * specified amount of time.
     *
     * <p>A bounded amount of elements will be produced by the result transform, and the result
     * {@link PCollection} will be {@link IsBounded#BOUNDED bounded}.
     */
    public UnboundedCountingInput withMaxReadTime(Duration readTime) {
      checkNotNull(readTime, "ReadTime cannot be null");
      return new UnboundedCountingInput(
          timestampFn, elementsPerPeriod, period, maxNumRecords, Optional.of(readTime));
    }

    @SuppressWarnings("deprecation")
    @Override
    public PCollection<Long> expand(PBegin begin) {
      Unbounded<Long> read =
          Read.from(
              CountingSource.createUnbounded()
                  .withTimestampFn(timestampFn)
                  .withRate(elementsPerPeriod, period));
      if (!maxNumRecords.isPresent() && !maxReadTime.isPresent()) {
        return begin.apply(read);
      } else if (maxNumRecords.isPresent() && !maxReadTime.isPresent()) {
        return begin.apply(read.withMaxNumRecords(maxNumRecords.get()));
      } else if (!maxNumRecords.isPresent() && maxReadTime.isPresent()) {
        return begin.apply(read.withMaxReadTime(maxReadTime.get()));
      } else {
        return begin.apply(
            read.withMaxReadTime(maxReadTime.get()).withMaxNumRecords(maxNumRecords.get()));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("timestampFn", timestampFn.getClass())
        .withLabel("Timestamp Function"));

      if (maxReadTime.isPresent()) {
        builder.add(DisplayData.item("maxReadTime", maxReadTime.get())
          .withLabel("Maximum Read Time"));
      }

      if (maxNumRecords.isPresent()) {
        builder.add(DisplayData.item("maxRecords", maxNumRecords.get())
          .withLabel("Maximum Read Records"));
      }
    }
  }
}
