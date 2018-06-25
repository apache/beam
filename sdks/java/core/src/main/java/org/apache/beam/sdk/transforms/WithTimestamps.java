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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} for assigning timestamps to all the elements of a {@link PCollection}.
 *
 * <p>Timestamps are used to assign {@link BoundedWindow Windows} to elements within the
 * {@link Window#into(org.apache.beam.sdk.transforms.windowing.WindowFn)}
 * {@link PTransform}. Assigning timestamps is useful when the input data set comes from a
 * {@link Source} without implicit timestamps (such as
 * {@link org.apache.beam.sdk.io.TextIO.Read TextIO}).
 *
 */
public class WithTimestamps<T> extends PTransform<PCollection<T>, PCollection<T>> {
  /**
   * For a {@link SerializableFunction} {@code fn} from {@code T} to {@link Instant}, outputs a
   * {@link PTransform} that takes an input {@link PCollection PCollection&lt;T&gt;} and outputs a
   * {@link PCollection PCollection&lt;T&gt;} containing every element {@code v} in the input where
   * each element is output with a timestamp obtained as the result of {@code fn.apply(v)}.
   *
   * <p>If the input {@link PCollection} elements have timestamps, the output timestamp for each
   * element must not be before the input element's timestamp minus the value of {@link
   * #getAllowedTimestampSkew()}. If an output timestamp is before this time, the transform will
   * throw an {@link IllegalArgumentException} when executed. Use {@link
   * #withAllowedTimestampSkew(Duration)} to update the allowed skew.
   *
   * <p>CAUTION: Use of {@link #withAllowedTimestampSkew(Duration)} permits elements to be emitted
   * behind the watermark. These elements are considered late, and if behind the {@link
   * Window#withAllowedLateness(Duration) allowed lateness} of a downstream
   * {@link PCollection} may be silently dropped. See https://issues.apache.org/jira/browse/BEAM-644
   * for details on a replacement.
   *
   * <p>Each output element will be in the same windows as the input element. If a new window based
   * on the new output timestamp is desired, apply a new instance of {@link Window#into(WindowFn)}.
   *
   * <p>This transform will fail at execution time with a {@link NullPointerException} if for any
   * input element the result of {@code fn.apply(v)} is {@code null}.
   *
   * <p>Example of use in Java 8:
   * <pre>{@code
   * PCollection<Record> timestampedRecords = records.apply(
   *     WithTimestamps.of((Record rec) -> rec.getInstant());
   * }</pre>
   */
  public static <T> WithTimestamps<T> of(SerializableFunction<T, Instant> fn) {
    return new WithTimestamps<>(fn, Duration.ZERO);
  }

  ///////////////////////////////////////////////////////////////////

  private final SerializableFunction<T, Instant> fn;
  private final Duration allowedTimestampSkew;

  private WithTimestamps(SerializableFunction<T, Instant> fn, Duration allowedTimestampSkew) {
    this.fn = checkNotNull(fn, "WithTimestamps fn cannot be null");
    this.allowedTimestampSkew = allowedTimestampSkew;
  }

  /**
   * Return a new WithTimestamps like this one with updated allowed timestamp skew, which is the
   * maximum duration that timestamps can be shifted backward. Does not modify this object.
   *
   * <p>The default value is {@code Duration.ZERO}, allowing timestamps to only be shifted into the
   * future. For infinite skew, use {@code new Duration(Long.MAX_VALUE)}.
   * @deprecated This method permits a to elements to be emitted behind the watermark. These
   *     elements are considered late, and if behind the
   *     {@link Window#withAllowedLateness(Duration) allowed lateness} of a downstream
   *     {@link PCollection} may be silently dropped. See
   *     https://issues.apache.org/jira/browse/BEAM-644 for details on a replacement.
   */
  @Deprecated
  public WithTimestamps<T> withAllowedTimestampSkew(Duration allowedTimestampSkew) {
    return new WithTimestamps<>(this.fn, allowedTimestampSkew);
  }

  /**
   * Returns the allowed timestamp skew duration, which is the maximum
   * duration that timestamps can be shifted backwards from the timestamp of the input element.
   *
   * @see DoFn#getAllowedTimestampSkew()
   * @deprecated This method permits a to elements to be emitted behind the watermark. These
   *     elements are considered late, and if behind the
   *     {@link Window#withAllowedLateness(Duration) allowed lateness} of a downstream
   *     {@link PCollection} may be silently dropped. See
   *     https://issues.apache.org/jira/browse/BEAM-644 for details on a replacement.
   */
  @Deprecated
  public Duration getAllowedTimestampSkew() {
    return allowedTimestampSkew;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input.apply(
        "AddTimestamps", ParDo.of(new AddTimestampsDoFn<>(fn, allowedTimestampSkew)));
  }

  private static class AddTimestampsDoFn<T> extends DoFn<T, T> {
    private final SerializableFunction<T, Instant> fn;
    private final Duration allowedTimestampSkew;

    public AddTimestampsDoFn(SerializableFunction<T, Instant> fn, Duration allowedTimestampSkew) {
      this.fn = fn;
      this.allowedTimestampSkew = allowedTimestampSkew;
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> r) {
      Instant timestamp = fn.apply(element);
      checkNotNull(
          timestamp, "Timestamps for WithTimestamps cannot be null. Timestamp provided by %s.", fn);
      r.outputWithTimestamp(element, timestamp);
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedTimestampSkew;
    }
  }
}
