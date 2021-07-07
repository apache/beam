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
package org.apache.beam.runners.core;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Encapsulate interaction with time within the execution environment.
 *
 * <p>This class allows setting and deleting timers, and also retrieving an estimate of the current
 * time.
 */
public interface TimerInternals {

  /**
   * Sets a timer to be fired when the current time in the specified time domain reaches the target
   * timestamp.
   *
   * <p>The combination of {@code namespace} and {@code timerId} uniquely identify a timer.
   *
   * <p>If a timer is set and then set again before it fires, later settings should clear the prior
   * setting.
   *
   * <p>It is an error to set a timer for two different time domains.
   */
  void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain);

  /**
   * @deprecated use {@link #setTimer(StateNamespace, String, String, Instant, Instant,
   *     TimeDomain)}.
   */
  @Deprecated
  void setTimer(TimerData timerData);

  /**
   * Deletes the given timer.
   *
   * <p>A timer's ID is enforced to be unique in validation of a {@link DoFn}, but runners often
   * manage timers for different time domains in very different ways, thus the {@link TimeDomain} is
   * a required parameter.
   */
  void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain);

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId);

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  void deleteTimer(TimerData timerKey);

  /** Returns the current timestamp in the {@link TimeDomain#PROCESSING_TIME} time domain. */
  Instant currentProcessingTime();

  /**
   * Returns the current timestamp in the {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} time
   * domain or {@code null} if unknown.
   */
  @Nullable
  Instant currentSynchronizedProcessingTime();

  /**
   * Return the current, local input watermark timestamp for this computation in the {@link
   * TimeDomain#EVENT_TIME} time domain.
   *
   * <p>This value:
   *
   * <ol>
   *   <li>Is never {@literal null}, but may be {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   *   <li>Is monotonically increasing.
   *   <li>May differ between workers due to network and other delays.
   *   <li>Will never be ahead of the global input watermark for this computation. But it may be
   *       arbitrarily behind the global input watermark.
   *   <li>Any element with a timestamp before the local input watermark can be considered 'locally
   *       late' and be subject to special processing or be dropped entirely.
   * </ol>
   *
   * <p>Note that because the local input watermark can be behind the global input watermark, it is
   * possible for an element to be considered locally on-time even though it is globally late.
   */
  Instant currentInputWatermarkTime();

  /**
   * Return the current, local output watermark timestamp for this computation in the {@link
   * TimeDomain#EVENT_TIME} time domain. Return {@code null} if unknown.
   *
   * <p>This value:
   *
   * <ol>
   *   <li>Is monotonically increasing.
   *   <li>Will never be ahead of {@link #currentInputWatermarkTime} as returned above.
   *   <li>May differ between workers due to network and other delays.
   *   <li>However will never be behind the global input watermark for any following computation.
   * </ol>
   *
   * <p>In pictures:
   *
   * <pre>{@code
   *  |              |       |       |       |
   *  |              |   D   |   C   |   B   |   A
   *  |              |       |       |       |
   * GIWM     <=    GOWM <= LOWM <= LIWM <= GIWM
   * (next stage)
   * -------------------------------------------------> event time
   * }</pre>
   *
   * <p>where
   *
   * <ul>
   *   <li>LOWM = local output water mark.
   *   <li>GOWM = global output water mark.
   *   <li>GIWM = global input water mark.
   *   <li>LIWM = local input water mark.
   *   <li>A = A globally on-time element.
   *   <li>B = A globally late, but locally on-time element.
   *   <li>C = A locally late element which may still contribute to the timestamp of a pane.
   *   <li>D = A locally late element which cannot contribute to the timestamp of a pane.
   * </ul>
   *
   * <p>Note that if a computation emits an element which is not before the current output watermark
   * then that element will always appear locally on-time in all following computations. However, it
   * is possible for an element emitted before the current output watermark to appear locally
   * on-time in a following computation. Thus we must be careful to never assume locally late data
   * viewed on the output of a computation remains locally late on the input of a following
   * computation.
   */
  @Nullable
  Instant currentOutputWatermarkTime();

  /** Data about a timer as represented within {@link TimerInternals}. */
  @AutoValue
  abstract class TimerData implements Comparable<TimerData> {

    public abstract String getTimerId();

    public abstract String getTimerFamilyId();

    public abstract StateNamespace getNamespace();

    public abstract Instant getTimestamp();

    /**
     * Timestamp the timer assigns to outputted elements from {@link
     * org.apache.beam.sdk.transforms.DoFn.OnTimer} method. For event time timers, output watermark
     * is held at this timestamp until the timer fires.
     */
    public abstract Instant getOutputTimestamp();

    public abstract TimeDomain getDomain();

    // When adding a new field, make sure to add it to the compareTo() method.

    /** Construct a {@link TimerData} for the given parameters. */
    public static TimerData of(
        String timerId,
        StateNamespace namespace,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain domain) {
      return new AutoValue_TimerInternals_TimerData(
          timerId, "", namespace, timestamp, outputTimestamp, domain);
    }

    /**
     * Construct a {@link TimerData} for the given parameters except for {@code outputTimestamp}.
     * {@code outputTimestamp} is set to timer {@code timestamp}.
     */
    public static TimerData of(
        String timerId,
        String timerFamilyId,
        StateNamespace namespace,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain domain) {
      return new AutoValue_TimerInternals_TimerData(
          timerId, timerFamilyId, namespace, timestamp, outputTimestamp, domain);
    }

    /**
     * Construct a {@link TimerData} for the given parameters except for timer ID. Timer ID is
     * deterministically generated from the {@code timestamp} and {@code domain}.
     */
    public static TimerData of(
        StateNamespace namespace, Instant timestamp, Instant outputTimestamp, TimeDomain domain) {
      String timerId =
          new StringBuilder()
              .append(domain.ordinal())
              .append(':')
              .append(timestamp.getMillis())
              .toString();
      return of(timerId, namespace, timestamp, outputTimestamp, domain);
    }

    /**
     * {@inheritDoc}.
     *
     * <p>Used for sorting {@link TimerData} by timestamp. Furthermore, we compare timers by all the
     * other fields so that {@code compareTo()} only returns 0 when {@code equals()} returns 0. This
     * ensures consistent sort order.
     */
    @Override
    public int compareTo(TimerData that) {
      if (this.equals(that)) {
        return 0;
      }
      ComparisonChain chain =
          ComparisonChain.start()
              .compare(this.getTimestamp(), that.getTimestamp())
              .compare(this.getOutputTimestamp(), that.getOutputTimestamp())
              .compare(this.getDomain(), that.getDomain())
              .compare(this.getTimerId(), that.getTimerId())
              .compare(this.getTimerFamilyId(), that.getTimerFamilyId());
      if (chain.result() == 0 && !this.getNamespace().equals(that.getNamespace())) {
        // Obtaining the stringKey may be expensive; only do so if required
        chain = chain.compare(getNamespace().stringKey(), that.getNamespace().stringKey());
      }
      return chain.result();
    }
  }

  /** A {@link Coder} for {@link TimerData}. */
  class TimerDataCoderV2 extends StructuredCoder<TimerData> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();
    private final Coder<? extends BoundedWindow> windowCoder;

    public static TimerDataCoderV2 of(Coder<? extends BoundedWindow> windowCoder) {
      return new TimerDataCoderV2(windowCoder);
    }

    private TimerDataCoderV2(Coder<? extends BoundedWindow> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TimerData timer, OutputStream outStream) throws CoderException, IOException {
      STRING_CODER.encode(timer.getTimerId(), outStream);
      STRING_CODER.encode(timer.getTimerFamilyId(), outStream);
      STRING_CODER.encode(timer.getNamespace().stringKey(), outStream);
      INSTANT_CODER.encode(timer.getTimestamp(), outStream);
      INSTANT_CODER.encode(timer.getOutputTimestamp(), outStream);
      STRING_CODER.encode(timer.getDomain().name(), outStream);
    }

    @Override
    public TimerData decode(InputStream inStream) throws CoderException, IOException {
      String timerId = STRING_CODER.decode(inStream);
      String timerFamilyId = STRING_CODER.decode(inStream);
      StateNamespace namespace =
          StateNamespaces.fromString(STRING_CODER.decode(inStream), windowCoder);
      Instant timestamp = INSTANT_CODER.decode(inStream);
      Instant outputTimestamp = INSTANT_CODER.decode(inStream);
      TimeDomain domain = TimeDomain.valueOf(STRING_CODER.decode(inStream));
      return TimerData.of(timerId, timerFamilyId, namespace, timestamp, outputTimestamp, domain);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "window coder must be deterministic", windowCoder);
    }
  }

  /**
   * A {@link Coder} for {@link TimerData}. To make it encoding and decoding backward compatible for
   * DataFlow
   */
  class TimerDataCoder extends StructuredCoder<TimerData> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();
    private final Coder<? extends BoundedWindow> windowCoder;

    public static TimerDataCoder of(Coder<? extends BoundedWindow> windowCoder) {
      return new TimerDataCoder(windowCoder);
    }

    private TimerDataCoder(Coder<? extends BoundedWindow> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TimerData timer, OutputStream outStream) throws CoderException, IOException {
      STRING_CODER.encode(timer.getTimerId(), outStream);
      STRING_CODER.encode(timer.getNamespace().stringKey(), outStream);
      INSTANT_CODER.encode(timer.getTimestamp(), outStream);
      STRING_CODER.encode(timer.getDomain().name(), outStream);
    }

    @Override
    public TimerData decode(InputStream inStream) throws CoderException, IOException {
      String timerId = STRING_CODER.decode(inStream);
      StateNamespace namespace =
          StateNamespaces.fromString(STRING_CODER.decode(inStream), windowCoder);
      Instant timestamp = INSTANT_CODER.decode(inStream);
      TimeDomain domain = TimeDomain.valueOf(STRING_CODER.decode(inStream));
      return TimerData.of(timerId, namespace, timestamp, timestamp, domain);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "window coder must be deterministic", windowCoder);
    }
  }
}
