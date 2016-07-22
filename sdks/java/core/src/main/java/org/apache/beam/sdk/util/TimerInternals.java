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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * Encapsulate interaction with time within the execution environment.
 *
 * <p>This class allows setting and deleting timers, and also retrieving an
 * estimate of the current time.
 */
public interface TimerInternals {

  /**
   * Writes out a timer to be fired when the watermark reaches the given
   * timestamp.
   *
   * <p>The combination of {@code namespace}, {@code timestamp} and {@code domain} uniquely
   * identify a timer. Multiple timers set for the same parameters can be safely deduplicated.
   */
  void setTimer(TimerData timerKey);

  /**
   * Deletes the given timer.
   */
  void deleteTimer(TimerData timerKey);

  /**
   * Returns the current timestamp in the {@link TimeDomain#PROCESSING_TIME} time domain.
   */
  Instant currentProcessingTime();

  /**
   * Returns the current timestamp in the {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} time
   * domain or {@code null} if unknown.
   */
  @Nullable
  Instant currentSynchronizedProcessingTime();

  /**
   * Return the current, local input watermark timestamp for this computation
   * in the {@link TimeDomain#EVENT_TIME} time domain.
   *
   * <p>This value:
   * <ol>
   * <li>Is never {@literal null}, but may be {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   * <li>Is monotonically increasing.
   * <li>May differ between workers due to network and other delays.
   * <li>Will never be ahead of the global input watermark for this computation. But it
   * may be arbitrarily behind the global input watermark.
   * <li>Any element with a timestamp before the local input watermark can be considered
   * 'locally late' and be subject to special processing or be dropped entirely.
   * </ol>
   *
   * <p>Note that because the local input watermark can be behind the global input watermark,
   * it is possible for an element to be considered locally on-time even though it is
   * globally late.
   */
  Instant currentInputWatermarkTime();

  /**
   * Return the current, local output watermark timestamp for this computation
   * in the {@link TimeDomain#EVENT_TIME} time domain. Return {@code null} if unknown.
   *
   * <p>This value:
   * <ol>
   * <li>Is monotonically increasing.
   * <li>Will never be ahead of {@link #currentInputWatermarkTime} as returned above.
   * <li>May differ between workers due to network and other delays.
   * <li>However will never be behind the global input watermark for any following computation.
   * </ol>
   *
   * <p> In pictures:
   * <pre>
   *  |              |       |       |       |
   *  |              |   D   |   C   |   B   |   A
   *  |              |       |       |       |
   * GIWM     <=    GOWM <= LOWM <= LIWM <= GIWM
   * (next stage)
   * -------------------------------------------------> event time
   * </pre>
   * where
   * <ul>
   * <li> LOWM = local output water mark.
   * <li> GOWM = global output water mark.
   * <li> GIWM = global input water mark.
   * <li> LIWM = local input water mark.
   * <li> A = A globally on-time element.
   * <li> B = A globally late, but locally on-time element.
   * <li> C = A locally late element which may still contribute to the timestamp of a pane.
   * <li> D = A locally late element which cannot contribute to the timestamp of a pane.
   * </ul>
   *
   * <p>Note that if a computation emits an element which is not before the current output watermark
   * then that element will always appear locally on-time in all following computations. However,
   * it is possible for an element emitted before the current output watermark to appear locally
   * on-time in a following computation. Thus we must be careful to never assume locally late data
   * viewed on the output of a computation remains locally late on the input of a following
   * computation.
   */
  @Nullable
  Instant currentOutputWatermarkTime();

  /**
   * Data about a timer as represented within {@link TimerInternals}.
   */
  public static class TimerData implements Comparable<TimerData> {
    private final StateNamespace namespace;
    private final Instant timestamp;
    private final TimeDomain domain;

    private TimerData(StateNamespace namespace, Instant timestamp, TimeDomain domain) {
      this.namespace = checkNotNull(namespace);
      this.timestamp = checkNotNull(timestamp);
      this.domain = checkNotNull(domain);
    }

    public StateNamespace getNamespace() {
      return namespace;
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    public TimeDomain getDomain() {
      return domain;
    }

    /**
     * Construct the {@code TimerKey} for the given parameters.
     */
    public static TimerData of(StateNamespace namespace, Instant timestamp, TimeDomain domain) {
      return new TimerData(namespace, timestamp, domain);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof TimerData)) {
        return false;
      }

      TimerData that = (TimerData) obj;
      return Objects.equals(this.domain, that.domain)
          && this.timestamp.isEqual(that.timestamp)
          && Objects.equals(this.namespace, that.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(domain, timestamp, namespace);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("namespace", namespace)
          .add("timestamp", timestamp)
          .add("domain", domain)
          .toString();
    }

    @Override
    public int compareTo(TimerData o) {
      return Long.compare(timestamp.getMillis(), o.getTimestamp().getMillis());
    }
  }

  /**
   * A {@link Coder} for {@link TimerData}.
   */
  public class TimerDataCoder extends StandardCoder<TimerData> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();
    private final Coder<? extends BoundedWindow> windowCoder;

    public static TimerDataCoder of(Coder<? extends BoundedWindow> windowCoder) {
      return new TimerDataCoder(windowCoder);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public static TimerDataCoder of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      checkArgument(components.size() == 1, "Expecting 1 components, got %s", components.size());
      return of((Coder<? extends BoundedWindow>) components.get(0));
    }

    private TimerDataCoder(Coder<? extends BoundedWindow> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TimerData timer, OutputStream outStream, Context context)
        throws CoderException, IOException {
      Context nestedContext = context.nested();
      STRING_CODER.encode(timer.namespace.stringKey(), outStream, nestedContext);
      INSTANT_CODER.encode(timer.timestamp, outStream, nestedContext);
      STRING_CODER.encode(timer.domain.name(), outStream, nestedContext);
    }

    @Override
    public TimerData decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Context nestedContext = context.nested();
      StateNamespace namespace =
          StateNamespaces.fromString(STRING_CODER.decode(inStream, nestedContext), windowCoder);
      Instant timestamp = INSTANT_CODER.decode(inStream, nestedContext);
      TimeDomain domain = TimeDomain.valueOf(STRING_CODER.decode(inStream, nestedContext));
      return TimerData.of(namespace, timestamp, domain);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic("window coder must be deterministic", windowCoder);
    }
  }
}
