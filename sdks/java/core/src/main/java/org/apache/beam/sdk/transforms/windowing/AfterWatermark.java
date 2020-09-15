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
package org.apache.beam.sdk.transforms.windowing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@code AfterWatermark} triggers fire based on progress of the system watermark. This time is a
 * lower-bound, sometimes heuristically established, on event times that have been fully processed
 * by the pipeline.
 *
 * <p>For sources that provide non-heuristic watermarks (e.g. PubsubIO when using arrival times as
 * event times), the watermark is a strict guarantee that no data with an event time earlier than
 * that watermark will ever be observed in the pipeline. In this case, it's safe to assume that any
 * pane triggered by an {@code AfterWatermark} trigger with a reference point at or beyond the end
 * of the window will be the last pane ever for that window.
 *
 * <p>For sources that provide heuristic watermarks (e.g. PubsubIO when using user-supplied event
 * times), the watermark itself becomes an <i>estimate</i> that no data with an event time earlier
 * than that watermark (i.e. "late data") will ever be observed in the pipeline. These heuristics
 * can often be quite accurate, but the chance of seeing late data for any given window is non-zero.
 * Thus, if absolute correctness over time is important to your use case, you may want to consider
 * using a trigger that accounts for late data. The default trigger, {@code
 * Repeatedly.forever(AfterWatermark.pastEndOfWindow())}, which fires once when the watermark passes
 * the end of the window and then immediately thereafter when any late data arrives, is one such
 * example.
 *
 * <p>The watermark is the clock that defines {@link TimeDomain#EVENT_TIME}.
 *
 * <p>Additionally firings before or after the watermark can be requested by calling {@code
 * AfterWatermark.pastEndOfWindow.withEarlyFirings(OnceTrigger)} or {@code
 * AfterWatermark.pastEndOfWindow.withLateFirings(OnceTrigger)}.
 */
@Experimental(Kind.TRIGGER)
public class AfterWatermark {

  private static final String TO_STRING = "AfterWatermark.pastEndOfWindow()";

  // Static factory class.
  private AfterWatermark() {}

  /** Creates a trigger that fires when the watermark passes the end of the window. */
  public static FromEndOfWindow pastEndOfWindow() {
    return new FromEndOfWindow();
  }

  /** @see AfterWatermark */
  public static class AfterWatermarkEarlyAndLate extends Trigger {

    private final OnceTrigger earlyTrigger;
    private final @Nullable OnceTrigger lateTrigger;

    public OnceTrigger getEarlyTrigger() {
      return earlyTrigger;
    }

    public OnceTrigger getLateTrigger() {
      return lateTrigger;
    }

    @SuppressWarnings("unchecked")
    private AfterWatermarkEarlyAndLate(OnceTrigger earlyTrigger, OnceTrigger lateTrigger) {
      super(
          lateTrigger == null
              ? ImmutableList.of(earlyTrigger)
              : ImmutableList.of(earlyTrigger, lateTrigger));
      this.earlyTrigger = checkNotNull(earlyTrigger, "earlyTrigger should not be null");
      this.lateTrigger = lateTrigger;
    }

    public AfterWatermarkEarlyAndLate withEarlyFirings(OnceTrigger earlyTrigger) {
      return new AfterWatermarkEarlyAndLate(earlyTrigger, lateTrigger);
    }

    public AfterWatermarkEarlyAndLate withLateFirings(OnceTrigger lateTrigger) {
      return new AfterWatermarkEarlyAndLate(earlyTrigger, lateTrigger);
    }

    @Override
    public Trigger getContinuationTrigger() {
      return new AfterWatermarkEarlyAndLate(
          earlyTrigger.getContinuationTrigger(),
          lateTrigger == null ? null : lateTrigger.getContinuationTrigger());
    }

    @Override
    protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
      throw new UnsupportedOperationException(
          "Should not call getContinuationTrigger(List<Trigger>)");
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      // Even without an early or late trigger, we'll still produce a firing at the watermark.
      return window.maxTimestamp();
    }

    /** @return true if there is no late firing set up, otherwise false */
    @Override
    public boolean mayFinish() {
      return lateTrigger == null;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(TO_STRING);

      if (!(earlyTrigger instanceof Never.NeverTrigger)) {
        builder.append(".withEarlyFirings(").append(earlyTrigger).append(")");
      }

      if (lateTrigger != null && !(lateTrigger instanceof Never.NeverTrigger)) {
        builder.append(".withLateFirings(").append(lateTrigger).append(")");
      }

      return builder.toString();
    }
  }

  /** A watermark trigger targeted relative to the end of the window. */
  public static class FromEndOfWindow extends OnceTrigger {

    private FromEndOfWindow() {
      super(null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
     * given {@code Trigger} fires before the watermark has passed the end of the window.
     */
    public AfterWatermarkEarlyAndLate withEarlyFirings(OnceTrigger earlyFirings) {
      checkNotNull(earlyFirings, "Must specify the trigger to use for early firings");
      return new AfterWatermarkEarlyAndLate(earlyFirings, null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
     * given {@code Trigger} fires after the watermark has passed the end of the window.
     */
    public AfterWatermarkEarlyAndLate withLateFirings(OnceTrigger lateFirings) {
      checkNotNull(lateFirings, "Must specify the trigger to use for late firings");
      return new AfterWatermarkEarlyAndLate(Never.ever(), lateFirings);
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      return window.maxTimestamp();
    }

    @Override
    protected FromEndOfWindow getContinuationTrigger(List<Trigger> continuationTriggers) {
      return this;
    }

    @Override
    public String toString() {
      return TO_STRING;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj instanceof FromEndOfWindow;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }
  }
}
