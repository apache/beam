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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.joda.time.Instant;

/**
 * <b><i>(Experimental)</i></b> Static utility methods and provided implementations for
 * {@link OutputTimeFn}.
 */
@Experimental(Experimental.Kind.OUTPUT_TIME)
public class OutputTimeFns {
  /**
   * The policy of outputting at the earliest of the input timestamps for non-late input data
   * that led to a computed value.
   *
   * <p>For example, suppose <i>v</i><sub>1</sub> through <i>v</i><sub>n</sub> are all on-time
   * elements being aggregated via some function {@code f} into
   * {@code f}(<i>v</i><sub>1</sub>, ..., <i>v</i><sub>n</sub>. When emitted, the output
   * timestamp of the result will be the earliest of the event time timestamps
   *
   * <p>If data arrives late, it has no effect on the output timestamp.
   */
  public static OutputTimeFn<BoundedWindow> outputAtEarliestInputTimestamp() {
    return new OutputAtEarliestInputTimestamp();
  }

  /**
   * The policy of holding the watermark to the latest of the input timestamps
   * for non-late input data that led to a computed value.
   *
   * <p>For example, suppose <i>v</i><sub>1</sub> through <i>v</i><sub>n</sub> are all on-time
   * elements being aggregated via some function {@code f} into
   * {@code f}(<i>v</i><sub>1</sub>, ..., <i>v</i><sub>n</sub>. When emitted, the output
   * timestamp of the result will be the latest of the event time timestamps
   *
   * <p>If data arrives late, it has no effect on the output timestamp.
   */
  public static OutputTimeFn<BoundedWindow> outputAtLatestInputTimestamp() {
    return new OutputAtLatestInputTimestamp();
  }

  /**
   * The policy of outputting with timestamps at the end of the window.
   *
   * <p>Note that this output timestamp depends only on the window. See
   * {#link dependsOnlyOnWindow()}.
   *
   * <p>When windows merge, instead of using {@link OutputTimeFn#combine} to obtain an output
   * timestamp for the results in the new window, it is mandatory to obtain a new output
   * timestamp from {@link OutputTimeFn#assignOutputTime} with the new window and an arbitrary
   * timestamp (because it is guaranteed that the timestamp is irrelevant).
   *
   * <p>For non-merging window functions, this {@link OutputTimeFn} works transparently.
   */
  public static OutputTimeFn<BoundedWindow> outputAtEndOfWindow() {
    return new OutputAtEndOfWindow();
  }

  /**
   * Applies the given {@link OutputTimeFn} to the given output times, obtaining
   * the output time for a value computed. See {@link OutputTimeFn#combine} for
   * a full specification.
   *
   * @throws IllegalArgumentException if {@code outputTimes} is empty.
   */
  public static Instant combineOutputTimes(
      OutputTimeFn<?> outputTimeFn, Iterable<? extends Instant> outputTimes) {
    checkArgument(
        !Iterables.isEmpty(outputTimes),
        "Collection of output times must not be empty in %s.combineOutputTimes",
        OutputTimeFns.class.getName());

    @Nullable
    Instant combinedOutputTime = null;
    for (Instant outputTime : outputTimes) {
      combinedOutputTime =
          combinedOutputTime == null
              ? outputTime : outputTimeFn.combine(combinedOutputTime, outputTime);
    }
    return combinedOutputTime;
  }

  /**
   * See {@link #outputAtEarliestInputTimestamp}.
   */
  private static class OutputAtEarliestInputTimestamp extends OutputTimeFn.Defaults<BoundedWindow> {
    @Override
    public Instant assignOutputTime(Instant inputTimestamp, BoundedWindow window) {
      return inputTimestamp;
    }

    @Override
    public Instant combine(Instant outputTime, Instant otherOutputTime) {
      return Ordering.natural().min(outputTime, otherOutputTime);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. The result of any combine will be the earliest input timestamp.
     */
    @Override
    public boolean dependsOnlyOnEarliestInputTimestamp() {
      return true;
    }
  }

  /**
   * See {@link #outputAtLatestInputTimestamp}.
   */
  private static class OutputAtLatestInputTimestamp extends OutputTimeFn.Defaults<BoundedWindow> {
    @Override
    public Instant assignOutputTime(Instant inputTimestamp, BoundedWindow window) {
      return inputTimestamp;
    }

    @Override
    public Instant combine(Instant outputTime, Instant otherOutputTime) {
      return Ordering.natural().max(outputTime, otherOutputTime);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code false}.
     */
    @Override
    public boolean dependsOnlyOnEarliestInputTimestamp() {
      return false;
    }
  }

  private static class OutputAtEndOfWindow extends OutputTimeFn.DependsOnlyOnWindow<BoundedWindow> {

    /**
     *{@inheritDoc}
     *
     *@return {@code window.maxTimestamp()}.
     */
    @Override
    protected Instant assignOutputTime(BoundedWindow window) {
      return window.maxTimestamp();
    }

    @Override
    public String toString() {
      return getClass().getCanonicalName();
    }
  }

  public static RunnerApi.OutputTime toProto(OutputTimeFn<?> outputTimeFn) {
    if (outputTimeFn instanceof OutputAtEarliestInputTimestamp) {
      return RunnerApi.OutputTime.EARLIEST_IN_PANE;
    } else if (outputTimeFn instanceof OutputAtLatestInputTimestamp) {
      return RunnerApi.OutputTime.LATEST_IN_PANE;
    } else if (outputTimeFn instanceof OutputAtEndOfWindow) {
      return RunnerApi.OutputTime.END_OF_WINDOW;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot convert %s to %s: %s",
              OutputTimeFn.class.getCanonicalName(),
              RunnerApi.OutputTime.class.getCanonicalName(),
              outputTimeFn));
    }
  }

  public static OutputTimeFn<?> fromProto(RunnerApi.OutputTime proto) {
    switch (proto) {
      case EARLIEST_IN_PANE:
        return OutputTimeFns.outputAtEarliestInputTimestamp();
      case LATEST_IN_PANE:
        return OutputTimeFns.outputAtLatestInputTimestamp();
      case END_OF_WINDOW:
        return OutputTimeFns.outputAtEndOfWindow();
      case UNRECOGNIZED:
      default:
        // Whether or not it is proto that cannot recognize it (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this OutputTime means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.OutputTime.class.getCanonicalName(),
                OutputTimeFn.class.getCanonicalName(),
                proto));
    }
  }
}
