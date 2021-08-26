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

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A {@link WindowFn} that windows values into sessions separated by periods with no input for at
 * least the duration specified by {@link #getGapDuration()}.
 *
 * <p>For example, in order to window data into session with at least 10 minute gaps in between
 * them:
 *
 * <pre>{@code
 * PCollection<Integer> pc = ...;
 * PCollection<Integer> windowed_pc = pc.apply(
 *   Window.<Integer>into(Sessions.withGapDuration(Duration.standardMinutes(10))));
 * }</pre>
 */
public class Sessions extends WindowFn<Object, IntervalWindow> {
  /** Duration of the gaps between sessions. */
  private final Duration gapDuration;

  /** Creates a {@code Sessions} {@link WindowFn} with the specified gap duration. */
  public static Sessions withGapDuration(Duration gapDuration) {
    return new Sessions(gapDuration);
  }

  /** Creates a {@code Sessions} {@link WindowFn} with the specified gap duration. */
  private Sessions(Duration gapDuration) {
    this.gapDuration = gapDuration;
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    // Assign each element into a window from its timestamp until gapDuration in the
    // future.  Overlapping windows (representing elements within gapDuration of
    // each other) will be merged.
    return Arrays.asList(new IntervalWindow(c.timestamp(), gapDuration));
  }

  @Override
  public void mergeWindows(MergeContext c) throws Exception {
    MergeOverlappingIntervalWindows.mergeWindows(c);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return other instanceof Sessions;
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "%s is only compatible with %s.",
              Sessions.class.getSimpleName(), Sessions.class.getSimpleName()));
    }
  }

  @Override
  public TypeDescriptor<IntervalWindow> getWindowTypeDescriptor() {
    return TypeDescriptor.of(IntervalWindow.class);
  }

  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    throw new UnsupportedOperationException("Sessions is not allowed in side inputs");
  }

  public Duration getGapDuration() {
    return gapDuration;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("gapDuration", gapDuration).withLabel("Session Gap Duration"));
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (!(object instanceof Sessions)) {
      return false;
    }
    Sessions other = (Sessions) object;
    return getGapDuration().equals(other.getGapDuration());
  }

  @Override
  public int hashCode() {
    return Objects.hash(gapDuration);
  }
}
