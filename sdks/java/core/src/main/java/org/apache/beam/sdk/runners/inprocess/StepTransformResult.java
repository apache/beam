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
package org.apache.beam.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.runners.inprocess.InMemoryWatermarkManager.TimerUpdate;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

import java.util.Collection;

import javax.annotation.Nullable;

/**
 * An immutable {@link InProcessTransformResult}.
 */
public class StepTransformResult implements InProcessTransformResult {
  private final AppliedPTransform<?, ?, ?> transform;
  private final Iterable<? extends UncommittedBundle<?>> bundles;
  @Nullable private final CopyOnAccessInMemoryStateInternals<?> state;
  private final TimerUpdate timerUpdate;
  @Nullable private final CounterSet counters;
  private final Instant watermarkHold;

  private StepTransformResult(
      AppliedPTransform<?, ?, ?> transform,
      Iterable<? extends UncommittedBundle<?>> outputBundles,
      CopyOnAccessInMemoryStateInternals<?> state,
      TimerUpdate timerUpdate,
      CounterSet counters,
      Instant watermarkHold) {
    this.transform = checkNotNull(transform);
    this.bundles = checkNotNull(outputBundles);
    this.state = state;
    this.timerUpdate = checkNotNull(timerUpdate);
    this.counters = counters;
    this.watermarkHold = checkNotNull(watermarkHold);
  }

  @Override
  public Iterable<? extends UncommittedBundle<?>> getOutputBundles() {
    return bundles;
  }

  @Override
  public CounterSet getCounters() {
    return counters;
  }

  @Override
  public AppliedPTransform<?, ?, ?> getTransform() {
    return transform;
  }

  @Override
  public Instant getWatermarkHold() {
    return watermarkHold;
  }

  @Nullable
  @Override
  public CopyOnAccessInMemoryStateInternals<?> getState() {
    return state;
  }

  @Override
  public TimerUpdate getTimerUpdate() {
    return timerUpdate;
  }

  public static Builder withHold(AppliedPTransform<?, ?, ?> transform, Instant watermarkHold) {
    return new Builder(transform, watermarkHold);
  }

  public static Builder withoutHold(AppliedPTransform<?, ?, ?> transform) {
    return new Builder(transform, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(StepTransformResult.class)
        .add("transform", transform)
        .toString();
  }

  /**
   * A builder for creating instances of {@link StepTransformResult}.
   */
  public static class Builder {
    private final AppliedPTransform<?, ?, ?> transform;
    private final ImmutableList.Builder<UncommittedBundle<?>> bundlesBuilder;
    private CopyOnAccessInMemoryStateInternals<?> state;
    private TimerUpdate timerUpdate;
    private CounterSet counters;
    private final Instant watermarkHold;

    private Builder(AppliedPTransform<?, ?, ?> transform, Instant watermarkHold) {
      this.transform = transform;
      this.watermarkHold = watermarkHold;
      this.bundlesBuilder = ImmutableList.builder();
      this.timerUpdate = TimerUpdate.builder(null).build();
    }

    public StepTransformResult build() {
      return new StepTransformResult(
          transform,
          bundlesBuilder.build(),
          state,
          timerUpdate,
          counters,
          watermarkHold);
    }

    public Builder withCounters(CounterSet counters) {
      this.counters = counters;
      return this;
    }

    public Builder withState(CopyOnAccessInMemoryStateInternals<?> state) {
      this.state = state;
      return this;
    }

    public Builder withTimerUpdate(TimerUpdate timerUpdate) {
      this.timerUpdate = timerUpdate;
      return this;
    }

    public Builder addOutput(
        UncommittedBundle<?> outputBundle, UncommittedBundle<?>... outputBundles) {
      bundlesBuilder.add(outputBundle);
      bundlesBuilder.add(outputBundles);
      return this;
    }

    public Builder addOutput(Collection<UncommittedBundle<?>> outputBundles) {
      bundlesBuilder.addAll(outputBundles);
      return this;
    }
  }
}
