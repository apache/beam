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
package org.apache.beam.runners.direct;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.joda.time.Instant;

/**
 * An immutable {@link TransformResult}.
 */
@AutoValue
public abstract class StepTransformResult implements TransformResult {

  public static Builder withHold(AppliedPTransform<?, ?, ?> transform, Instant watermarkHold) {
    return new Builder(transform, watermarkHold);
  }

  public static Builder withoutHold(AppliedPTransform<?, ?, ?> transform) {
    return new Builder(transform, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  @Override
  public TransformResult withLogicalMetricUpdates(MetricUpdates metricUpdates) {
    return new AutoValue_StepTransformResult(
        getTransform(),
        getOutputBundles(),
        getUnprocessedElements(),
        getAggregatorChanges(),
        metricUpdates,
        getWatermarkHold(),
        getState(),
        getTimerUpdate(),
        getOutputTypes());
  }

  /**
   * A builder for creating instances of {@link StepTransformResult}.
   */
  public static class Builder {
    private final AppliedPTransform<?, ?, ?> transform;
    private final ImmutableList.Builder<UncommittedBundle<?>> bundlesBuilder;
    private final ImmutableList.Builder<WindowedValue<?>> unprocessedElementsBuilder;
    private MetricUpdates metricUpdates;
    private CopyOnAccessInMemoryStateInternals<?> state;
    private TimerUpdate timerUpdate;
    private AggregatorContainer.Mutator aggregatorChanges;
    private final Set<OutputType> producedOutputs;
    private final Instant watermarkHold;

    private Builder(AppliedPTransform<?, ?, ?> transform, Instant watermarkHold) {
      this.transform = transform;
      this.watermarkHold = watermarkHold;
      this.bundlesBuilder = ImmutableList.builder();
      this.producedOutputs = EnumSet.noneOf(OutputType.class);
      this.unprocessedElementsBuilder = ImmutableList.builder();
      this.timerUpdate = TimerUpdate.builder(null).build();
      this.metricUpdates = MetricUpdates.EMPTY;
    }

    public StepTransformResult build() {
      return new AutoValue_StepTransformResult(
          transform,
          bundlesBuilder.build(),
          unprocessedElementsBuilder.build(),
          aggregatorChanges,
          metricUpdates,
          watermarkHold,
          state,
          timerUpdate,
          producedOutputs);
    }

    public Builder withAggregatorChanges(AggregatorContainer.Mutator aggregatorChanges) {
      this.aggregatorChanges = aggregatorChanges;
      return this;
    }

    public Builder withMetricUpdates(MetricUpdates metricUpdates) {
      this.metricUpdates = metricUpdates;
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

    public Builder addUnprocessedElements(WindowedValue<?>... unprocessed) {
      unprocessedElementsBuilder.addAll(Arrays.asList(unprocessed));
      return this;
    }

    public Builder addUnprocessedElements(Iterable<? extends WindowedValue<?>> unprocessed) {
      unprocessedElementsBuilder.addAll(unprocessed);
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

    public Builder withAdditionalOutput(OutputType producedAdditionalOutput) {
      producedOutputs.add(producedAdditionalOutput);
      return this;
    }
  }
}
