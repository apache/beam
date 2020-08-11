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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.InMemoryBundleFinalizer;
import org.apache.beam.runners.core.InMemoryBundleFinalizer.Finalization;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/** An immutable {@link TransformResult}. */
@AutoValue
abstract class StepTransformResult<InputT> implements TransformResult<InputT> {

  public static <InputT> Builder<InputT> withHold(
      AppliedPTransform<?, ?, ?> transform, Instant watermarkHold) {
    return new Builder(transform, watermarkHold);
  }

  public static <InputT> Builder<InputT> withoutHold(AppliedPTransform<?, ?, ?> transform) {
    return new Builder(transform, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  @Override
  public TransformResult<InputT> withLogicalMetricUpdates(MetricUpdates metricUpdates) {
    return new AutoValue_StepTransformResult(
        getTransform(),
        getOutputBundles(),
        getUnprocessedElements(),
        metricUpdates,
        getWatermarkHold(),
        getState(),
        getTimerUpdate(),
        getOutputTypes(),
        getBundleFinalizations());
  }

  /** A builder for creating instances of {@link StepTransformResult}. */
  public static class Builder<InputT> {
    private final AppliedPTransform<?, ?, ?> transform;
    private final ImmutableList.Builder<UncommittedBundle<?>> bundlesBuilder;
    private final ImmutableList.Builder<WindowedValue<InputT>> unprocessedElementsBuilder;
    private MetricUpdates metricUpdates;
    private CopyOnAccessInMemoryStateInternals state;
    private TimerUpdate timerUpdate;
    private List<Finalization> finalizations;
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
      this.finalizations = Collections.EMPTY_LIST;
    }

    public StepTransformResult<InputT> build() {
      return new AutoValue_StepTransformResult<>(
          transform,
          bundlesBuilder.build(),
          unprocessedElementsBuilder.build(),
          metricUpdates,
          watermarkHold,
          state,
          timerUpdate,
          producedOutputs,
          finalizations);
    }

    public Builder<InputT> withMetricUpdates(MetricUpdates metricUpdates) {
      this.metricUpdates = metricUpdates;
      return this;
    }

    public Builder<InputT> withState(CopyOnAccessInMemoryStateInternals state) {
      this.state = state;
      return this;
    }

    public Builder<InputT> withTimerUpdate(TimerUpdate timerUpdate) {
      this.timerUpdate = timerUpdate;
      return this;
    }

    public Builder<InputT> addUnprocessedElements(WindowedValue<InputT>... unprocessed) {
      unprocessedElementsBuilder.addAll(Arrays.asList(unprocessed));
      return this;
    }

    public Builder<InputT> withBundleFinalizations(
        List<InMemoryBundleFinalizer.Finalization> finalizations) {
      this.finalizations = finalizations;
      return this;
    }

    public Builder<InputT> addUnprocessedElements(
        Iterable<? extends WindowedValue<InputT>> unprocessed) {
      unprocessedElementsBuilder.addAll(unprocessed);
      return this;
    }

    public Builder<InputT> addOutput(
        UncommittedBundle<?> outputBundle, UncommittedBundle<?>... outputBundles) {
      bundlesBuilder.add(outputBundle);
      bundlesBuilder.add(outputBundles);
      return this;
    }

    public Builder<InputT> addOutput(Collection<UncommittedBundle<?>> outputBundles) {
      bundlesBuilder.addAll(outputBundles);
      return this;
    }

    public Builder<InputT> withAdditionalOutput(OutputType producedAdditionalOutput) {
      producedOutputs.add(producedAdditionalOutput);
      return this;
    }
  }
}
