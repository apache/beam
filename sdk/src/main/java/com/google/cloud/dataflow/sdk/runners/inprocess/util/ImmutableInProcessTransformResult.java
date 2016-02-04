/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess.util;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

import java.util.Collection;

/**
 * An immutable {@link InProcessTransformResult}.
 */
public class ImmutableInProcessTransformResult implements InProcessTransformResult {
  private final AppliedPTransform<?, ?, ?> transform;
  private final Iterable<? extends Bundle<?>> bundles;
  private final CounterSet counters;
  private final Instant watermarkHold;

  private ImmutableInProcessTransformResult(AppliedPTransform<?, ?, ?> transform,
      Iterable<? extends Bundle<?>> outputBundles, CounterSet counters, Instant watermarkHold) {
    this.transform = transform;
    this.bundles = outputBundles;
    this.counters = counters;
    this.watermarkHold = watermarkHold;
  }

  @Override
  public Iterable<? extends Bundle<?>> getBundles() {
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

  public static Builder withHold(AppliedPTransform<?, ?, ?> transform,
      Instant watermarkHold) {
    return new Builder(transform, watermarkHold);
  }

  public static Builder withoutHold(AppliedPTransform<?, ?, ?> transform) {
    return new Builder(transform, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * A builder for creating instances of {@link ImmutableInProcessTransformResult}.
   */
  public static class Builder {
    private final AppliedPTransform<?, ?, ?> transform;
    private final ImmutableList.Builder<Bundle<?>> bundlesBuilder;
    private CounterSet counters;
    private final Instant watermarkHold;

    public Builder(AppliedPTransform<?, ?, ?> transform,
        Instant watermarkHold) {
      this.transform = transform;
      this.watermarkHold = watermarkHold;
      this.bundlesBuilder = ImmutableList.builder();
    }

    public ImmutableInProcessTransformResult build() {
      return new ImmutableInProcessTransformResult(
          transform, bundlesBuilder.build(), counters, watermarkHold);
    }

    public Builder withCounters(CounterSet counters) {
      this.counters = counters;
      return this;
    }

    public Builder addOutput(Bundle<?> outputBundle, Bundle<?>... outputBundles) {
      bundlesBuilder.add(outputBundle);
      bundlesBuilder.add(outputBundles);
      return this;
    }

    public Builder addOutput(Collection<Bundle<?>> outputBundles) {
      bundlesBuilder.addAll(outputBundles);
      return this;
    }
  }
}
