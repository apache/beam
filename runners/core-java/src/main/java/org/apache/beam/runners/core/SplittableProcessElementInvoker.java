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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A runner-specific hook for invoking a {@link DoFn.ProcessElement} method for a splittable {@link
 * DoFn}, in particular, allowing the runner to access the {@link RestrictionTracker}.
 */
public abstract class SplittableProcessElementInvoker<
    InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT> {
  /** Specifies how to resume a splittable {@link DoFn.ProcessElement} call. */
  public class Result {
    private final @Nullable RestrictionT residualRestriction;
    private final DoFn.ProcessContinuation continuation;
    private final @Nullable Instant futureOutputWatermark;
    private final @Nullable WatermarkEstimatorStateT futureWatermarkEstimatorState;

    @SuppressFBWarnings(
        value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification = "Spotbugs incorrectly thinks continuation is marked @Nullable")
    public Result(
        @Nullable RestrictionT residualRestriction,
        DoFn.ProcessContinuation continuation,
        @Nullable Instant futureOutputWatermark,
        @Nullable WatermarkEstimatorStateT futureWatermarkEstimatorState) {
      checkArgument(continuation != null, "continuation must not be null");
      this.continuation = continuation;
      this.residualRestriction = residualRestriction;
      this.futureOutputWatermark = futureOutputWatermark;
      this.futureWatermarkEstimatorState = futureWatermarkEstimatorState;
    }

    /**
     * Can be {@code null} only if {@link #getContinuation} when there is no more work to resume.
     */
    public @Nullable RestrictionT getResidualRestriction() {
      return residualRestriction;
    }

    public DoFn.ProcessContinuation getContinuation() {
      return continuation;
    }

    public @Nullable Instant getFutureOutputWatermark() {
      return futureOutputWatermark;
    }

    public @Nullable WatermarkEstimatorStateT getFutureWatermarkEstimatorState() {
      return futureWatermarkEstimatorState;
    }
  }

  /**
   * Invokes the {@link DoFn.ProcessElement} method using the given {@link DoFnInvoker} for the
   * original {@link DoFn}, on the given element and with the given {@link RestrictionTracker}.
   *
   * @return Information on how to resume the call: residual restriction, a {@link
   *     DoFn.ProcessContinuation}, and a future output watermark.
   */
  public abstract Result invokeProcessElement(
      DoFnInvoker<InputT, OutputT> invoker,
      WindowedValue<InputT> element,
      RestrictionTracker<RestrictionT, PositionT> tracker,
      WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator);
}
