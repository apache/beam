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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A factory for creating instances of {@link TransformEvaluator} for the application of a {@link
 * PTransform}.
 *
 * <p>{@link TransformEvaluatorFactory TransformEvaluatorFactories} will be reused within a single
 * execution of a {@link Pipeline} but will not be reused across executions.
 */
interface TransformEvaluatorFactory {
  /**
   * Create a new {@link TransformEvaluator} for the application of the {@link PTransform}.
   *
   * <p>Any work that must be done before input elements are processed (such as calling {@code
   * DoFn.StartBundle}) must be done before the {@link TransformEvaluator} is made available to the
   * caller.
   *
   * <p>May return null if the application cannot produce an evaluator (for example, it is a {@link
   * Read} {@link PTransform} where all evaluators are in-use).
   *
   * @return An evaluator capable of processing the transform on the bundle, or null if no evaluator
   *     can be constructed.
   * @throws Exception whenever constructing the underlying evaluator throws an exception
   */
  @Nullable
  <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception;

  /**
   * Cleans up any state maintained by this {@link TransformEvaluatorFactory}. Called after a {@link
   * Pipeline} is shut down. No more calls to {@link #forApplication(AppliedPTransform,
   * CommittedBundle)} will be made after a call to {@link #cleanup()}.
   */
  void cleanup() throws Exception;
}
