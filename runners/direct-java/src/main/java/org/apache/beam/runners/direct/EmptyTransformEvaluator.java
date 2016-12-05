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

import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A {@link TransformEvaluator} that ignores all input and produces no output. The result of
 * invoking {@link #finishBundle()} on this evaluator is to return an
 * {@link TransformResult} with no elements and a timestamp hold equal to
 * {@link BoundedWindow#TIMESTAMP_MIN_VALUE}. Because the result contains no elements, this hold
 * will not affect the watermark.
 */
final class EmptyTransformEvaluator<T> implements TransformEvaluator<T> {
  public static <T> TransformEvaluator<T> create(AppliedPTransform<?, ?, ?> transform) {
    return new EmptyTransformEvaluator<T>(transform);
  }

  private final AppliedPTransform<?, ?, ?> transform;

  private EmptyTransformEvaluator(AppliedPTransform<?, ?, ?> transform) {
    this.transform = transform;
  }

  @Override
  public void processElement(WindowedValue<T> element) throws Exception {}

  @Override
  public TransformResult<T> finishBundle() throws Exception {
    return StepTransformResult.<T>withHold(transform, BoundedWindow.TIMESTAMP_MIN_VALUE)
        .build();
  }
}
