/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

/**
 * A {@link TransformEvaluator} that ignores all input and produces no output. The result of
 * invoking {@link #finishBundle()} on this evaluator is to return an
 * {@link InProcessTransformResult} with no elements and a timestamp hold equal to
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
  public InProcessTransformResult finishBundle() throws Exception {
    return StepTransformResult.withHold(transform, BoundedWindow.TIMESTAMP_MIN_VALUE)
        .build();
  }
}

