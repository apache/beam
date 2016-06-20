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

import java.util.Objects;

/**
 * A (Transform, Pipeline Execution) key for stateful evaluators.
 *
 * Source evaluators are stateful to ensure data is not read multiple times. Evaluators are cached
 * to ensure that the reader is not restarted if the evaluator is retriggered. An
 * {@link EvaluatorKey} is used to ensure that multiple Pipelines can be executed without sharing
 * the same evaluators.
 */
final class EvaluatorKey {
  private final AppliedPTransform<?, ?, ?> transform;
  private final EvaluationContext context;

  public EvaluatorKey(AppliedPTransform<?, ?, ?> transform, EvaluationContext context) {
    this.transform = transform;
    this.context = context;
  }

  @Override
  public int hashCode() {
    return Objects.hash(transform, context);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof EvaluatorKey)) {
      return false;
    }
    EvaluatorKey that = (EvaluatorKey) other;
    return Objects.equals(this.transform, that.transform)
        && Objects.equals(this.context, that.context);
  }
}
