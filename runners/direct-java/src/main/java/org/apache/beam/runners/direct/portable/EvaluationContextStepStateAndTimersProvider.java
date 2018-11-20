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
package org.apache.beam.runners.direct.portable;

import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.local.StructuralKey;

/** A {@link StepStateAndTimers.Provider} that uses an {@link EvaluationContext}. */
class EvaluationContextStepStateAndTimersProvider implements StepStateAndTimers.Provider {
  public static StepStateAndTimers.Provider forContext(EvaluationContext context) {
    return new EvaluationContextStepStateAndTimersProvider(context);
  }

  private final EvaluationContext context;

  private EvaluationContextStepStateAndTimersProvider(EvaluationContext context) {
    this.context = context;
  }

  @Override
  public <K> StepStateAndTimers<K> forStepAndKey(PTransformNode transform, StructuralKey<K> key) {
    return context.getStateAndTimers(transform, key);
  }
}
