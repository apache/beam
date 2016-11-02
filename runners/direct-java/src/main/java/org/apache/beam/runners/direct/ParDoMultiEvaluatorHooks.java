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

import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

/** Support for {@link ParDo.BoundMulti} in {@link ParDoEvaluatorFactory}. */
class ParDoMultiEvaluatorHooks<InputT, OutputT>
    implements ParDoEvaluatorFactory.TransformHooks<
        InputT, OutputT, PCollectionTuple, ParDo.BoundMulti<InputT, OutputT>> {
  @Override
  public DoFn<InputT, OutputT> getDoFn(ParDo.BoundMulti<InputT, OutputT> transform) {
    return transform.getNewFn();
  }

  @Override
  public ParDoEvaluator<InputT, OutputT> createParDoEvaluator(
      EvaluationContext evaluationContext,
      AppliedPTransform<PCollection<InputT>, PCollectionTuple, ParDo.BoundMulti<InputT, OutputT>>
          application,
      DirectStepContext stepContext,
      DoFn<InputT, OutputT> fnLocal) {
    ParDo.BoundMulti<InputT, OutputT> transform = application.getTransform();
    return ParDoEvaluator.create(
        evaluationContext,
        stepContext,
        application,
        application.getInput().getWindowingStrategy(),
        fnLocal,
        transform.getSideInputs(),
        transform.getMainOutputTag(),
        transform.getSideOutputTags().getAll(),
        application.getOutput().getAll());
  }
}
