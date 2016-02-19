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
package com.google.cloud.dataflow.sdk.runners.inprocess.evaluator;

import com.google.cloud.dataflow.sdk.io.Read.Unbounded;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.StepTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluatorFactory;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
public class UnboundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  /*
   * An evaluator for a Source is stateful, to ensure the CheckpointMark is properly persisted.
   * Evaluators are cached here to ensure that the checkpoint mark is appropriately reused
   * and any splits are honored.
   */
  private final Map<EvaluatorKey, UnboundedReadEvaluator<?>> sourceEvaluators = new HashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return getTransformEvaluator((AppliedPTransform) application, evaluationContext);
  }

  private <OutputT> TransformEvaluator<?> getTransformEvaluator(
      final AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    EvaluatorKey key = new EvaluatorKey(transform, evaluationContext);
    @SuppressWarnings("unchecked")
    UnboundedReadEvaluator<OutputT> result =
        (UnboundedReadEvaluator<OutputT>) sourceEvaluators.get(key);
    if (result == null) {
      result = new UnboundedReadEvaluator<OutputT>(transform, evaluationContext);
      sourceEvaluators.put(key, result);
    }
    return result;
  }

  private static class UnboundedReadEvaluator<OutputT> implements TransformEvaluator<Object> {
    private static final int ARBITRARY_MAX_ELEMENTS = 10;
    private final AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform;
    private final InProcessEvaluationContext evaluationContext;
    private CheckpointMark checkpointMark;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform,
        InProcessEvaluationContext evaluationContext) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.checkpointMark = null;
    }

    @Override
    public void processElement(WindowedValue<Object> element) {}

    @Override
    public InProcessTransformResult finishBundle() throws IOException {
      UncommittedBundle<OutputT> output = evaluationContext.createRootBundle(transform.getOutput());
      UnboundedReader<OutputT> reader =
          createReader(
              transform.getTransform().getSource(), evaluationContext.getPipelineOptions());
      int numElements = 0;
      if (reader.start()) {
        do {
          output.add(
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp()));
          numElements++;
        } while (numElements < ARBITRARY_MAX_ELEMENTS && reader.advance());
      }
      checkpointMark = reader.getCheckpointMark();
      checkpointMark.finalizeCheckpoint();
      // TODO: When exercising create initial splits, make this the minimum across all existing
      // readers
      return StepTransformResult.withHold(transform, reader.getWatermark())
          .addOutput(output)
          .build();
    }

    private <CheckpointMarkT extends CheckpointMark> UnboundedReader<OutputT> createReader(
        UnboundedSource<OutputT, CheckpointMarkT> source, PipelineOptions options) {
      @SuppressWarnings("unchecked")
      CheckpointMarkT mark = (CheckpointMarkT) checkpointMark;
      return source.createReader(options, mark);
    }
  }
}

