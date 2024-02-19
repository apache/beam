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
package org.apache.beam.runners.samza.translation;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.runtime.WindowAssignOp;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.samza.operators.MessageStream;

/**
 * Translates {@link org.apache.beam.sdk.transforms.windowing.Window.Assign} to Samza {@link
 * WindowAssignOp}.
 */
class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {
  @Override
  public void translate(
      Window.Assign<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);

    @SuppressWarnings("unchecked")
    final WindowFn<T, ?> windowFn = (WindowFn<T, ?>) output.getWindowingStrategy().getWindowFn();

    final MessageStream<OpMessage<T>> inputStream = ctx.getMessageStream(ctx.getInput(transform));

    final MessageStream<OpMessage<T>> outputStream =
        inputStream.flatMapAsync(OpAdapter.adapt(new WindowAssignOp<>(windowFn), ctx));

    ctx.registerMessageStream(output, outputStream);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    final RunnerApi.WindowIntoPayload payload;
    try {
      payload =
          RunnerApi.WindowIntoPayload.parseFrom(transform.getTransform().getSpec().getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          String.format("failed to parse WindowIntoPayload: %s", transform.getId()), e);
    }

    @SuppressWarnings("unchecked")
    final WindowFn<T, ?> windowFn =
        (WindowFn<T, ?>) WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn());

    final MessageStream<OpMessage<T>> inputStream = ctx.getOneInputMessageStream(transform);

    final MessageStream<OpMessage<T>> outputStream =
        inputStream.flatMapAsync(OpAdapter.adapt(new WindowAssignOp<>(windowFn), ctx));

    ctx.registerMessageStream(ctx.getOutputId(transform), outputStream);
  }
}
