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
package org.apache.beam.runners.flink.unified.translators;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;

public class ReadSourceTranslator<T>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  private final BoundedReadSourceTranslator<T> boundedTranslator =
      new BoundedReadSourceTranslator<>();
  private final UnboundedReadSourceTranslator<T> unboundedTranslator =
      new UnboundedReadSourceTranslator<>();

  /**
   * Transform types from SDK types to runner types. The runner uses byte array representation for
   * non {@link ModelCoders} coders.
   *
   * @param inCoder the input coder (SDK-side)
   * @param outCoder the output coder (runner-side)
   * @param value encoded value
   * @param <InputT> SDK-side type
   * @param <OutputT> runer-side type
   * @return re-encoded {@link WindowedValue}
   */
  public static <InputT, OutputT> WindowedValue<OutputT> intoWireTypes(
      Coder<WindowedValue<InputT>> inCoder,
      Coder<WindowedValue<OutputT>> outCoder,
      WindowedValue<InputT> value) {

    try {
      return CoderUtils.decodeFromByteArray(outCoder, CoderUtils.encodeToByteArray(inCoder, value));
    } catch (CoderException ex) {
      throw new IllegalStateException("Could not transform element into wire types", ex);
    }
  }

  @Override
  public void translate(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    RunnerApi.ReadPayload payload;
    try {
      payload = RunnerApi.ReadPayload.parseFrom(transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse ReadPayload from transform", e);
    }

    if (payload.getIsBounded() == RunnerApi.IsBounded.Enum.BOUNDED) {
      boundedTranslator.translate(transform, pipeline, context);
    } else {
      unboundedTranslator.translate(transform, pipeline, context);
    }
  }
}
