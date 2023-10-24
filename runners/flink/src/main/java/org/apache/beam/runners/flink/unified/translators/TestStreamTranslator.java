
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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.TestStreamTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestStreamSource;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;


public class TestStreamTranslator<T>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {
  @Override
  public void translate(
      PTransformNode transform, Pipeline pipeline, UnifiedTranslationContext context) {
    SerializableFunction<byte[], TestStream<T>> testStreamDecoder =
        bytes -> {
          try {
            RunnerApi.TestStreamPayload testStreamPayload =
                RunnerApi.TestStreamPayload.parseFrom(bytes);
            @SuppressWarnings("unchecked")
            TestStream<T> testStream =
                (TestStream<T>)
                    TestStreamTranslation.testStreamFromProtoPayload(
                        testStreamPayload, context.getComponents(pipeline));
            return testStream;
          } catch (Exception e) {
            throw new RuntimeException("Can't decode TestStream payload.", e);
          }
        };

    RunnerApi.PTransform pTransform = transform.getTransform();
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());
    Coder<WindowedValue<T>> coder =
      context.getWindowedInputCoder(pipeline, outputPCollectionId);

    DataStream<WindowedValue<T>> source =
        context
            .getExecutionEnvironment()
            .addSource(
                new TestStreamSource<>(
                    testStreamDecoder, pTransform.getSpec().getPayload().toByteArray()),
                new CoderTypeInformation<>(coder, context.getPipelineOptions()));

    context.addDataStream(outputPCollectionId, source);
  }
}
