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
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class WindowAssignTranslator<T>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  @Override
  public void translate(
      PTransformNode transform, RunnerApi.Pipeline pipeline, UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    WindowingStrategy<T, BoundedWindow> windowingStrategy =
        (WindowingStrategy<T, BoundedWindow>)
            context.getWindowingStrategy(pipeline, inputPCollectionId);

    TypeInformation<WindowedValue<T>> typeInfo = context.getTypeInfo(pipeline, outputPCollectionId);

    DataStream<WindowedValue<T>> inputDataStream = context.getDataStreamOrThrow(inputPCollectionId);

    WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

    FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
        new FlinkAssignWindows<>(windowFn);

    String fullName = pTransform.getUniqueName();
    SingleOutputStreamOperator<WindowedValue<T>> outputDataStream =
        inputDataStream
            .flatMap(assignWindowsFunction)
            .name(fullName)
            .uid(fullName)
            .returns(typeInfo);

    context.addDataStream(outputPCollectionId, outputDataStream);
  }
}
