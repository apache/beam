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
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class ImpulseTranslator
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  @Override
  public void translate(
      PTransformNode pTransform, RunnerApi.Pipeline pipeline, UnifiedTranslationContext context) {
    TypeInformation<WindowedValue<byte[]>> typeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE),
            context.getPipelineOptions());

    FlinkBoundedSource<byte[]> impulseSource;
    WatermarkStrategy<WindowedValue<byte[]>> watermarkStrategy;
    if (context.isStreaming()) {
      long shutdownAfterIdleSourcesMs =
          context
              .getPipelineOptions()
              .as(FlinkPipelineOptions.class)
              .getShutdownSourcesAfterIdleMs();
      impulseSource = FlinkSource.unboundedImpulse(shutdownAfterIdleSourcesMs);
      watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
    } else {
      impulseSource = FlinkSource.boundedImpulse();
      watermarkStrategy = WatermarkStrategy.noWatermarks();
    }

    SingleOutputStreamOperator<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .fromSource(impulseSource, watermarkStrategy, "Impulse")
            .returns(typeInfo);

    // TODO: is this correct ?
    if (context.isStreaming()) {
      source = source.setParallelism(1);
    }

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getTransform().getOutputsMap().values()), source);
  }
}
