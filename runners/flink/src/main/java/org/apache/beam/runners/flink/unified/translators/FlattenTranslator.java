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

import java.util.Map;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;

public class FlattenTranslator<T>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  @Override
  public void translate(
      PTransformNode transform, RunnerApi.Pipeline pipeline, UnifiedTranslationContext context) {
    Map<String, String> allInputs = transform.getTransform().getInputsMap();

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataStreamSource<String> dummySource =
          context.getExecutionEnvironment().fromElements("dummy");

      DataStream<WindowedValue<T>> result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE),
                      context.getPipelineOptions()));
      context.addDataStream(
          Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), result);

    } else {
      DataStream<T> result = null;

      // Determine DataStreams that we use as input several times. For those, we need to uniquify
      // input streams because Flink seems to swallow watermarks when we have a union of one and
      // the same stream.
      HashMultiset<DataStream<T>> inputCounts = HashMultiset.create();
      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        inputCounts.add(current, 1);
      }

      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        final int timesRequired = inputCounts.count(current);
        if (timesRequired > 1) {
          current =
              current.flatMap(
                  new FlatMapFunction<T, T>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(T t, Collector<T> collector) {
                      collector.collect(t);
                    }
                  });
        }
        result = (result == null) ? current : result.union(current);
      }

      context.addDataStream(
          Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()),
          Objects.requireNonNull(result));
    }
  }
}
