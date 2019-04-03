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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.createOutputMap;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

/** Translates a bounded portable pipeline into a Spark job. */
public class SparkBatchPortablePipelineTranslator {

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  interface PTransformTranslator {

    /** Translates transformNode from Beam into the Spark context. */
    void translate(
        PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context);
  }

  public Set<String> knownUrns() {
    return urnToTransformTranslator.keySet();
  }

  public SparkBatchPortablePipelineTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator> translatorMap = ImmutableMap.builder();
    translatorMap.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        SparkBatchPortablePipelineTranslator::translateImpulse);
    translatorMap.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        SparkBatchPortablePipelineTranslator::translateGroupByKey);
    translatorMap.put(
        ExecutableStage.URN, SparkBatchPortablePipelineTranslator::translateExecutableStage);
    this.urnToTransformTranslator = translatorMap.build();
  }

  /** Translates pipeline from Beam into the Spark context. */
  public void translate(final RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transformNode : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(
              transformNode.getTransform().getSpec().getUrn(),
              SparkBatchPortablePipelineTranslator::urnNotFound)
          .translate(transformNode, pipeline, context);
    }
  }

  private static void urnNotFound(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Transform %s has unknown URN %s",
            transformNode.getId(), transformNode.getTransform().getSpec().getUrn()));
  }

  private static void translateImpulse(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    BoundedDataset<byte[]> output =
        new BoundedDataset<>(
            Collections.singletonList(new byte[0]), context.getSparkContext(), ByteArrayCoder.of());
    context.pushDataset(getOutputId(transformNode), output);
  }

  private static <K, V> void translateGroupByKey(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {

    RunnerApi.Components components = pipeline.getComponents();
    String inputId = getInputId(transformNode);
    PCollection inputPCollection = components.getPcollectionsOrThrow(inputId);
    Dataset inputDataset = context.popDataset(inputId);
    JavaRDD<WindowedValue<KV<K, V>>> inputRdd = ((BoundedDataset<KV<K, V>>) inputDataset).getRDD();
    PCollectionNode inputPCollectionNode = PipelineNode.pCollection(inputId, inputPCollection);
    WindowedValueCoder<KV<K, V>> inputCoder;
    try {
      inputCoder =
          (WindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(inputPCollectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
    Coder<K> inputKeyCoder = inputKvCoder.getKeyCoder();
    Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
    WindowingStrategy windowingStrategy = getWindowingStrategy(inputId, components);
    WindowFn<Object, BoundedWindow> windowFn = windowingStrategy.getWindowFn();
    WindowedValue.WindowedValueCoder<V> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(inputValueCoder, windowFn.windowCoder());

    JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupedByKeyAndWindow;
    if (windowingStrategy.getWindowFn().isNonMerging()
        && windowingStrategy.getTimestampCombiner() == TimestampCombiner.END_OF_WINDOW) {
      // we can have a memory sensitive translation for non-merging windows
      groupedByKeyAndWindow =
          GroupNonMergingWindowsFunctions.groupByKeyAndWindow(
              inputRdd, inputKeyCoder, inputValueCoder, windowingStrategy);
    } else {
      Partitioner partitioner = getPartitioner(context);
      JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupedByKeyOnly =
          GroupCombineFunctions.groupByKeyOnly(inputRdd, inputKeyCoder, wvCoder, partitioner);
      // for batch, GroupAlsoByWindow uses an in-memory StateInternals.
      groupedByKeyAndWindow =
          groupedByKeyOnly.flatMap(
              new SparkGroupAlsoByWindowViaOutputBufferFn<>(
                  windowingStrategy,
                  new TranslationUtils.InMemoryStateInternalsFactory<>(),
                  SystemReduceFn.buffering(inputValueCoder),
                  context.serializablePipelineOptions,
                  AggregatorsAccumulator.getInstance()));
    }
    context.pushDataset(getOutputId(transformNode), new BoundedDataset<>(groupedByKeyAndWindow));
  }

  private static <InputT, OutputT> void translateExecutableStage(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transformNode.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String inputPCollectionId = stagePayload.getInput();
    Dataset inputDataset = context.popDataset(inputPCollectionId);
    JavaRDD<WindowedValue<InputT>> inputRdd = ((BoundedDataset<InputT>) inputDataset).getRDD();
    Map<String, String> outputs = transformNode.getTransform().getOutputsMap();
    BiMap<String, Integer> outputMap = createOutputMap(outputs.values());

    SparkExecutableStageFunction<InputT> function =
        new SparkExecutableStageFunction<>(stagePayload, context.jobInfo, outputMap);
    JavaRDD<RawUnionValue> staged = inputRdd.mapPartitions(function);

    for (String outputId : outputs.values()) {
      JavaRDD<WindowedValue<OutputT>> outputRdd =
          staged.flatMap(new SparkExecutableStageExtractionFunction<>(outputMap.get(outputId)));
      context.pushDataset(outputId, new BoundedDataset<>(outputRdd));
    }
  }

  @Nullable
  private static Partitioner getPartitioner(SparkTranslationContext context) {
    Long bundleSize =
        context.serializablePipelineOptions.get().as(SparkPipelineOptions.class).getBundleSize();
    return (bundleSize > 0)
        ? null
        : new HashPartitioner(context.getSparkContext().defaultParallelism());
  }

  private static String getInputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getInputsMap().values());
  }

  private static String getOutputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getOutputsMap().values());
  }
}
