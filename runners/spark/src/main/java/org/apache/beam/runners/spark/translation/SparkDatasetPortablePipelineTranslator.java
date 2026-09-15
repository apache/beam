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
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getInputId;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getOutputId;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowedValueCoder;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.hasUnboundedPCollections;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowedValues.WindowedValueCoder;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import scala.Tuple2;

/**
 * Translates a bounded portable pipeline into Spark Dataset operations.
 *
 * <p>Executable stages run through the Fn API bridge of {@link SparkExecutableStageFunction} inside
 * {@code mapPartitions}. Side inputs are collected and broadcast. Pipelines with unbounded input,
 * user state, or timers fail at translation.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkDatasetPortablePipelineTranslator
    implements SparkPortablePipelineTranslator<SparkDatasetTranslationContext> {

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  interface PTransformTranslator {
    void translate(
        PTransformNode transformNode,
        RunnerApi.Pipeline pipeline,
        SparkDatasetTranslationContext context);
  }

  public SparkDatasetPortablePipelineTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator> translatorMap = ImmutableMap.builder();
    translatorMap.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        SparkDatasetPortablePipelineTranslator::translateImpulse);
    translatorMap.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        SparkDatasetPortablePipelineTranslator::translateGroupByKey);
    translatorMap.put(
        ExecutableStage.URN, SparkDatasetPortablePipelineTranslator::translateExecutableStage);
    translatorMap.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN,
        SparkDatasetPortablePipelineTranslator::translateFlatten);
    translatorMap.put(
        PTransformTranslation.RESHUFFLE_URN,
        SparkDatasetPortablePipelineTranslator::translateReshuffle);
    this.urnToTransformTranslator = translatorMap.build();
  }

  @Override
  public Set<String> knownUrns() {
    return urnToTransformTranslator.keySet();
  }

  @Override
  public void translate(RunnerApi.Pipeline pipeline, SparkDatasetTranslationContext context) {
    if (hasUnboundedPCollections(pipeline)) {
      throw new UnsupportedOperationException(
          "The Dataset-based portable Spark runner runs bounded pipelines only. Unbounded input"
              + " needs a Structured Streaming query, which this backend does not build yet, see"
              + " https://github.com/apache/beam/issues/36841.");
    }
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PTransformNode transformNode : p.getTopologicallyOrderedTransforms()) {
      for (String inputId : transformNode.getTransform().getInputsMap().values()) {
        context.addConsumer(inputId);
      }
    }
    for (PTransformNode transformNode : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(
              transformNode.getTransform().getSpec().getUrn(),
              SparkDatasetPortablePipelineTranslator::urnNotFound)
          .translate(transformNode, pipeline, context);
    }
  }

  @Override
  public SparkDatasetTranslationContext createTranslationContext(
      JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo) {
    return new SparkDatasetTranslationContext(jsc, options, jobInfo);
  }

  private static void urnNotFound(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Transform %s has unknown URN %s",
            transformNode.getId(), transformNode.getTransform().getSpec().getUrn()));
  }

  private static void translateImpulse(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    String outputId = getOutputId(transformNode);
    Dataset<WindowedValue<byte[]>> dataset =
        context
            .getSparkSession()
            .createDataset(
                Collections.singletonList(WindowedValues.valueInGlobalWindow(new byte[0])),
                context.windowedEncoder(outputId, pipeline.getComponents()));
    context.putDataset(outputId, dataset);
  }

  private static <InputT, SideInputT> void translateExecutableStage(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transformNode.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Stage %s uses state or timers, which the Dataset-based portable Spark runner does"
                  + " not support yet, see https://github.com/apache/beam/issues/20396 and"
                  + " https://github.com/apache/beam/issues/20397.",
              transformNode.getId()));
    }
    RunnerApi.Components components = pipeline.getComponents();
    String inputId = stagePayload.getInput();
    Dataset<WindowedValue<InputT>> input = context.getDataset(inputId);
    Map<String, String> outputs = transformNode.getTransform().getOutputsMap();
    BiMap<String, Integer> outputMap = createOutputMap(outputs.values());
    Coder windowCoder = getWindowingStrategy(inputId, components).getWindowFn().windowCoder();

    SparkExecutableStageFunction<InputT, SideInputT> stageFunction =
        new SparkExecutableStageFunction<>(
            context.getSerializableOptions(),
            stagePayload,
            context.jobInfo,
            outputMap,
            SparkExecutableStageContextFactory.getInstance(),
            broadcastSideInputs(stagePayload, context),
            MetricsAccumulator.getInstance(),
            windowCoder,
            getWindowedValueCoder(inputId, components),
            true);

    if (outputs.isEmpty()) {
      // Fusion can leave a stage without runner-visible output. It still has to run, so it
      // becomes a leaf that emits nothing.
      Dataset<WindowedValue<InputT>> sink =
          input.mapPartitions(
              (MapPartitionsFunction<WindowedValue<InputT>, WindowedValue<InputT>>)
                  elements -> {
                    Iterator<RawUnionValue> results = stageFunction.call(elements);
                    while (results.hasNext()) {
                      results.next();
                    }
                    return Collections.emptyIterator();
                  },
              input.encoder());
      context.putDataset(String.format("EmptyOutputSink_%d", context.nextSinkId()), sink);
      return;
    }

    // One encoder per output, in union tag order.
    List<Encoder<WindowedValue<Object>>> encoders =
        new ArrayList<>(Collections.nCopies(outputMap.size(), null));
    for (Map.Entry<String, Integer> output : outputMap.entrySet()) {
      encoders.set(output.getValue(), context.windowedEncoder(output.getKey(), components));
    }
    Dataset<Tuple2<Integer, WindowedValue<Object>>> staged =
        input.mapPartitions(
            (MapPartitionsFunction<WindowedValue<InputT>, Tuple2<Integer, WindowedValue<Object>>>)
                elements -> tagged(stageFunction.call(elements)),
            EncoderHelpers.oneOfEncoder(encoders));
    boolean staging = outputs.size() > 1;
    if (staging) {
      // Every output is a projection of the same stage run. Persist so the stage runs once.
      staged = staged.persist(context.getStorageLevel());
    }
    for (Map.Entry<String, Integer> output : outputMap.entrySet()) {
      int tag = output.getValue();
      TypedColumn<Tuple2<Integer, WindowedValue<Object>>, WindowedValue<Object>> column =
          (TypedColumn) col(Integer.toString(tag)).as(encoders.get(tag));
      // A projection of persisted rows is not cached again.
      context.putDataset(
          output.getKey(), staged.filter(column.isNotNull()).select(column), !staging);
    }
  }

  private static Iterator<Tuple2<Integer, WindowedValue<Object>>> tagged(
      Iterator<RawUnionValue> values) {
    return Iterators.transform(
        values,
        value -> new Tuple2<>(value.getUnionTag(), (WindowedValue<Object>) value.getValue()));
  }

  /** Collects each side input of a stage and broadcasts its encoded elements. */
  private static <SideInputT>
      ImmutableMap<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
          broadcastSideInputs(
              RunnerApi.ExecutableStagePayload stagePayload,
              SparkDatasetTranslationContext context) {
    Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>> broadcasts =
        new HashMap<>();
    RunnerApi.Components components = stagePayload.getComponents();
    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      String collectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      if (broadcasts.containsKey(collectionId)) {
        continue;
      }
      WindowedValueCoder<SideInputT> coder = getWindowedValueCoder(collectionId, components);
      Dataset<WindowedValue<SideInputT>> dataset = context.getDataset(collectionId);
      List<byte[]> bytes =
          new ArrayList<>(
              dataset
                  .map(
                      (MapFunction<WindowedValue<SideInputT>, byte[]>)
                          value -> CoderHelpers.toByteArray(value, coder),
                      Encoders.BINARY())
                  .collectAsList());
      broadcasts.put(collectionId, new Tuple2<>(context.getSparkContext().broadcast(bytes), coder));
    }
    return ImmutableMap.copyOf(broadcasts);
  }

  private static <K, V> void translateGroupByKey(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();
    String inputId = getInputId(transformNode);
    String outputId = getOutputId(transformNode);
    Dataset<WindowedValue<KV<K, V>>> input = context.getDataset(inputId);
    WindowedValueCoder<KV<K, V>> inputCoder = getWindowedValueCoder(inputId, components);
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
    Coder<K> keyCoder = kvCoder.getKeyCoder();
    WindowingStrategy<?, BoundedWindow> windowingStrategy =
        getWindowingStrategy(inputId, components);

    // Batch semantics: all values of a key are present, so every window of the key can close.
    SparkGroupAlsoByWindowViaOutputBufferFn<K, V, BoundedWindow> groupAlsoByWindow =
        new SparkGroupAlsoByWindowViaOutputBufferFn<>(
            windowingStrategy,
            new TranslationUtils.InMemoryStateInternalsFactory<>(),
            SystemReduceFn.buffering(kvCoder.getValueCoder()),
            context.getSerializableOptions());

    Dataset<WindowedValue<KV<K, Iterable<V>>>> grouped =
        input
            .groupByKey(
                (MapFunction<WindowedValue<KV<K, V>>, byte[]>)
                    value -> CoderHelpers.toByteArray(value.getValue().getKey(), keyCoder),
                Encoders.BINARY())
            .flatMapGroups(
                (FlatMapGroupsFunction<
                        byte[], WindowedValue<KV<K, V>>, WindowedValue<KV<K, Iterable<V>>>>)
                    (keyBytes, values) -> {
                      K key = CoderHelpers.fromByteArray(keyBytes, keyCoder);
                      List<WindowedValue<V>> windowedValues = new ArrayList<>();
                      while (values.hasNext()) {
                        WindowedValue<KV<K, V>> value = values.next();
                        windowedValues.add(value.withValue(value.getValue().getValue()));
                      }
                      return groupAlsoByWindow.call(
                          KV.<K, Iterable<WindowedValue<V>>>of(key, windowedValues));
                    },
                context.windowedEncoder(outputId, components));
    context.putDataset(outputId, grouped);
  }

  private static <T> void translateFlatten(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();
    String outputId = getOutputId(transformNode);
    WindowedValueCoder<T> outputCoder = getWindowedValueCoder(outputId, components);
    Encoder<WindowedValue<T>> outputEncoder = context.windowedEncoder(outputId, components);
    Dataset<WindowedValue<T>> result = null;
    for (String inputId : transformNode.getTransform().getInputsMap().values()) {
      Dataset<WindowedValue<T>> input = context.getDataset(inputId);
      if (!getWindowedValueCoder(inputId, components).equals(outputCoder)) {
        // Re-encode so every branch of the union shares the output schema.
        input = input.map((MapFunction<WindowedValue<T>, WindowedValue<T>>) v -> v, outputEncoder);
      }
      result = result == null ? input : result.union(input);
    }
    if (result == null) {
      result = context.getSparkSession().emptyDataset(outputEncoder);
    }
    context.putDataset(outputId, result);
  }

  private static <T> void translateReshuffle(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkDatasetTranslationContext context) {
    Dataset<WindowedValue<T>> input = context.getDataset(getInputId(transformNode));
    context.putDataset(
        getOutputId(transformNode),
        input.repartition(context.getSparkContext().defaultParallelism()));
  }
}
