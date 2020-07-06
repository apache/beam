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
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/** Translates a bounded portable pipeline into a Spark job. */
public class SparkBatchPortablePipelineTranslator
    implements SparkPortablePipelineTranslator<SparkTranslationContext> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkBatchPortablePipelineTranslator.class);

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  interface PTransformTranslator {

    /** Translates transformNode from Beam into the Spark context. */
    void translate(
        PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context);
  }

  @Override
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
    translatorMap.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN,
        SparkBatchPortablePipelineTranslator::translateFlatten);
    translatorMap.put(
        PTransformTranslation.RESHUFFLE_URN,
        SparkBatchPortablePipelineTranslator::translateReshuffle);
    this.urnToTransformTranslator = translatorMap.build();
  }

  /** Translates pipeline from Beam into the Spark context. */
  @Override
  public void translate(final RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transformNode : p.getTopologicallyOrderedTransforms()) {
      // Pre-scan pipeline to count which pCollections are consumed as inputs more than once so
      // their corresponding RDDs can later be cached.
      for (String inputId : transformNode.getTransform().getInputsMap().values()) {
        context.incrementConsumptionCountBy(inputId, 1);
      }
      // Executable stage consists of two parts: computation and extraction. This means the result
      // of computation is an intermediate RDD, which we might also need to cache.
      if (transformNode.getTransform().getSpec().getUrn().equals(ExecutableStage.URN)) {
        context.incrementConsumptionCountBy(
            getExecutableStageIntermediateId(transformNode),
            transformNode.getTransform().getOutputsMap().size());
      }
      for (String outputId : transformNode.getTransform().getOutputsMap().values()) {
        WindowedValueCoder outputCoder = getWindowedValueCoder(outputId, pipeline.getComponents());
        context.putCoder(outputId, outputCoder);
      }
    }
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
    Dataset inputDataset = context.popDataset(inputId);
    JavaRDD<WindowedValue<KV<K, V>>> inputRdd = ((BoundedDataset<KV<K, V>>) inputDataset).getRDD();
    WindowedValueCoder<KV<K, V>> inputCoder = getWindowedValueCoder(inputId, components);
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
    Coder<K> inputKeyCoder = inputKvCoder.getKeyCoder();
    Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
    WindowingStrategy windowingStrategy = getWindowingStrategy(inputId, components);
    WindowFn<Object, BoundedWindow> windowFn = windowingStrategy.getWindowFn();
    WindowedValue.WindowedValueCoder<V> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(inputValueCoder, windowFn.windowCoder());

    JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupedByKeyAndWindow;
    Partitioner partitioner = getPartitioner(context);
    if (GroupNonMergingWindowsFunctions.isEligibleForGroupByWindow(windowingStrategy)) {
      // we can have a memory sensitive translation for non-merging windows
      groupedByKeyAndWindow =
          GroupNonMergingWindowsFunctions.groupByKeyAndWindow(
              inputRdd, inputKeyCoder, inputValueCoder, windowingStrategy, partitioner);
    } else {
      JavaRDD<KV<K, Iterable<WindowedValue<V>>>> groupedByKeyOnly =
          GroupCombineFunctions.groupByKeyOnly(inputRdd, inputKeyCoder, wvCoder, partitioner);
      // for batch, GroupAlsoByWindow uses an in-memory StateInternals.
      groupedByKeyAndWindow =
          groupedByKeyOnly.flatMap(
              new SparkGroupAlsoByWindowViaOutputBufferFn<>(
                  windowingStrategy,
                  new TranslationUtils.InMemoryStateInternalsFactory<>(),
                  SystemReduceFn.buffering(inputValueCoder),
                  context.serializablePipelineOptions));
    }
    context.pushDataset(getOutputId(transformNode), new BoundedDataset<>(groupedByKeyAndWindow));
  }

  private static <InputT, OutputT, SideInputT> void translateExecutableStage(
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
    Map<String, String> outputs = transformNode.getTransform().getOutputsMap();
    BiMap<String, Integer> outputExtractionMap = createOutputMap(outputs.values());
    Components components = pipeline.getComponents();
    Coder windowCoder =
        getWindowingStrategy(inputPCollectionId, components).getWindowFn().windowCoder();

    ImmutableMap<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
        broadcastVariables = broadcastSideInputs(stagePayload, context);

    JavaRDD<RawUnionValue> staged;
    if (stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0) {
      Coder<WindowedValue<InputT>> windowedInputCoder =
          instantiateCoder(inputPCollectionId, components);
      Coder valueCoder =
          ((WindowedValue.FullWindowedValueCoder) windowedInputCoder).getValueCoder();
      // Stateful stages are only allowed of KV input to be able to group on the key
      if (!(valueCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            String.format(
                Locale.ENGLISH,
                "The element coder for stateful DoFn '%s' must be KvCoder but is: %s",
                inputPCollectionId,
                valueCoder.getClass().getSimpleName()));
      }
      Coder keyCoder = ((KvCoder) valueCoder).getKeyCoder();
      Coder innerValueCoder = ((KvCoder) valueCoder).getValueCoder();
      WindowingStrategy windowingStrategy = getWindowingStrategy(inputPCollectionId, components);
      WindowFn<Object, BoundedWindow> windowFn = windowingStrategy.getWindowFn();
      WindowedValue.WindowedValueCoder wvCoder =
          WindowedValue.FullWindowedValueCoder.of(innerValueCoder, windowFn.windowCoder());

      JavaPairRDD<ByteArray, Iterable<WindowedValue<KV>>> groupedByKey =
          groupByKeyPair(inputDataset, keyCoder, wvCoder);
      SparkExecutableStageFunction<KV, SideInputT> function =
          new SparkExecutableStageFunction<>(
              stagePayload,
              context.jobInfo,
              outputExtractionMap,
              SparkExecutableStageContextFactory.getInstance(),
              broadcastVariables,
              MetricsAccumulator.getInstance(),
              windowCoder);
      staged = groupedByKey.flatMap(function.forPair());
    } else {
      JavaRDD<WindowedValue<InputT>> inputRdd2 = ((BoundedDataset<InputT>) inputDataset).getRDD();
      SparkExecutableStageFunction<InputT, SideInputT> function2 =
          new SparkExecutableStageFunction<>(
              stagePayload,
              context.jobInfo,
              outputExtractionMap,
              SparkExecutableStageContextFactory.getInstance(),
              broadcastVariables,
              MetricsAccumulator.getInstance(),
              windowCoder);
      staged = inputRdd2.mapPartitions(function2);
    }

    String intermediateId = getExecutableStageIntermediateId(transformNode);
    context.pushDataset(
        intermediateId,
        new Dataset() {
          @Override
          public void cache(String storageLevel, Coder<?> coder) {
            StorageLevel level = StorageLevel.fromString(storageLevel);
            staged.persist(level);
          }

          @Override
          public void action() {
            // Empty function to force computation of RDD.
            staged.foreach(TranslationUtils.emptyVoidFunction());
          }

          @Override
          public void setName(String name) {
            staged.setName(name);
          }
        });
    // pop dataset to mark RDD as used
    context.popDataset(intermediateId);

    for (String outputId : outputs.values()) {
      JavaRDD<WindowedValue<OutputT>> outputRdd =
          staged.flatMap(
              new SparkExecutableStageExtractionFunction<>(outputExtractionMap.get(outputId)));
      context.pushDataset(outputId, new BoundedDataset<>(outputRdd));
    }
    if (outputs.isEmpty()) {
      // After pipeline translation, we traverse the set of unconsumed PCollections and add a
      // no-op sink to each to make sure they are materialized by Spark. However, some SDK-executed
      // stages have no runner-visible output after fusion. We handle this case by adding a sink
      // here.
      JavaRDD<WindowedValue<OutputT>> outputRdd =
          staged.flatMap((rawUnionValue) -> Collections.emptyIterator());
      context.pushDataset(
          String.format("EmptyOutputSink_%d", context.nextSinkId()),
          new BoundedDataset<>(outputRdd));
    }
  }

  /** Wrapper to help with type inference for {@link GroupCombineFunctions#groupByKeyPair}. */
  private static <K, V> JavaPairRDD<ByteArray, Iterable<WindowedValue<KV<K, V>>>> groupByKeyPair(
      Dataset dataset, Coder<K> keyCoder, WindowedValueCoder<V> wvCoder) {
    JavaRDD<WindowedValue<KV<K, V>>> inputRdd = ((BoundedDataset<KV<K, V>>) dataset).getRDD();
    return GroupCombineFunctions.groupByKeyPair(inputRdd, keyCoder, wvCoder);
  }

  /**
   * Broadcast the side inputs of an executable stage. *This can be expensive.*
   *
   * @return Map from PCollection ID to Spark broadcast variable and coder to decode its contents.
   */
  private static <SideInputT>
      ImmutableMap<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
          broadcastSideInputs(
              RunnerApi.ExecutableStagePayload stagePayload, SparkTranslationContext context) {
    Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
        broadcastVariables = new HashMap<>();
    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      RunnerApi.Components stagePayloadComponents = stagePayload.getComponents();
      String collectionId =
          stagePayloadComponents
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      if (broadcastVariables.containsKey(collectionId)) {
        // This PCollection has already been broadcast.
        continue;
      }
      Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>> tuple2 =
          broadcastSideInput(collectionId, stagePayloadComponents, context);
      broadcastVariables.put(collectionId, tuple2);
    }
    return ImmutableMap.copyOf(broadcastVariables);
  }

  /**
   * Collect and serialize the data and then broadcast the result. *This can be expensive.*
   *
   * @return Spark broadcast variable and coder to decode its contents
   */
  private static <T> Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<T>> broadcastSideInput(
      String collectionId, RunnerApi.Components components, SparkTranslationContext context) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> dataset = (BoundedDataset<T>) context.popDataset(collectionId);
    WindowedValueCoder<T> coder = getWindowedValueCoder(collectionId, components);
    List<byte[]> bytes = dataset.getBytes(coder);
    Broadcast<List<byte[]>> broadcast = context.getSparkContext().broadcast(bytes);
    return new Tuple2<>(broadcast, coder);
  }

  private static <T> void translateFlatten(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {

    Map<String, String> inputsMap = transformNode.getTransform().getInputsMap();

    JavaRDD<WindowedValue<T>> unionRDD;
    if (inputsMap.isEmpty()) {
      unionRDD = context.getSparkContext().emptyRDD();
    } else {
      JavaRDD<WindowedValue<T>>[] rdds = new JavaRDD[inputsMap.size()];
      int index = 0;
      for (String inputId : inputsMap.values()) {
        rdds[index] = ((BoundedDataset<T>) context.popDataset(inputId)).getRDD();
        index++;
      }
      unionRDD = context.getSparkContext().union(rdds);
    }
    context.pushDataset(getOutputId(transformNode), new BoundedDataset<>(unionRDD));
  }

  private static <T> void translateReshuffle(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    String inputId = getInputId(transformNode);
    WindowedValueCoder<T> coder = getWindowedValueCoder(inputId, pipeline.getComponents());
    JavaRDD<WindowedValue<T>> inRDD = ((BoundedDataset<T>) context.popDataset(inputId)).getRDD();
    JavaRDD<WindowedValue<T>> reshuffled = GroupCombineFunctions.reshuffle(inRDD, coder);
    context.pushDataset(getOutputId(transformNode), new BoundedDataset<>(reshuffled));
  }

  private static @Nullable Partitioner getPartitioner(SparkTranslationContext context) {
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

  private static <T> WindowedValueCoder<T> getWindowedValueCoder(
      String pCollectionId, RunnerApi.Components components) {
    PCollection pCollection = components.getPcollectionsOrThrow(pCollectionId);
    PCollectionNode pCollectionNode = PipelineNode.pCollection(pCollectionId, pCollection);
    WindowedValueCoder<T> coder;
    try {
      coder =
          (WindowedValueCoder) WireCoders.instantiateRunnerWireCoder(pCollectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return coder;
  }

  private static String getExecutableStageIntermediateId(PTransformNode transformNode) {
    return transformNode.getId();
  }

  @Override
  public SparkTranslationContext createTranslationContext(
      JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo) {
    return new SparkTranslationContext(jsc, options, jobInfo);
  }

  /** Predicate to determine whether a URN is a Spark native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsSparkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return PTransformTranslation.RESHUFFLE_URN.equals(
          PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }
}
