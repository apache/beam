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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.io.SourceRDD;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
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
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/** Translates a bounded portable pipeline into a Spark job. */
public class SparkBatchPortablePipelineTranslator {

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  interface PTransformTranslator {

    /** Translates transformNode from Beam into the Spark context. */
    void translate(
        PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context);
  }

  public Set<String> knownUrns() {
    // Do not expose Read as a known URN because we only want to support Read
    // through the Java ExpansionService. We can't translate Reads for other
    // languages.
    return Sets.difference(
        urnToTransformTranslator.keySet(),
        ImmutableSet.of(PTransformTranslation.READ_TRANSFORM_URN));
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
        PTransformTranslation.READ_TRANSFORM_URN,
        SparkBatchPortablePipelineTranslator::translateRead);
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
    JavaRDD<WindowedValue<InputT>> inputRdd = ((BoundedDataset<InputT>) inputDataset).getRDD();
    Map<String, String> outputs = transformNode.getTransform().getOutputsMap();
    BiMap<String, Integer> outputMap = createOutputMap(outputs.values());

    ImmutableMap.Builder<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
        broadcastVariablesBuilder = ImmutableMap.builder();
    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      RunnerApi.Components components = stagePayload.getComponents();
      String collectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>> tuple2 =
          broadcastSideInput(collectionId, components, context);
      broadcastVariablesBuilder.put(collectionId, tuple2);
    }

    SparkExecutableStageFunction<InputT, SideInputT> function =
        new SparkExecutableStageFunction<>(
            stagePayload,
            context.jobInfo,
            outputMap,
            broadcastVariablesBuilder.build(),
            MetricsAccumulator.getInstance());
    JavaRDD<RawUnionValue> staged = inputRdd.mapPartitions(function);

    for (String outputId : outputs.values()) {
      JavaRDD<WindowedValue<OutputT>> outputRdd =
          staged.flatMap(new SparkExecutableStageExtractionFunction<>(outputMap.get(outputId)));
      context.pushDataset(outputId, new BoundedDataset<>(outputRdd));
    }
    if (outputs.isEmpty()) {
      // After pipeline translation, we traverse the set of unconsumed PCollections and add a
      // no-op sink to each to make sure they are materialized by Spark. However, some SDK-executed
      // stages have no runner-visible output after fusion. We handle this case by adding a sink
      // here.
      JavaRDD<WindowedValue<OutputT>> outputRdd =
          staged.flatMap((rawUnionValue) -> Collections.emptyIterator());
      context.pushDataset("EmptyOutputSink", new BoundedDataset<>(outputRdd));
    }
  }

  /**
   * Collect and serialize the data and then broadcast the result. *This can be expensive.*
   *
   * @return Spark broadcast variable and coder to decode its contents
   */
  private static <T> Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<T>> broadcastSideInput(
      String collectionId, RunnerApi.Components components, SparkTranslationContext context) {
    PCollection collection = components.getPcollectionsOrThrow(collectionId);
    @SuppressWarnings("unchecked")
    BoundedDataset<T> dataset = (BoundedDataset<T>) context.popDataset(collectionId);
    PCollectionNode collectionNode = PipelineNode.pCollection(collectionId, collection);
    WindowedValueCoder<T> coder;
    try {
      coder =
          (WindowedValueCoder<T>) WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  private static <T> void translateRead(
      PTransformNode transformNode, RunnerApi.Pipeline pipeline, SparkTranslationContext context) {
    String stepName = transformNode.getTransform().getUniqueName();
    final JavaSparkContext jsc = context.getSparkContext();

    BoundedSource boundedSource;
    try {
      boundedSource =
          ReadTranslation.boundedSourceFromProto(
              RunnerApi.ReadPayload.parseFrom(transformNode.getTransform().getSpec().getPayload()));
    } catch (IOException e) {
      throw new RuntimeException("Failed to extract BoundedSource from ReadPayload.", e);
    }

    // create an RDD from a BoundedSource.
    JavaRDD<WindowedValue<T>> input =
        new SourceRDD.Bounded<>(
                jsc.sc(), boundedSource, context.serializablePipelineOptions, stepName)
            .toJavaRDD();

    context.pushDataset(getOutputId(transformNode), new BoundedDataset<>(input));
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
