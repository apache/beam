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
package org.apache.beam.runners.flink;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStagePruningFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.ImpulseInputFormat;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.MapPartitionOperator;

/**
 * A translator that translates bounded portable pipelines into executable Flink pipelines.
 *
 * <p>Example usage:
 *
 * <pre>
 *   FlinkBatchPortablePipelineTranslator translator =
 *       FlinkBatchPortablePipelineTranslator.createTranslator();
 *   BatchTranslationContext context =
 *       FlinkBatchPortablePipelineTranslator.createTranslationContext(options);
 *   translator.translate(context, pipeline);
 *   ExecutionEnvironment executionEnvironment = context.getExecutionEnvironment();
 *   // Do something with executionEnvironment...
 * </pre>
 *
 * <p>After translation the {@link ExecutionEnvironment} in the translation context will contain the
 * full not-yet-executed pipeline DAG corresponding to the input pipeline.
 */
public class FlinkBatchPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

  /**
   * Creates a batch translation context. The resulting Flink execution dag will live in a new
   * {@link ExecutionEnvironment}.
   */
  public static BatchTranslationContext createTranslationContext(FlinkPipelineOptions options) {
    ExecutionEnvironment executionEnvironment =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(options);
    return new BatchTranslationContext(options, executionEnvironment);
  }

  /** Creates a batch translator. */
  public static FlinkBatchPortablePipelineTranslator createTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator> translatorMap =
        ImmutableMap.builder();
    translatorMap.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN,
        FlinkBatchPortablePipelineTranslator::translateFlatten);
    translatorMap.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        FlinkBatchPortablePipelineTranslator::translateGroupByKey);
    translatorMap.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        FlinkBatchPortablePipelineTranslator::translateImpulse);
    translatorMap.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
        FlinkBatchPortablePipelineTranslator::translateAssignWindows);
    translatorMap.put(
        ExecutableStage.URN, FlinkBatchPortablePipelineTranslator::translateExecutableStage);
    translatorMap.put(
        PTransformTranslation.RESHUFFLE_URN,
        FlinkBatchPortablePipelineTranslator::translateReshuffle);
    translatorMap.put(
        PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
        FlinkBatchPortablePipelineTranslator::translateView);
    return new FlinkBatchPortablePipelineTranslator(translatorMap.build());
  }

  /**
   * Batch translation context. Stores metadata about known PCollections/DataSets and holds the
   * flink {@link ExecutionEnvironment} that the execution plan will be applied to.
   */
  public static class BatchTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext {

    private final FlinkPipelineOptions options;
    private final ExecutionEnvironment executionEnvironment;
    private final Map<String, DataSet<?>> dataSets;
    private final Set<String> danglingDataSets;

    private BatchTranslationContext(
        FlinkPipelineOptions options, ExecutionEnvironment executionEnvironment) {
      this.options = options;
      this.executionEnvironment = executionEnvironment;
      dataSets = new HashMap<>();
      danglingDataSets = new HashSet<>();
    }

    @Override
    public FlinkPipelineOptions getPipelineOptions() {
      return options;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    public <T> void addDataSet(String pCollectionId, DataSet<T> dataSet) {
      checkArgument(!dataSets.containsKey(pCollectionId));
      dataSets.put(pCollectionId, dataSet);
      danglingDataSets.add(pCollectionId);
    }

    public <T> DataSet<T> getDataSetOrThrow(String pCollectionId) {
      DataSet<T> dataSet = (DataSet<T>) dataSets.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
            String.format("Unknown dataset for id %s.", pCollectionId));
      }

      // Assume that the DataSet is consumed if requested. We use this as a proxy for consumption
      // because Flink does not expose its internal execution plan.
      danglingDataSets.remove(pCollectionId);
      return dataSet;
    }

    public Collection<DataSet<?>> getDanglingDataSets() {
      return danglingDataSets.stream().map(id -> dataSets.get(id)).collect(Collectors.toList());
    }
  }

  /** Transform translation interface. */
  @FunctionalInterface
  private interface PTransformTranslator {
    /** Translate a PTransform into the given translation context. */
    void translate(
        PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context);
  }

  private final Map<String, PTransformTranslator> urnToTransformTranslator;

  private FlinkBatchPortablePipelineTranslator(
      Map<String, PTransformTranslator> urnToTransformTranslator) {
    this.urnToTransformTranslator = urnToTransformTranslator;
  }

  @Override
  public void translate(BatchTranslationContext context, RunnerApi.Pipeline pipeline) {
    // Use a QueryablePipeline to traverse transforms topologically.
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(
              transform.getTransform().getSpec().getUrn(),
              FlinkBatchPortablePipelineTranslator::urnNotFound)
          .translate(transform, pipeline, context);
    }

    // Ensure that side effects are performed for unconsumed DataSets.
    for (DataSet<?> dataSet : context.getDanglingDataSets()) {
      dataSet.output(new DiscardingOutputFormat<>());
    }
  }

  private static <InputT> void translateView(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {

    DataSet<WindowedValue<InputT>> inputDataSet =
        context.getDataSetOrThrow(
            Iterables.getOnlyElement(transform.getTransform().getInputsMap().values()));

    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), inputDataSet);
  }

  private static <K, V> void translateReshuffle(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    DataSet<WindowedValue<KV<K, V>>> inputDataSet =
        context.getDataSetOrThrow(
            Iterables.getOnlyElement(transform.getTransform().getInputsMap().values()));
    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()),
        inputDataSet.rebalance());
  }

  private static <T> void translateAssignWindows(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.WindowIntoPayload payload;
    try {
      payload =
          RunnerApi.WindowIntoPayload.parseFrom(transform.getTransform().getSpec().getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
    WindowFn<T, ? extends BoundedWindow> windowFn =
        (WindowFn<T, ? extends BoundedWindow>)
            WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn());

    String inputCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    String outputCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());
    Coder<WindowedValue<T>> outputCoder =
        instantiateRunnerWireCoder(outputCollectionId, components);
    TypeInformation<WindowedValue<T>> resultTypeInfo = new CoderTypeInformation<>(outputCoder);

    DataSet<WindowedValue<T>> inputDataSet = context.getDataSetOrThrow(inputCollectionId);

    FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
        new FlinkAssignWindows<>(windowFn);

    DataSet<WindowedValue<T>> resultDataSet =
        inputDataSet
            .flatMap(assignWindowsFunction)
            .name(transform.getTransform().getUniqueName())
            .returns(resultTypeInfo);

    context.addDataSet(outputCollectionId, resultDataSet);
  }

  private static <InputT> void translateExecutableStage(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    // TODO: Fail on stateful DoFns for now.
    // TODO: Support stateful DoFns by inserting group-by-keys where necessary.
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.

    RunnerApi.Components components = pipeline.getComponents();
    Map<String, String> outputs = transform.getTransform().getOutputsMap();
    // Mapping from PCollection id to coder tag id.
    BiMap<String, Integer> outputMap = createOutputMap(outputs.values());
    // Collect all output Coders and create a UnionCoder for our tagged outputs.
    List<Coder<?>> unionCoders = Lists.newArrayList();
    // Enforce tuple tag sorting by union tag index.
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    for (String collectionId : new TreeMap<>(outputMap.inverse()).values()) {
      Coder<WindowedValue<?>> windowCoder =
          (Coder) instantiateRunnerWireCoder(collectionId, components);
      outputCoders.put(collectionId, windowCoder);
      unionCoders.add(windowCoder);
    }
    UnionCoder unionCoder = UnionCoder.of(unionCoders);
    TypeInformation<RawUnionValue> typeInformation = new CoderTypeInformation<>(unionCoder);

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    FlinkExecutableStageFunction<InputT> function =
        new FlinkExecutableStageFunction<>(
            stagePayload,
            PipelineOptionsTranslation.toProto(context.getPipelineOptions()),
            outputMap);

    DataSet<WindowedValue<InputT>> inputDataSet =
        context.getDataSetOrThrow(stagePayload.getInput());

    MapPartitionOperator<WindowedValue<InputT>, RawUnionValue> taggedDataset =
        new MapPartitionOperator<>(
            inputDataSet, typeInformation, function, transform.getTransform().getUniqueName());

    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      String collectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      // Register under the global PCollection name. Only ExecutableStageFunction needs to know the
      // mapping from local name to global name and how to translate the broadcast data to a state
      // API view.
      taggedDataset.withBroadcastSet(context.getDataSetOrThrow(collectionId), collectionId);
    }

    for (String collectionId : outputs.values()) {
      pruneOutput(
          taggedDataset,
          context,
          outputMap.get(collectionId),
          outputCoders.get(collectionId),
          transform.getTransform().getUniqueName(),
          collectionId);
    }
    if (outputs.isEmpty()) {
      // NOTE: After pipeline translation, we traverse the set of unconsumed PCollections and add a
      // no-op sink to each to make sure they are materialized by Flink. However, some SDK-executed
      // stages have no runner-visible output after fusion. We handle this case by adding a sink
      // here.
      taggedDataset.output(new DiscardingOutputFormat());
    }
  }

  private static <T> void translateFlatten(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    Map<String, String> allInputs = transform.getTransform().getInputsMap();
    DataSet<WindowedValue<T>> result = null;

    if (allInputs.isEmpty()) {

      // Create an empty dummy source to satisfy downstream operations. We cannot create an empty
      // source in Flink, so we send the DataSet to a flatMap that never forwards its element.
      DataSource<String> dummySource = context.getExecutionEnvironment().fromElements("dummy");
      result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE)));
    } else {
      for (String pCollectionId : allInputs.values()) {
        DataSet<WindowedValue<T>> current = context.getDataSetOrThrow(pCollectionId);
        if (result == null) {
          result = current;
        } else {
          result = result.union(current);
        }
      }
    }

    // Insert a dummy filter. Flink produces duplicate elements after the union in some cases if we
    // don't do so.
    result = result.filter(tWindowedValue -> true).name("UnionFixFilter");
    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), result);
  }

  private static <K, V> void translateGroupByKey(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    String inputPCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    DataSet<WindowedValue<KV<K, V>>> inputDataSet = context.getDataSetOrThrow(inputPCollectionId);
    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline
            .getComponents()
            .getWindowingStrategiesOrThrow(
                pipeline
                    .getComponents()
                    .getPcollectionsOrThrow(inputPCollectionId)
                    .getWindowingStrategyId());

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());

    WindowingStrategy<Object, BoundedWindow> windowingStrategy;
    try {
      windowingStrategy =
          (WindowingStrategy<Object, BoundedWindow>)
              WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto),
          e);
    }

    WindowedValueCoder<KV<K, V>> inputCoder =
        instantiateRunnerWireCoder(inputPCollectionId, pipeline.getComponents());

    KvCoder<K, V> inputElementCoder = (KvCoder<K, V>) inputCoder.getValueCoder();

    Concatenate<V> combineFn = new Concatenate<>();
    Coder<List<V>> accumulatorCoder =
        combineFn.getAccumulatorCoder(
            CoderRegistry.createDefault(), inputElementCoder.getValueCoder());

    Coder<WindowedValue<KV<K, List<V>>>> outputCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputElementCoder.getKeyCoder(), accumulatorCoder),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, List<V>>>> partialReduceTypeInfo =
        new CoderTypeInformation<>(outputCoder);

    Grouping<WindowedValue<KV<K, V>>> inputGrouping =
        inputDataSet.groupBy(new KvKeySelector<>(inputElementCoder.getKeyCoder()));

    FlinkPartialReduceFunction<K, V, List<V>, ?> partialReduceFunction =
        new FlinkPartialReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    FlinkReduceFunction<K, List<V>, List<V>, ?> reduceFunction =
        new FlinkReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    // Partially GroupReduce the values into the intermediate format AccumT (combine)
    GroupCombineOperator<WindowedValue<KV<K, V>>, WindowedValue<KV<K, List<V>>>> groupCombine =
        new GroupCombineOperator<>(
            inputGrouping,
            partialReduceTypeInfo,
            partialReduceFunction,
            "GroupCombine: " + transform.getTransform().getUniqueName());

    Grouping<WindowedValue<KV<K, List<V>>>> intermediateGrouping =
        groupCombine.groupBy(new KvKeySelector<>(inputElementCoder.getKeyCoder()));

    // Fully reduce the values and create output format VO
    GroupReduceOperator<WindowedValue<KV<K, List<V>>>, WindowedValue<KV<K, List<V>>>>
        outputDataSet =
            new GroupReduceOperator<>(
                intermediateGrouping,
                partialReduceTypeInfo,
                reduceFunction,
                transform.getTransform().getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), outputDataSet);
  }

  private static void translateImpulse(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    TypeInformation<WindowedValue<byte[]>> typeInformation =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    DataSource<WindowedValue<byte[]>> dataSource =
        new DataSource<>(
            context.getExecutionEnvironment(),
            new ImpulseInputFormat(),
            typeInformation,
            transform.getTransform().getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), dataSource);
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use to translate {@link GroupByKey}. For a large {@link PCollection} this is
   * expected to crash!
   *
   * <p>This is copied from the dataflow runner code.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  private static void urnNotFound(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN %s for PTransform with id %s.",
            transform.getTransform().getSpec().getUrn(), transform.getId()));
  }

  private static void pruneOutput(
      DataSet<RawUnionValue> taggedDataset,
      BatchTranslationContext context,
      int unionTag,
      Coder<WindowedValue<?>> outputCoder,
      String transformName,
      String collectionId) {
    TypeInformation<WindowedValue<?>> outputType = new CoderTypeInformation<>(outputCoder);
    FlinkExecutableStagePruningFunction pruningFunction =
        new FlinkExecutableStagePruningFunction(unionTag);
    FlatMapOperator<RawUnionValue, WindowedValue<?>> pruningOperator =
        new FlatMapOperator<>(
            taggedDataset,
            outputType,
            pruningFunction,
            String.format("%s/out.%d", transformName, unionTag));
    context.addDataSet(collectionId, pruningOperator);
  }

  /**  Creates a mapping from PCollection id to output tag integer. */
  private static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    for (String tag : localOutputs) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

  /** Instantiates a Java coder for windowed values of the given PCollection id. */
  private static <T> WindowedValueCoder<T> instantiateRunnerWireCoder(
      String collectionId, RunnerApi.Components components) {
    RunnerApi.PCollection collection = components.getPcollectionsOrThrow(collectionId);
    PCollectionNode collectionNode = PipelineNode.pCollection(collectionId, collection);

    // Instantiate the wire coder by length-prefixing unknown coders.
    RunnerApi.MessageWithComponents protoCoder =
        WireCoders.createRunnerWireCoder(collectionNode, components, components::containsCoders);
    Coder<?> javaCoder;
    try {
      javaCoder =
          CoderTranslation.fromProto(
              protoCoder.getCoder(),
              RehydratedComponents.forComponents(protoCoder.getComponents()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    checkArgument(javaCoder instanceof WindowedValueCoder);
    return (WindowedValueCoder<T>) javaCoder;
  }
}
