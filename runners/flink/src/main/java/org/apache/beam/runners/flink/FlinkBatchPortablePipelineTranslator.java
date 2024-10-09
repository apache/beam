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

import static org.apache.beam.runners.flink.translation.utils.FlinkPortableRunnerUtils.requiresTimeSortedInput;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.createOutputMap;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;
import static org.apache.beam.sdk.util.construction.ExecutableStageTranslation.generateNameFromStagePayload;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.Concatenate;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStagePruningFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.ImpulseInputFormat;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Order;
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
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A translator that translates bounded portable pipelines into executable Flink pipelines.
 *
 * <p>Example usage:
 *
 * <pre>
 *   FlinkBatchPortablePipelineTranslator translator =
 *       FlinkBatchPortablePipelineTranslator.createTranslator();
 *   BatchTranslationContext context =
 *       FlinkBatchPortablePipelineTranslator.createTranslationContext(jobInfo, confDir, filesToStage);
 *   translator.translate(context, pipeline);
 *   ExecutionEnvironment executionEnvironment = context.getExecutionEnvironment();
 *   // Do something with executionEnvironment...
 * </pre>
 *
 * <p>After translation the {@link ExecutionEnvironment} in the translation context will contain the
 * full not-yet-executed pipeline DAG corresponding to the input pipeline.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
  "nullness"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class FlinkBatchPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

  /**
   * Creates a batch translation context. The resulting Flink execution dag will live in a new
   * {@link ExecutionEnvironment}.
   */
  @Override
  public BatchTranslationContext createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage) {
    ExecutionEnvironment executionEnvironment =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            pipelineOptions, filesToStage, confDir);
    return createTranslationContext(jobInfo, pipelineOptions, executionEnvironment);
  }

  public static BatchTranslationContext createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      ExecutionEnvironment executionEnvironment) {
    return new BatchTranslationContext(jobInfo, pipelineOptions, executionEnvironment);
  }

  /** Creates a batch translator. */
  public static FlinkBatchPortablePipelineTranslator createTranslator() {
    return createTranslator(ImmutableMap.of());
  }

  /** Creates a batch translator. */
  public static FlinkBatchPortablePipelineTranslator createTranslator(
      Map<String, PTransformTranslator> extraTranslations) {
    ImmutableMap.Builder<String, PTransformTranslator> translatorMap = ImmutableMap.builder();
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
        ExecutableStage.URN, FlinkBatchPortablePipelineTranslator::translateExecutableStage);
    translatorMap.put(
        PTransformTranslation.RESHUFFLE_URN,
        FlinkBatchPortablePipelineTranslator::translateReshuffle);
    translatorMap.putAll(extraTranslations);

    return new FlinkBatchPortablePipelineTranslator(translatorMap.build());
  }

  /**
   * Batch translation context. Stores metadata about known PCollections/DataSets and holds the
   * flink {@link ExecutionEnvironment} that the execution plan will be applied to.
   */
  public static class BatchTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext,
          FlinkPortablePipelineTranslator.Executor {

    private final JobInfo jobInfo;
    private final FlinkPipelineOptions options;
    private final ExecutionEnvironment executionEnvironment;
    private final Map<String, DataSet<?>> dataSets;
    private final Set<String> danglingDataSets;

    private BatchTranslationContext(
        JobInfo jobInfo, FlinkPipelineOptions options, ExecutionEnvironment executionEnvironment) {
      this.jobInfo = jobInfo;
      this.options = options;
      this.executionEnvironment = executionEnvironment;
      dataSets = new HashMap<>();
      danglingDataSets = new HashSet<>();
    }

    @Override
    public JobInfo getJobInfo() {
      return jobInfo;
    }

    @Override
    public FlinkPipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
      return getExecutionEnvironment().execute(jobName);
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
  public interface PTransformTranslator {
    /** Translate a PTransform into the given translation context. */
    void translate(
        PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context);
  }

  private final Map<String, PTransformTranslator> urnToTransformTranslator;

  public FlinkBatchPortablePipelineTranslator(
      Map<String, PTransformTranslator> urnToTransformTranslator) {
    this.urnToTransformTranslator = urnToTransformTranslator;
  }

  @Override
  public Set<String> knownUrns() {
    return urnToTransformTranslator.keySet();
  }

  /** Predicate to determine whether a URN is a Flink native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return PTransformTranslation.RESHUFFLE_URN.equals(
          PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  @Override
  public FlinkPortablePipelineTranslator.Executor translate(
      BatchTranslationContext context, RunnerApi.Pipeline pipeline) {
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
      dataSet.output(new DiscardingOutputFormat<>()).name("DiscardingOutput");
    }

    return context;
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

  private static <InputT> void translateExecutableStage(
      PTransformNode transform, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
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
      PCollectionNode collectionNode =
          PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
      Coder<WindowedValue<?>> coder;
      try {
        coder = (Coder) WireCoders.instantiateRunnerWireCoder(collectionNode, components);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputCoders.put(collectionId, coder);
      unionCoders.add(coder);
    }
    UnionCoder unionCoder = UnionCoder.of(unionCoders);
    TypeInformation<RawUnionValue> typeInformation =
        new CoderTypeInformation<>(unionCoder, context.getPipelineOptions());

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId = stagePayload.getInput();
    Coder<WindowedValue<InputT>> windowedInputCoder =
        instantiateCoder(inputPCollectionId, components);

    DataSet<WindowedValue<InputT>> inputDataSet = context.getDataSetOrThrow(inputPCollectionId);

    final FlinkExecutableStageFunction<InputT> function =
        new FlinkExecutableStageFunction<>(
            transform.getTransform().getUniqueName(),
            context.getPipelineOptions(),
            stagePayload,
            context.getJobInfo(),
            outputMap,
            FlinkExecutableStageContextFactory.getInstance(),
            getWindowingStrategy(inputPCollectionId, components).getWindowFn().windowCoder(),
            windowedInputCoder);

    final String operatorName = generateNameFromStagePayload(stagePayload);

    final SingleInputUdfOperator taggedDataset;
    if (stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0) {

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

      Grouping<WindowedValue<InputT>> groupedInput =
          inputDataSet.groupBy(new KvKeySelector<>(keyCoder));
      boolean requiresTimeSortedInput = requiresTimeSortedInput(stagePayload, false);
      if (requiresTimeSortedInput) {
        groupedInput =
            ((UnsortedGrouping<WindowedValue<InputT>>) groupedInput)
                .sortGroup(WindowedValue::getTimestamp, Order.ASCENDING);
      }

      taggedDataset =
          new GroupReduceOperator<>(groupedInput, typeInformation, function, operatorName);

    } else {
      taggedDataset =
          new MapPartitionOperator<>(inputDataSet, typeInformation, function, operatorName);
    }

    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      String collectionId =
          stagePayload
              .getComponents()
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
          collectionId);
    }
    if (outputs.isEmpty()) {
      // NOTE: After pipeline translation, we traverse the set of unconsumed PCollections and add a
      // no-op sink to each to make sure they are materialized by Flink. However, some SDK-executed
      // stages have no runner-visible output after fusion. We handle this case by adding a sink
      // here.
      taggedDataset.output(new DiscardingOutputFormat<>()).name("DiscardingOutput");
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
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE),
                      context.getPipelineOptions()));
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
    RunnerApi.Components components = pipeline.getComponents();
    String inputPCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    PCollectionNode inputCollection =
        PipelineNode.pCollection(
            inputPCollectionId, components.getPcollectionsOrThrow(inputPCollectionId));
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

    WindowedValueCoder<KV<K, V>> inputCoder;
    try {
      inputCoder =
          (WindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(inputCollection, pipeline.getComponents());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    KvCoder<K, V> inputElementCoder = (KvCoder<K, V>) inputCoder.getValueCoder();

    Concatenate<V> combineFn = new Concatenate<>();
    Coder<List<V>> accumulatorCoder =
        combineFn.getAccumulatorCoder(
            CoderRegistry.createDefault(null), inputElementCoder.getValueCoder());

    Coder<WindowedValue<KV<K, List<V>>>> outputCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputElementCoder.getKeyCoder(), accumulatorCoder),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, List<V>>>> partialReduceTypeInfo =
        new CoderTypeInformation<>(outputCoder, context.getPipelineOptions());

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
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE),
            context.getPipelineOptions());
    DataSource<WindowedValue<byte[]>> dataSource =
        new DataSource<>(
                context.getExecutionEnvironment(),
                new ImpulseInputFormat(),
                typeInformation,
                transform.getTransform().getUniqueName())
            .name("Impulse");

    context.addDataSet(
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values()), dataSource);
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
      String collectionId) {
    TypeInformation<WindowedValue<?>> outputType =
        new CoderTypeInformation<>(outputCoder, context.getPipelineOptions());
    FlinkExecutableStagePruningFunction pruningFunction =
        new FlinkExecutableStagePruningFunction(unionTag, context.getPipelineOptions());
    FlatMapOperator<RawUnionValue, WindowedValue<?>> pruningOperator =
        new FlatMapOperator<>(
            taggedDataset,
            outputType,
            pruningFunction,
            String.format("ExtractOutput[%s]", unionTag));
    context.addDataSet(collectionId, pruningOperator);
  }
}
