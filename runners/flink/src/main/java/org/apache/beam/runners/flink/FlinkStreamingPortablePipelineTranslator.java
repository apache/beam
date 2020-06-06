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

import static java.lang.String.format;
import static org.apache.beam.runners.core.construction.ExecutableStageTranslation.generateNameFromStagePayload;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.createOutputMap;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.RunnerPCollectionView;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TestStreamTranslation;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.functions.ImpulseSourceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.DedupingOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.StreamingImpulseSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestStreamSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Translate an unbounded portable pipeline representation into a Flink pipeline representation. */
public class FlinkStreamingPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext> {

  /**
   * Creates a streaming translation context. The resulting Flink execution dag will live in a new
   * {@link StreamExecutionEnvironment}.
   */
  @Override
  public StreamingTranslationContext createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      String confDir,
      List<String> filesToStage) {
    StreamExecutionEnvironment executionEnvironment =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            pipelineOptions, filesToStage, confDir);
    return new StreamingTranslationContext(jobInfo, pipelineOptions, executionEnvironment);
  }

  /**
   * Streaming translation context. Stores metadata about known PCollections/DataStreams and holds
   * the Flink {@link StreamExecutionEnvironment} that the execution plan will be applied to.
   */
  public static class StreamingTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext,
          FlinkPortablePipelineTranslator.Executor {

    private final JobInfo jobInfo;
    private final FlinkPipelineOptions options;
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;

    private StreamingTranslationContext(
        JobInfo jobInfo,
        FlinkPipelineOptions options,
        StreamExecutionEnvironment executionEnvironment) {
      this.jobInfo = jobInfo;
      this.options = options;
      this.executionEnvironment = executionEnvironment;
      dataStreams = new HashMap<>();
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

    public StreamExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    public <T> void addDataStream(String pCollectionId, DataStream<T> dataStream) {
      dataStreams.put(pCollectionId, dataStream);
    }

    public <T> DataStream<T> getDataStreamOrThrow(String pCollectionId) {
      DataStream<T> dataSet = (DataStream<T>) dataStreams.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
            String.format("Unknown datastream for id %s.", pCollectionId));
      }
      return dataSet;
    }
  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  /** @deprecated Legacy non-portable source which can be replaced by a DoFn with timers. */
  @Deprecated
  private static final String STREAMING_IMPULSE_TRANSFORM_URN =
      "flink:transform:streaming_impulse:v1";

  private final Map<String, PTransformTranslator<StreamingTranslationContext>>
      urnToTransformTranslator;

  FlinkStreamingPortablePipelineTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>> translatorMap =
        ImmutableMap.builder();
    translatorMap.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, this::translateFlatten);
    translatorMap.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, this::translateGroupByKey);
    translatorMap.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, this::translateImpulse);
    translatorMap.put(ExecutableStage.URN, this::translateExecutableStage);
    translatorMap.put(PTransformTranslation.RESHUFFLE_URN, this::translateReshuffle);

    // TODO Legacy transforms which need to be removed
    // Consider removing now that timers are supported
    translatorMap.put(STREAMING_IMPULSE_TRANSFORM_URN, this::translateStreamingImpulse);
    // Remove once unbounded Reads can be wrapped in SDFs
    translatorMap.put(PTransformTranslation.READ_TRANSFORM_URN, this::translateUnboundedRead);

    // For testing only
    translatorMap.put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, this::translateTestStream);

    this.urnToTransformTranslator = translatorMap.build();
  }

  @Override
  public Set<String> knownUrns() {
    // Do not expose Read as a known URN because TrivialNativeTransformExpander otherwise removes
    // the subtransforms which are added in case of bounded reads. We only have a
    // translator here for unbounded Reads which are native transforms which do not
    // have subtransforms. Unbounded Reads are used by cross-language transforms, e.g.
    // KafkaIO.
    return Sets.difference(
        urnToTransformTranslator.keySet(),
        ImmutableSet.of(PTransformTranslation.READ_TRANSFORM_URN));
  }

  @Override
  public FlinkPortablePipelineTranslator.Executor translate(
      StreamingTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }

    return context;
  }

  private void urnNotFound(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.TranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN %s for PTransform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(), id));
  }

  private <K, V> void translateReshuffle(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(Iterables.getOnlyElement(transform.getInputsMap().values()));
    context.addDataStream(
        Iterables.getOnlyElement(transform.getOutputsMap().values()), inputDataStream.rebalance());
  }

  private <T> void translateFlatten(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    Map<String, String> allInputs = transform.getInputsMap();

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      long shutdownAfterIdleSourcesMs =
          context.getPipelineOptions().getShutdownSourcesAfterIdleMs();
      DataStreamSource<WindowedValue<byte[]>> dummySource =
          context
              .getExecutionEnvironment()
              .addSource(new ImpulseSourceFunction(shutdownAfterIdleSourcesMs));

      DataStream<WindowedValue<T>> result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE)));
      context.addDataStream(Iterables.getOnlyElement(transform.getOutputsMap().values()), result);
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

      context.addDataStream(Iterables.getOnlyElement(transform.getOutputsMap().values()), result);
    }
  }

  private <K, V> void translateGroupByKey(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {

    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());

    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline
            .getComponents()
            .getWindowingStrategiesOrThrow(
                pipeline
                    .getComponents()
                    .getPcollectionsOrThrow(inputPCollectionId)
                    .getWindowingStrategyId());

    WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy =
          WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto),
          e);
    }

    WindowedValueCoder<KV<K, V>> windowedInputCoder =
        (WindowedValueCoder) instantiateCoder(inputPCollectionId, pipeline.getComponents());

    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> outputDataStream =
        addGBK(
            inputDataStream,
            windowingStrategy,
            windowedInputCoder,
            pTransform.getUniqueName(),
            context);
    // Assign a unique but consistent id to re-map operator state
    outputDataStream.uid(pTransform.getUniqueName());

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()), outputDataStream);
  }

  private <K, V> SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> addGBK(
      DataStream<WindowedValue<KV<K, V>>> inputDataStream,
      WindowingStrategy<?, ?> windowingStrategy,
      WindowedValueCoder<KV<K, V>> windowedInputCoder,
      String operatorName,
      StreamingTranslationContext context) {
    KvCoder<K, V> inputElementCoder = (KvCoder<K, V>) windowedInputCoder.getValueCoder();

    SingletonKeyedWorkItemCoder<K, V> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            inputElementCoder.getKeyCoder(),
            inputElementCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<SingletonKeyedWorkItem<K, V>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemTypeInfo =
        new CoderTypeInformation<>(windowedWorkItemCoder);

    DataStream<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemStream =
        inputDataStream
            .flatMap(
                new FlinkStreamingTransformTranslators.ToKeyedWorkItem<>(
                    context.getPipelineOptions()))
            .returns(workItemTypeInfo)
            .name("ToKeyedWorkItem");

    WorkItemKeySelector<K, V> keySelector =
        new WorkItemKeySelector<>(inputElementCoder.getKeyCoder());

    KeyedStream<WindowedValue<SingletonKeyedWorkItem<K, V>>, ByteBuffer> keyedWorkItemStream =
        workItemStream.keyBy(keySelector);

    SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn =
        SystemReduceFn.buffering(inputElementCoder.getValueCoder());

    Coder<Iterable<V>> accumulatorCoder = IterableCoder.of(inputElementCoder.getValueCoder());

    Coder<WindowedValue<KV<K, Iterable<V>>>> outputCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputElementCoder.getKeyCoder(), accumulatorCoder),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, Iterable<V>>>> outputTypeInfo =
        new CoderTypeInformation<>(outputCoder);

    TupleTag<KV<K, Iterable<V>>> mainTag = new TupleTag<>("main output");

    WindowDoFnOperator<K, V, Iterable<V>> doFnOperator =
        new WindowDoFnOperator<>(
            reduceFn,
            operatorName,
            (Coder) windowedWorkItemCoder,
            mainTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory(mainTag, outputCoder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            context.getPipelineOptions(),
            inputElementCoder.getKeyCoder(),
            (KeySelector) keySelector /* key selector */);

    SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> outputDataStream =
        keyedWorkItemStream.transform(
            operatorName, outputTypeInfo, (OneInputStreamOperator) doFnOperator);

    return outputDataStream;
  }

  @SuppressWarnings("unchecked")
  private <T> void translateUnboundedRead(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    String outputCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());

    RunnerApi.ReadPayload payload;
    try {
      payload = RunnerApi.ReadPayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse ReadPayload from transform", e);
    }

    Preconditions.checkState(
        payload.getIsBounded() != RunnerApi.IsBounded.Enum.BOUNDED,
        "Bounded reads should run inside an environment instead of being translated by the Runner.");

    DataStream<WindowedValue<T>> source =
        translateUnboundedSource(
            transform.getUniqueName(),
            outputCollectionId,
            payload,
            pipeline,
            context.getPipelineOptions(),
            context.getExecutionEnvironment());

    context.addDataStream(outputCollectionId, source);
  }

  private static <T> DataStream<WindowedValue<T>> translateUnboundedSource(
      String transformName,
      String outputCollectionId,
      RunnerApi.ReadPayload payload,
      RunnerApi.Pipeline pipeline,
      PipelineOptions pipelineOptions,
      StreamExecutionEnvironment env) {

    final DataStream<WindowedValue<T>> source;
    final DataStream<WindowedValue<ValueWithRecordId<T>>> nonDedupSource;
    Coder<WindowedValue<T>> windowCoder =
        instantiateCoder(outputCollectionId, pipeline.getComponents());

    TypeInformation<WindowedValue<T>> outputTypeInfo = new CoderTypeInformation<>(windowCoder);

    WindowingStrategy windowStrategy =
        getWindowingStrategy(outputCollectionId, pipeline.getComponents());
    TypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(
                ValueWithRecordId.ValueWithRecordIdCoder.of(
                    ((WindowedValueCoder) windowCoder).getValueCoder()),
                windowStrategy.getWindowFn().windowCoder()));

    UnboundedSource unboundedSource = ReadTranslation.unboundedSourceFromProto(payload);

    try {
      int parallelism =
          env.getMaxParallelism() > 0 ? env.getMaxParallelism() : env.getParallelism();
      UnboundedSourceWrapper sourceWrapper =
          new UnboundedSourceWrapper<>(
              transformName, pipelineOptions, unboundedSource, parallelism);
      nonDedupSource =
          env.addSource(sourceWrapper)
              .name(transformName)
              .uid(transformName)
              .returns(withIdTypeInfo);

      if (unboundedSource.requiresDeduping()) {
        source =
            nonDedupSource
                .keyBy(new FlinkStreamingTransformTranslators.ValueWithRecordIdKeySelector<>())
                .transform("deduping", outputTypeInfo, new DedupingOperator<>(pipelineOptions))
                .uid(format("%s/__deduplicated__", transformName));
      } else {
        source =
            nonDedupSource
                .flatMap(new FlinkStreamingTransformTranslators.StripIdsMap<>(pipelineOptions))
                .returns(outputTypeInfo);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while translating UnboundedSource: " + unboundedSource, e);
    }

    return source;
  }

  private void translateImpulse(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));

    long shutdownAfterIdleSourcesMs = context.getPipelineOptions().getShutdownSourcesAfterIdleMs();
    SingleOutputStreamOperator<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .addSource(new ImpulseSourceFunction(shutdownAfterIdleSourcesMs), "Impulse")
            .returns(typeInfo);

    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
  }

  /** Predicate to determine whether a URN is a Flink native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return STREAMING_IMPULSE_TRANSFORM_URN.equals(
          PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  private void translateStreamingImpulse(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));

    ObjectMapper objectMapper = new ObjectMapper();
    final int intervalMillis;
    final int messageCount;
    try {
      JsonNode config = objectMapper.readTree(pTransform.getSpec().getPayload().toByteArray());
      intervalMillis = config.path("interval_ms").asInt(100);
      messageCount = config.path("message_count").asInt(0);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse configuration for streaming impulse", e);
    }

    SingleOutputStreamOperator<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .addSource(
                new StreamingImpulseSource(intervalMillis, messageCount),
                StreamingImpulseSource.class.getSimpleName())
            .returns(typeInfo);

    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
  }

  private <InputT, OutputT> void translateExecutableStage(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.
    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    Map<String, String> outputs = transform.getOutputsMap();

    final RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId = stagePayload.getInput();
    final TransformedSideInputs transformedSideInputs;

    if (stagePayload.getSideInputsCount() > 0) {
      transformedSideInputs = transformSideInputs(stagePayload, components, context);
    } else {
      transformedSideInputs = new TransformedSideInputs(Collections.emptyMap(), null);
    }

    Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newLinkedHashMap();
    Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newLinkedHashMap();
    // TODO: does it matter which output we designate as "main"
    final TupleTag<OutputT> mainOutputTag =
        outputs.isEmpty() ? null : new TupleTag(outputs.keySet().iterator().next());

    // associate output tags with ids, output manager uses these Integer ids to serialize state
    BiMap<String, Integer> outputIndexMap = createOutputMap(outputs.keySet());
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
    Map<String, TupleTag<?>> collectionIdToTupleTag = Maps.newHashMap();
    // order output names for deterministic mapping
    for (String localOutputName : new TreeMap<>(outputIndexMap).keySet()) {
      String collectionId = outputs.get(localOutputName);
      Coder<WindowedValue<?>> windowCoder = (Coder) instantiateCoder(collectionId, components);
      outputCoders.put(localOutputName, windowCoder);
      TupleTag<?> tupleTag = new TupleTag<>(localOutputName);
      CoderTypeInformation<WindowedValue<?>> typeInformation =
          new CoderTypeInformation(windowCoder);
      tagsToOutputTags.put(tupleTag, new OutputTag<>(localOutputName, typeInformation));
      tagsToCoders.put(tupleTag, windowCoder);
      tagsToIds.put(tupleTag, outputIndexMap.get(localOutputName));
      collectionIdToTupleTag.put(collectionId, tupleTag);
    }

    final SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;
    DataStream<WindowedValue<InputT>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation =
        (!outputs.isEmpty())
            ? new CoderTypeInformation(outputCoders.get(mainOutputTag.getId()))
            : null;

    ArrayList<TupleTag<?>> additionalOutputTags = Lists.newArrayList();
    for (TupleTag<?> tupleTag : tagsToCoders.keySet()) {
      if (!mainOutputTag.getId().equals(tupleTag.getId())) {
        additionalOutputTags.add(tupleTag);
      }
    }

    final Coder<WindowedValue<InputT>> windowedInputCoder =
        instantiateCoder(inputPCollectionId, components);

    final boolean stateful =
        stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0;
    Coder keyCoder = null;
    KeySelector<WindowedValue<InputT>, ?> keySelector = null;
    if (stateful) {
      // Stateful stages are only allowed of KV input
      Coder valueCoder =
          ((WindowedValue.FullWindowedValueCoder) windowedInputCoder).getValueCoder();
      if (!(valueCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            String.format(
                Locale.ENGLISH,
                "The element coder for stateful DoFn '%s' must be KvCoder but is: %s",
                inputPCollectionId,
                valueCoder.getClass().getSimpleName()));
      }
      keyCoder = ((KvCoder) valueCoder).getKeyCoder();
      keySelector = new KvToByteBufferKeySelector(keyCoder);
      inputDataStream = inputDataStream.keyBy(keySelector);
    }

    DoFnOperator.MultiOutputOutputManagerFactory<OutputT> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainOutputTag, tagsToOutputTags, tagsToCoders, tagsToIds);

    DoFnOperator<InputT, OutputT> doFnOperator =
        new ExecutableStageDoFnOperator<>(
            transform.getUniqueName(),
            windowedInputCoder,
            Collections.emptyMap(),
            mainOutputTag,
            additionalOutputTags,
            outputManagerFactory,
            transformedSideInputs.unionTagToView,
            new ArrayList<>(transformedSideInputs.unionTagToView.values()),
            getSideInputIdToPCollectionViewMap(stagePayload, components),
            context.getPipelineOptions(),
            stagePayload,
            context.getJobInfo(),
            FlinkExecutableStageContextFactory.getInstance(),
            collectionIdToTupleTag,
            getWindowingStrategy(inputPCollectionId, components),
            keyCoder,
            keySelector);

    final String operatorName = generateNameFromStagePayload(stagePayload);

    if (transformedSideInputs.unionTagToView.isEmpty()) {
      outputStream = inputDataStream.transform(operatorName, outputTypeInformation, doFnOperator);
    } else {
      DataStream<RawUnionValue> sideInputStream =
          transformedSideInputs.unionedSideInputs.broadcast();
      if (stateful) {
        // We have to manually construct the two-input transform because we're not
        // allowed to have only one input keyed, normally. Since Flink 1.5.0 it's
        // possible to use the Broadcast State Pattern which provides a more elegant
        // way to process keyed main input with broadcast state, but it's not feasible
        // here because it breaks the DoFnOperator abstraction.
        TwoInputTransformation<WindowedValue<KV<?, InputT>>, RawUnionValue, WindowedValue<OutputT>>
            rawFlinkTransform =
                new TwoInputTransformation(
                    inputDataStream.getTransformation(),
                    sideInputStream.getTransformation(),
                    transform.getUniqueName(),
                    doFnOperator,
                    outputTypeInformation,
                    inputDataStream.getParallelism());

        rawFlinkTransform.setStateKeyType(((KeyedStream) inputDataStream).getKeyType());
        rawFlinkTransform.setStateKeySelectors(
            ((KeyedStream) inputDataStream).getKeySelector(), null);

        outputStream =
            new SingleOutputStreamOperator(
                inputDataStream.getExecutionEnvironment(),
                rawFlinkTransform) {}; // we have to cheat around the ctor being protected
      } else {
        outputStream =
            inputDataStream
                .connect(sideInputStream)
                .transform(operatorName, outputTypeInformation, doFnOperator);
      }
    }
    // Assign a unique but consistent id to re-map operator state
    outputStream.uid(transform.getUniqueName());

    if (mainOutputTag != null) {
      context.addDataStream(outputs.get(mainOutputTag.getId()), outputStream);
    }

    for (TupleTag<?> tupleTag : additionalOutputTags) {
      context.addDataStream(
          outputs.get(tupleTag.getId()),
          outputStream.getSideOutput(tagsToOutputTags.get(tupleTag)));
    }
  }

  private <T> void translateTestStream(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();

    SerializableFunction<byte[], TestStream<T>> testStreamDecoder =
        bytes -> {
          try {
            RunnerApi.TestStreamPayload testStreamPayload =
                RunnerApi.TestStreamPayload.parseFrom(bytes);
            @SuppressWarnings("unchecked")
            TestStream<T> testStream =
                (TestStream<T>)
                    TestStreamTranslation.testStreamFromProtoPayload(
                        testStreamPayload, RehydratedComponents.forComponents(components));
            return testStream;
          } catch (Exception e) {
            throw new RuntimeException("Can't decode TestStream payload.", e);
          }
        };

    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    Coder<WindowedValue<T>> coder = instantiateCoder(outputPCollectionId, components);

    DataStream<WindowedValue<T>> source =
        context
            .getExecutionEnvironment()
            .addSource(
                new TestStreamSource<>(
                    testStreamDecoder, transform.getSpec().getPayload().toByteArray()),
                new CoderTypeInformation<>(coder));

    context.addDataStream(outputPCollectionId, source);
  }

  private static LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
      getSideInputIdToPCollectionViewMap(
          RunnerApi.ExecutableStagePayload stagePayload, RunnerApi.Components components) {

    RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(components);

    LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputs =
        new LinkedHashMap<>();
    // for PCollectionView compatibility, not used to transform materialization
    ViewFn<Iterable<WindowedValue<?>>, ?> viewFn =
        (ViewFn)
            new PCollectionViews.MultimapViewFn<>(
                (PCollectionViews.TypeDescriptorSupplier<Iterable<WindowedValue<Void>>>)
                    () -> TypeDescriptors.iterables(new TypeDescriptor<WindowedValue<Void>>() {}),
                (PCollectionViews.TypeDescriptorSupplier<Void>) TypeDescriptors::voids);

    for (RunnerApi.ExecutableStagePayload.SideInputId sideInputId :
        stagePayload.getSideInputsList()) {

      // TODO: local name is unique as long as only one transform with side input can be within a
      // stage
      String sideInputTag = sideInputId.getLocalName();
      String collectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      RunnerApi.WindowingStrategy windowingStrategyProto =
          components.getWindowingStrategiesOrThrow(
              components.getPcollectionsOrThrow(collectionId).getWindowingStrategyId());

      final WindowingStrategy<?, ?> windowingStrategy;
      try {
        windowingStrategy =
            WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(
            String.format(
                "Unable to hydrate side input windowing strategy %s.", windowingStrategyProto),
            e);
      }

      Coder<WindowedValue<Object>> coder = instantiateCoder(collectionId, components);
      // side input materialization via GBK (T -> Iterable<T>)
      WindowedValueCoder wvCoder = (WindowedValueCoder) coder;
      coder = wvCoder.withValueCoder(IterableCoder.of(wvCoder.getValueCoder()));

      sideInputs.put(
          sideInputId,
          new RunnerPCollectionView<>(
              null,
              new TupleTag<>(sideInputTag),
              viewFn,
              // TODO: support custom mapping fn
              windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
              windowingStrategy,
              coder));
    }
    return sideInputs;
  }

  private TransformedSideInputs transformSideInputs(
      RunnerApi.ExecutableStagePayload stagePayload,
      RunnerApi.Components components,
      StreamingTranslationContext context) {

    LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputs =
        getSideInputIdToPCollectionViewMap(stagePayload, components);

    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    List<WindowedValueCoder<KV<Void, Object>>> kvCoders = new ArrayList<>();
    List<Coder<?>> viewCoders = new ArrayList<>();

    int count = 0;
    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput :
        sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      intToViewMapping.put(count, sideInput.getValue());
      tagToIntMapping.put(tag, count);
      count++;
      String collectionId =
          components
              .getTransformsOrThrow(sideInput.getKey().getTransformId())
              .getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<Object> sideInputStream = context.getDataStreamOrThrow(collectionId);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException("Input Stream TypeInformation is no CoderTypeInformation.");
      }

      WindowedValueCoder<Object> coder =
          (WindowedValueCoder) ((CoderTypeInformation) tpe).getCoder();
      Coder<KV<Void, Object>> kvCoder = KvCoder.of(VoidCoder.of(), coder.getValueCoder());
      kvCoders.add(coder.withValueCoder(kvCoder));
      // coder for materialized view matching GBK below
      WindowedValueCoder<KV<Void, Iterable<Object>>> viewCoder =
          coder.withValueCoder(KvCoder.of(VoidCoder.of(), IterableCoder.of(coder.getValueCoder())));
      viewCoders.add(viewCoder);
    }

    // second pass, now that we gathered the input coders
    UnionCoder unionCoder = UnionCoder.of(viewCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
        new CoderTypeInformation<>(unionCoder);

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput :
        sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      RunnerApi.PTransform pTransform =
          components.getTransformsOrThrow(sideInput.getKey().getTransformId());
      String collectionId = pTransform.getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<WindowedValue<?>> sideInputStream = context.getDataStreamOrThrow(collectionId);

      // insert GBK to materialize side input view
      String viewName =
          sideInput.getKey().getTransformId() + "-" + sideInput.getKey().getLocalName();
      WindowedValueCoder<KV<Void, Object>> kvCoder = kvCoders.get(intTag);
      DataStream<WindowedValue<KV<Void, Object>>> keyedSideInputStream =
          sideInputStream.map(new ToVoidKeyValue(context.getPipelineOptions()));

      SingleOutputStreamOperator<WindowedValue<KV<Void, Iterable<Object>>>> viewStream =
          addGBK(
              keyedSideInputStream,
              sideInput.getValue().getWindowingStrategyInternal(),
              kvCoder,
              viewName,
              context);
      // Assign a unique but consistent id to re-map operator state
      viewStream.uid(pTransform.getUniqueName() + "-" + sideInput.getKey().getLocalName());

      DataStream<RawUnionValue> unionValueStream =
          viewStream
              .map(
                  new FlinkStreamingTransformTranslators.ToRawUnion<>(
                      intTag, context.getPipelineOptions()))
              .returns(unionTypeInformation);

      if (sideInputUnion == null) {
        sideInputUnion = unionValueStream;
      } else {
        sideInputUnion = sideInputUnion.union(unionValueStream);
      }
    }

    return new TransformedSideInputs(intToViewMapping, sideInputUnion);
  }

  private static class TransformedSideInputs {
    final Map<Integer, PCollectionView<?>> unionTagToView;
    final DataStream<RawUnionValue> unionedSideInputs;

    TransformedSideInputs(
        Map<Integer, PCollectionView<?>> unionTagToView,
        DataStream<RawUnionValue> unionedSideInputs) {
      this.unionTagToView = unionTagToView;
      this.unionedSideInputs = unionedSideInputs;
    }
  }

  private static class ToVoidKeyValue<T>
      extends RichMapFunction<WindowedValue<T>, WindowedValue<KV<Void, T>>> {

    private final SerializablePipelineOptions options;

    public ToVoidKeyValue(PipelineOptions pipelineOptions) {
      this.options = new SerializablePipelineOptions(pipelineOptions);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public WindowedValue<KV<Void, T>> map(WindowedValue<T> value) {
      return value.withValue(KV.of(null, value.getValue()));
    }
  }
}
