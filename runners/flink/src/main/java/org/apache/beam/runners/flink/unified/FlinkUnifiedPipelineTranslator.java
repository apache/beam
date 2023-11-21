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
package org.apache.beam.runners.flink.unified;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.CreateStreamingFlinkView;
import org.apache.beam.runners.flink.FlinkExecutionEnvironments;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPortablePipelineTranslator;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.unified.translators.CombinePerKeyTranslator;
import org.apache.beam.runners.flink.unified.translators.CreateViewStreamingTranslator;
import org.apache.beam.runners.flink.unified.translators.ExecutableStageTranslator;
import org.apache.beam.runners.flink.unified.translators.FlattenTranslator;
import org.apache.beam.runners.flink.unified.translators.GBKIntoKeyedWorkItemsTranslator;
import org.apache.beam.runners.flink.unified.translators.GroupByKeyTranslator;
import org.apache.beam.runners.flink.unified.translators.ImpulseTranslator;
import org.apache.beam.runners.flink.unified.translators.NotImplementedTranslator;
import org.apache.beam.runners.flink.unified.translators.ParDoTranslator;
import org.apache.beam.runners.flink.unified.translators.ReadSourceTranslator;
import org.apache.beam.runners.flink.unified.translators.ReshuffleTranslator;
import org.apache.beam.runners.flink.unified.translators.SplittableProcessElementsStreamingTranslator;
import org.apache.beam.runners.flink.unified.translators.TestStreamTranslator;
import org.apache.beam.runners.flink.unified.translators.WindowAssignTranslator;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Translate a pipeline representation into a Flink pipeline representation. */
public class FlinkUnifiedPipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkUnifiedPipelineTranslator.class);

  private final Map<
          String, PTransformTranslator<FlinkUnifiedPipelineTranslator.UnifiedTranslationContext>>
      urnToTransformTranslator;

  private boolean isStreaming;
  private boolean isPortableRunnerExec;

  @Override
  public UnifiedTranslationContext createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage) {
    StreamExecutionEnvironment executionEnvironment =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            pipelineOptions, filesToStage, confDir);
    return new UnifiedTranslationContext(
        jobInfo, pipelineOptions, executionEnvironment, isStreaming, isPortableRunnerExec);
  }

  public UnifiedTranslationContext createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      StreamExecutionEnvironment executionEnvironment,
      boolean isStreaming,
      boolean isPortableRunnerExec) {

    return new UnifiedTranslationContext(
        jobInfo, pipelineOptions, executionEnvironment, isStreaming, isPortableRunnerExec);
  }

  public static class UnifiedTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext,
          FlinkPortablePipelineTranslator.Executor {

    private final JobInfo jobInfo;
    private final FlinkPipelineOptions options;
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;
    private final Map<PCollectionView<?>, DataStream<?>> sideInputs;
    private final Map<String, PipelineNode.PTransformNode> producers = new HashMap<>();
    @Nullable private PipelineNode.PTransformNode currentTransform;
    private final boolean isStreaming;
    private final boolean isPortableRunnerExec;

    private UnifiedTranslationContext(
        JobInfo jobInfo,
        FlinkPipelineOptions options,
        StreamExecutionEnvironment executionEnvironment,
        boolean isStreaming,
        boolean isPortableRunnerExec) {
      this.jobInfo = jobInfo;
      this.options = options;
      this.executionEnvironment = executionEnvironment;
      dataStreams = new HashMap<>();
      sideInputs = new HashMap<>();
      this.isStreaming = isStreaming;
      this.isPortableRunnerExec = isPortableRunnerExec;
    }

    /**
     * Sets the AppliedPTransform which carries input/output.
     *
     * @param currentTransform
     */
    public void setCurrentTransform(PipelineNode.PTransformNode currentTransform) {
      this.currentTransform = currentTransform;
    }

    public boolean isPortableRunnerExec() {
      return isPortableRunnerExec;
    }

    @Nullable
    public PipelineNode.PTransformNode getCurrentTransform() {
      return currentTransform;
    }

    @Nullable
    public PipelineNode.PTransformNode getProducer(String pCollectionId) {
      return producers.get(pCollectionId);
    }

    @Override
    public JobInfo getJobInfo() {
      return jobInfo;
    }

    @Override
    public FlinkPipelineOptions getPipelineOptions() {
      return options;
    }

    public boolean isStreaming() {
      return isStreaming;
    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
      StreamExecutionEnvironment env = getExecutionEnvironment();
      if (!getPipelineOptions().isStreaming()) {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
      }
      return env.execute(jobName);
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    public <T> void addDataStream(String pCollectionId, DataStream<T> dataStream) {
      PipelineNode.PTransformNode current = getCurrentTransform();
      PipelineNode.PTransformNode previousProducer;
      if (current != null) {
        previousProducer = producers.put(pCollectionId, current);
        Preconditions.checkArgument(
            previousProducer == null, "PValue can only have a single producer.");
      }
      dataStreams.put(pCollectionId, dataStream);
    }

    public <T> DataStream<T> getDataStreamOrThrow(String pCollectionId) {
      DataStream<T> dataStream = (DataStream) dataStreams.get(pCollectionId);
      if (dataStream == null) {
        throw new IllegalArgumentException(
            String.format("Unknown datastream for pCollectionId %s.", pCollectionId));
      }
      return dataStream;
    }

    public <T> void addSideInputDataStream(PCollectionView<?> view, DataStream<T> dataStream) {
      sideInputs.put(view, dataStream);
    }

    public <T> DataStream<T> getSideInputDataStream(PCollectionView<T> view) {
      DataStream<T> dataStream = (DataStream) sideInputs.get(view);
      if (dataStream == null) {
        throw new IllegalArgumentException(String.format("Unknown datastream for view %s.", view));
      }
      return dataStream;
    }

    public RehydratedComponents getComponents(RunnerApi.Components components) {
      return RehydratedComponents.forComponents(components);
    }

    public RehydratedComponents getComponents(RunnerApi.Pipeline pipeline) {
      return getComponents(pipeline.getComponents());
    }

    public WindowingStrategy<?, ?> getWindowingStrategy(
        RunnerApi.Pipeline pipeline, String pCollectionId) {
      RunnerApi.WindowingStrategy windowingStrategyProto =
          pipeline
              .getComponents()
              .getWindowingStrategiesOrThrow(
                  pipeline
                      .getComponents()
                      .getPcollectionsOrThrow(pCollectionId)
                      .getWindowingStrategyId());
      try {
        return WindowingStrategyTranslation.fromProto(
            windowingStrategyProto, getComponents(pipeline));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(
            String.format("Unable to hydrate windowing strategy %s.", windowingStrategyProto), e);
      }
    }

    /**
     * XXX: When a ParDo emits Rows and a RowCoder is set with an explicit Schema,
     * SimpleParDoRunner expects the Coder to be an instance of SchemaCoder.
     * If that's not the case, a NPE is thrown.
     */
    public <T> Coder<T> getOutputCoderHack(
      RunnerApi.Pipeline pipeline, String pCollectionId) {
        WindowedValueCoder<T> coder = getWindowedInputCoder(pipeline, pCollectionId);
        Coder<T> valueCoder = coder.getValueCoder();
        if(valueCoder instanceof LengthPrefixCoder) {
          Coder<T> outputCoder = ((LengthPrefixCoder) valueCoder).getValueCoder();
          if(outputCoder instanceof SchemaCoder) {
            return outputCoder;
          }
        }
        return (Coder) coder;


    }

    /**
     * Get SDK coder for given PCollection. The SDK coder is the coder that the SDK-harness would
     * have used to encode data before passing it to the runner over {@link SdkHarnessClient}.
     *
     * @param pCollectionId ID of PCollection in components
     * @param components the Pipeline components (proto)
     * @return SDK-side coder for the PCollection
     */
    public <T> WindowedValue.FullWindowedValueCoder<T> getSdkCoder(
        String pCollectionId, RunnerApi.Components components) {

      PipelineNode.PCollectionNode pCollectionNode =
          PipelineNode.pCollection(pCollectionId, components.getPcollectionsOrThrow(pCollectionId));
      RunnerApi.Components.Builder componentsBuilder = components.toBuilder();
      String coderId =
          WireCoders.addSdkWireCoder(
              pCollectionNode,
              componentsBuilder,
              RunnerApi.ExecutableStagePayload.WireCoderSetting.getDefaultInstance());
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(componentsBuilder.build());
      try {
        @SuppressWarnings("unchecked")
        WindowedValue.FullWindowedValueCoder<T> res =
            (WindowedValue.FullWindowedValueCoder<T>) rehydratedComponents.getCoder(coderId);
        return res;
      } catch (IOException ex) {
        throw new IllegalStateException("Could not get SDK coder.", ex);
      }
    }

    public <T> WindowedValueCoder<T> getWindowedInputCoder(
        RunnerApi.Pipeline pipeline, String pCollectionId) {
      if (isPortableRunnerExec()) {
        // In case if portable execution, we use the wire coder provided by PipelineTranslatorUtils.
        return (WindowedValueCoder)
            PipelineTranslatorUtils.instantiateCoder(pCollectionId, pipeline.getComponents());
      } else {
        // In case of legacy execution, return the SDK Coder
        LOG.debug(
            String.format(
                "Coder for %s is %s",
                pCollectionId, getSdkCoder(pCollectionId, pipeline.getComponents())));
        return getSdkCoder(pCollectionId, pipeline.getComponents());
      }
    }

    public <T> TypeInformation<WindowedValue<T>> getTypeInfo(
        RunnerApi.Pipeline pipeline, String pCollectionId) {
      WindowedValueCoder<T> windowedInputCoder = getWindowedInputCoder(pipeline, pCollectionId);
      return new CoderTypeInformation<WindowedValue<T>>(windowedInputCoder, getPipelineOptions());
    }
  }

  public interface PTransformTranslator<T> {
    void translate(PTransformNode transform, RunnerApi.Pipeline pipeline, T t);
  }

  protected FlinkUnifiedPipelineTranslator(
      Map<String, PTransformTranslator<FlinkUnifiedPipelineTranslator.UnifiedTranslationContext>>
          translatorMap,
      boolean isStreaming,
      boolean isPortableRunnerExec) {

    this.urnToTransformTranslator = translatorMap;
    this.isStreaming = isStreaming;
    this.isPortableRunnerExec = isPortableRunnerExec;
  }

  private static Map<String, PTransformTranslator<UnifiedTranslationContext>>
      getPortableTranslators() {
    return ImmutableMap.<String, PTransformTranslator<UnifiedTranslationContext>>builder()
        .put(ExecutableStage.URN, new ExecutableStageTranslator<>())
        .build();
  }

  private static Map<String, PTransformTranslator<UnifiedTranslationContext>>
      getNativeTranslators() {
    return ImmutableMap.<String, PTransformTranslator<UnifiedTranslationContext>>builder()
        .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslator<>())
        .put(SplittableParDo.SPLITTABLE_GBKIKWI_URN, new GBKIntoKeyedWorkItemsTranslator<>())
        .put(
            SplittableParDo.SPLITTABLE_PROCESS_URN,
            new SplittableProcessElementsStreamingTranslator<>())
        .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator<>())
        .put(
            CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN,
            new CreateViewStreamingTranslator<>())
        .put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new CombinePerKeyTranslator<>())
        .build();
  }

  /** @deprecated Legacy non-portable source which can be replaced by a DoFn with timers. */
  @Deprecated
  private static final String STREAMING_IMPULSE_TRANSFORM_URN =
      "flink:transform:streaming_impulse:v1";

  public static FlinkUnifiedPipelineTranslator createTranslator(
      boolean isStreaming, boolean isPortableRunnerExec) {
    ImmutableMap.Builder<String, PTransformTranslator<UnifiedTranslationContext>> translatorMap =
        ImmutableMap.builder();

    // Common transforms
    translatorMap
        .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator<>())
        .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>())
        .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator())
        .put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslator<>());

    if (isPortableRunnerExec) {
      translatorMap.putAll(getPortableTranslators());
    } else {
      translatorMap.putAll(getNativeTranslators());
    }

    // ---
    // Streaming only transforms
    // TODO Legacy transforms which need to be removed
    // Consider removing now that timers are supported
    translatorMap
        .put(
            STREAMING_IMPULSE_TRANSFORM_URN,
            new NotImplementedTranslator<>(STREAMING_IMPULSE_TRANSFORM_URN))
        // Remove once unbounded Reads can be wrapped in SDFs
        .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator<>())
        // For testing only
        .put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator<>());

    return new FlinkUnifiedPipelineTranslator(
        translatorMap.build(), isStreaming, isPortableRunnerExec);
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

  /** Predicate to determine whether a URN is a Flink native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return PTransformTranslation.RESHUFFLE_URN.equals(
          PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  private void urnNotFound(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    // Detect and ignore No-op transform
    // (PCollectionTuple projection. See PipelineTest.testTupleProjectionTransform)
    String urn = transform.getTransform().getSpec().getUrn();
    if(urn.isEmpty()) {
      String input = Iterables.getOnlyElement(transform.getTransform().getInputsMap().entrySet()).getValue();
      String output = Iterables.getOnlyElement(transform.getTransform().getOutputsMap().entrySet()).getValue();
      if(input.equals(output)) {
        return;
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN `%s` for PTransform with id %s.",
            transform.getTransform().getSpec().getUrn(), transform.getId()));
  }

  private List<String> getExpandedTransformsList(
      List<String> rootTransforms, RunnerApi.Components components) {
    return rootTransforms.stream()
        .flatMap(s -> getExpandedTransformsList(s, components))
        .collect(Collectors.toList());
  }

  @SuppressWarnings({"dereference.of.nullable"})
  private Stream<String> getExpandedTransformsList(
      String transform, RunnerApi.Components components) {
    RunnerApi.PTransform t = components.getTransformsMap().get(transform);
    if (t.getSubtransformsCount() > 0) {
      return t.getSubtransformsList().stream()
          .flatMap(sub -> getExpandedTransformsList(sub, components));
    } else {
      return Stream.of(transform);
    }
  }

  @Override
  public Executor translate(UnifiedTranslationContext context, RunnerApi.Pipeline pipeline) {

    // QueryablePipeline p =
    // QueryablePipeline.forTransforms(getExpandedTransformsList(pipeline.getRootTransformIdsList(),
    // pipeline.getComponents()), pipeline.getComponents());

    // List<PipelineNode.PTransformNode> expandedTopologicalOrder =
    //   p.getTopologicallyOrderedTransforms();

    List<PipelineNode.PTransformNode> expandedTopologicalOrder =
        getExpandedTransformsList(pipeline.getRootTransformIdsList(), pipeline.getComponents())
            .stream()
            .map(
                t -> {
                  RunnerApi.PTransform pt = pipeline.getComponents().getTransformsMap().get(t);
                  if (pt == null) {
                    throw new RuntimeException("PTranform not found: " + t);
                  }
                  return PipelineNode.pTransform(t, pt);
                })
            .collect(Collectors.toList());

    for (PipelineNode.PTransformNode transform : expandedTopologicalOrder) {
      context.setCurrentTransform(transform);
      String name = transform.getTransform().getUniqueName();
      String urn = transform.getTransform().getSpec().getUrn();
      LOG.debug(
          "Translating :"
              + name
              + " with URN "
              + urn
              + "with "
              + urnToTransformTranslator.getOrDefault(urn, this::urnNotFound).getClass());
      urnToTransformTranslator
          .getOrDefault(urn, this::urnNotFound)
          .translate(transform, pipeline, context);
    }

    return context;
  }

  private Stream<PipelineNode.PTransformNode> expandNode(
      PipelineNode.PTransformNode node, RunnerApi.Components components) {
    if (node.getTransform().getSubtransformsCount() > 0) {
      Map<String, RunnerApi.PTransform> transforms = components.getTransformsMap();
      return node.getTransform().getSubtransformsList().stream()
          .map(
              s -> {
                RunnerApi.PTransform t = transforms.get(s);
                if (t == null) {
                  throw new IllegalStateException("Transform not found");
                }
                return PipelineNode.pTransform(s, t);
              })
          .flatMap(n -> expandNode(n, components));
    } else {
      return Stream.of(node);
    }
  }
}
