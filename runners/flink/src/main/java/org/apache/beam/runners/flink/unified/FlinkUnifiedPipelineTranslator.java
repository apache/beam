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
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.FlinkExecutionEnvironments;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPortablePipelineTranslator;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.unified.translators.CombinePerKeyTranslator;
import org.apache.beam.runners.flink.unified.translators.ExecutableStageTranslator;
import org.apache.beam.runners.flink.unified.translators.FlattenTranslator;
import org.apache.beam.runners.flink.unified.translators.GBKIntoKeyedWorkItemsTranslator;
import org.apache.beam.runners.flink.unified.translators.GroupByKeyTranslator;
import org.apache.beam.runners.flink.unified.translators.ImpulseTranslator;
import org.apache.beam.runners.flink.unified.translators.NotImplementedTranslator;
import org.apache.beam.runners.flink.unified.translators.ParDoTranslator;
import org.apache.beam.runners.flink.unified.translators.ReadSourceTranslator;
import org.apache.beam.runners.flink.unified.translators.ReshuffleTranslator;
import org.apache.beam.runners.flink.unified.translators.TestStreamTranslator;
import org.apache.beam.runners.flink.unified.translators.WindowAssignTranslator;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Translate a pipeline representation into a Flink pipeline representation. */
public class FlinkUnifiedPipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

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
      jobInfo,
      pipelineOptions,
      executionEnvironment,
      isStreaming,
      isPortableRunnerExec);
  }

  public static class UnifiedTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext,
          FlinkPortablePipelineTranslator.Executor {

    private final JobInfo jobInfo;
    private final FlinkPipelineOptions options;
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;
    private final Map<String, PipelineNode.PTransformNode> producers = new HashMap<>();
    @Nullable
    private PipelineNode.PTransformNode currentTransform;
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
      return getExecutionEnvironment().execute(jobName);
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

    public <T> WindowedValueCoder<T> getWindowedInputCoder(RunnerApi.Pipeline pipeline, String pCollectionId) {
        return (WindowedValueCoder) PipelineTranslatorUtils.instantiateCoder(pCollectionId, pipeline.getComponents());
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
    Map<String, PTransformTranslator<FlinkUnifiedPipelineTranslator.UnifiedTranslationContext>> translatorMap,
    boolean isStreaming,
    boolean isPortableRunnerExec) {

    this.urnToTransformTranslator = translatorMap;
    this.isStreaming = isStreaming;
    this.isPortableRunnerExec = isPortableRunnerExec;
  }

  private static Map<String, PTransformTranslator<UnifiedTranslationContext>> getPortableTranslators() {
    ImmutableMap.Builder<String, PTransformTranslator<UnifiedTranslationContext>> translatorMap =
        ImmutableMap.builder();
    translatorMap.put(ExecutableStage.URN, new ExecutableStageTranslator<>());
    return translatorMap.build();
  }

  private static Map<String, PTransformTranslator<UnifiedTranslationContext>> getNativeTranslators() {
    ImmutableMap.Builder<String, PTransformTranslator<UnifiedTranslationContext>> translatorMap =
        ImmutableMap.builder();

    translatorMap.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslator<>());
    translatorMap.put(
        SplittableParDo.SPLITTABLE_GBKIKWI_URN, new GBKIntoKeyedWorkItemsTranslator<>());
    translatorMap.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator<>());
    translatorMap.put(org.apache.beam.runners.flink.CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN,
      new NotImplementedTranslator<>());
    translatorMap.put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new CombinePerKeyTranslator<>());

    return translatorMap.build();
  }

  @Deprecated
  private static final String STREAMING_IMPULSE_TRANSFORM_URN =
      "flink:transform:streaming_impulse:v1";

  public static FlinkUnifiedPipelineTranslator createTranslator(
    boolean isStreaming,
    boolean isPortableRunnerExec) {
    ImmutableMap.Builder<String, PTransformTranslator<UnifiedTranslationContext>> translatorMap =
        ImmutableMap.builder();

    // Common transforms
    translatorMap.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator<>());
    translatorMap.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>());
    translatorMap.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
    translatorMap.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslator<>());

    if(isPortableRunnerExec) {
      translatorMap.putAll(getPortableTranslators());
    } else {
      translatorMap.putAll(getNativeTranslators());
    }

    // ---
    // Streaming only transforms
    // TODO Legacy transforms which need to be removed
    // Consider removing now that timers are supported
    translatorMap.put(STREAMING_IMPULSE_TRANSFORM_URN, new NotImplementedTranslator<>());
    // Remove once unbounded Reads can be wrapped in SDFs
    translatorMap.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator<>());
    // For testing only
    translatorMap.put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator<>());

    return new FlinkUnifiedPipelineTranslator(
      translatorMap.build(),
      isStreaming,
      isPortableRunnerExec);
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
    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN %s for PTransform with id %s.",
            transform.getTransform().getSpec().getUrn(), transform.getId()));
  }

  @Override
  public Executor translate(UnifiedTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      context.setCurrentTransform(transform);
      String urn = transform.getTransform().getSpec().getUrn();
      urnToTransformTranslator.getOrDefault(urn, this::urnNotFound)
        .translate(transform, pipeline, context);
    }

    return context;
  }
}
