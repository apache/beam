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
package org.apache.beam.runners.direct.portable;

import static org.apache.beam.runners.core.construction.SyntheticComponents.uniqueId;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables.getOnlyElement;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.Resources;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ComponentsOrBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform.Builder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.ModelCoders.KvCoderComponents;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.PipelineValidator;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides.TransformReplacement;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.SingleEnvironmentInstanceJobBundleFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The "ReferenceRunner" engine implementation. The ReferenceRunner uses the portability framework
 * to execute a Pipeline on a single machine.
 */
public class ReferenceRunner {
  private final RunnerApi.Pipeline pipeline;
  private final Struct options;
  private final String artifactRetrievalToken;

  private final EnvironmentType environmentType;

  private final IdGenerator idGenerator = IdGenerators.incrementingLongs();

  /** @param environmentType The environment to use for the SDK Harness. */
  private ReferenceRunner(
      Pipeline p, Struct options, String artifactRetrievalToken, EnvironmentType environmentType) {
    this.pipeline = executable(p);
    this.options = options;
    this.environmentType = environmentType;
    this.artifactRetrievalToken = artifactRetrievalToken;
  }

  /**
   * Creates a "ReferenceRunner" engine for a single pipeline with a Dockerized SDK harness.
   *
   * @param p Pipeline being executed for this job.
   * @param options PipelineOptions for this job.
   * @param artifactRetrievalToken Token to retrieve artifacts that have been staged.
   */
  public static ReferenceRunner forPipeline(
      RunnerApi.Pipeline p, Struct options, String artifactRetrievalToken) {
    return new ReferenceRunner(p, options, artifactRetrievalToken, EnvironmentType.DOCKER);
  }

  static ReferenceRunner forInProcessPipeline(RunnerApi.Pipeline p, Struct options) {
    return new ReferenceRunner(p, options, "", EnvironmentType.IN_PROCESS);
  }

  private RunnerApi.Pipeline executable(RunnerApi.Pipeline original) {
    RunnerApi.Pipeline p = original;
    PipelineValidator.validate(p);
    p =
        ProtoOverrides.updateTransform(
            PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
            p,
            new SplittableProcessKeyedReplacer());
    p =
        ProtoOverrides.updateTransform(
            PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, p, new PortableGroupByKeyReplacer());
    p = GreedyPipelineFuser.fuse(p).toPipeline();

    p = foldFeedSDFIntoExecutableStage(p);
    PipelineValidator.validate(p);

    return p;
  }

  private static Set<PCollectionNode> getKeyedPCollections(
      ExecutableGraph<PTransformNode, PCollectionNode> graph) {
    // This mimics KeyedPValueTrackingVisitor behavior in regular direct runner,
    // but without propagating keyed-ness through key-preserving DoFn's.
    // That is not yet necessary, but will be necessary once we implement state and timers.
    // See https://issues.apache.org/jira/browse/BEAM-4557.
    Set<PCollectionNode> res = Sets.newHashSet();
    Set<String> keyedProducers =
        Sets.newHashSet(DirectGroupByKey.DIRECT_GBKO_URN, DirectGroupByKey.DIRECT_GABW_URN);
    for (PTransformNode transform : graph.getExecutables()) {
      if (keyedProducers.contains(transform.getTransform().getSpec().getUrn())) {
        res.addAll(graph.getProduced(transform));
      }
    }
    return res;
  }

  /**
   * First starts all the services needed, then configures and starts the {@link
   * ExecutorServiceParallelExecutor}.
   */
  public void execute() throws Exception {
    ExecutableGraph<PTransformNode, PCollectionNode> graph = PortableGraph.forPipeline(pipeline);
    BundleFactory bundleFactory = ImmutableListBundleFactory.create();
    EvaluationContext ctxt =
        EvaluationContext.create(Instant::new, bundleFactory, graph, getKeyedPCollections(graph));
    RootProviderRegistry rootRegistry = RootProviderRegistry.javaPortableRegistry(bundleFactory);
    int targetParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 3);
    ServerFactory serverFactory = createServerFactory();
    ControlClientPool controlClientPool = MapControlClientPool.create();
    ExecutorService dataExecutor = Executors.newCachedThreadPool();
    ProvisionInfo provisionInfo =
        ProvisionInfo.newBuilder()
            .setJobId("id")
            .setJobName("reference")
            .setPipelineOptions(options)
            .setWorkerId("foo")
            .setResourceLimits(Resources.getDefaultInstance())
            .setRetrievalToken(artifactRetrievalToken)
            .build();
    try (GrpcFnServer<GrpcLoggingService> logging =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
        GrpcFnServer<ArtifactRetrievalService> artifact =
            GrpcFnServer.allocatePortAndCreateFor(
                BeamFileSystemArtifactRetrievalService.create(), serverFactory);
        GrpcFnServer<StaticGrpcProvisionService> provisioning =
            GrpcFnServer.allocatePortAndCreateFor(
                StaticGrpcProvisionService.create(provisionInfo), serverFactory);
        GrpcFnServer<FnApiControlClientPoolService> control =
            GrpcFnServer.allocatePortAndCreateFor(
                FnApiControlClientPoolService.offeringClientsToPool(
                    controlClientPool.getSink(),
                    GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
                serverFactory);
        GrpcFnServer<GrpcDataService> data =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcDataService.create(dataExecutor, OutboundObserverFactory.serverDirect()),
                serverFactory);
        GrpcFnServer<GrpcStateService> state =
            GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory)) {

      EnvironmentFactory environmentFactory =
          createEnvironmentFactory(control, logging, artifact, provisioning, controlClientPool);
      JobBundleFactory jobBundleFactory =
          SingleEnvironmentInstanceJobBundleFactory.create(
              environmentFactory, data, state, idGenerator);

      TransformEvaluatorRegistry transformRegistry =
          TransformEvaluatorRegistry.portableRegistry(
              graph,
              pipeline.getComponents(),
              bundleFactory,
              jobBundleFactory,
              EvaluationContextStepStateAndTimersProvider.forContext(ctxt));
      ExecutorServiceParallelExecutor executor =
          ExecutorServiceParallelExecutor.create(
              targetParallelism, rootRegistry, transformRegistry, graph, ctxt);
      executor.start();
      executor.waitUntilFinish(Duration.ZERO);
    } finally {
      dataExecutor.shutdown();
    }
  }

  private ServerFactory createServerFactory() {
    switch (environmentType) {
      case DOCKER:
        return ServerFactory.createDefault();
      case IN_PROCESS:
        return InProcessServerFactory.create();
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s %s", EnvironmentType.class.getSimpleName(), environmentType));
    }
  }

  private EnvironmentFactory createEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> control,
      GrpcFnServer<GrpcLoggingService> logging,
      GrpcFnServer<ArtifactRetrievalService> artifact,
      GrpcFnServer<StaticGrpcProvisionService> provisioning,
      ControlClientPool controlClient) {
    switch (environmentType) {
      case DOCKER:
        return new DockerEnvironmentFactory.Provider(PipelineOptionsTranslation.fromProto(options))
            .createEnvironmentFactory(
                control, logging, artifact, provisioning, controlClient, idGenerator);
      case IN_PROCESS:
        return EmbeddedEnvironmentFactory.create(
            PipelineOptionsFactory.create(), logging, control, controlClient.getSource());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s %s", EnvironmentType.class.getSimpleName(), environmentType));
    }
  }

  @VisibleForTesting
  static class PortableGroupByKeyReplacer implements TransformReplacement {
    @Override
    public MessageWithComponents getReplacement(String gbkId, ComponentsOrBuilder components) {
      PTransform gbk = components.getTransformsOrThrow(gbkId);
      checkArgument(
          PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN.equals(gbk.getSpec().getUrn()),
          "URN must be %s, got %s",
          PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
          gbk.getSpec().getUrn());

      PTransform.Builder newTransform = gbk.toBuilder();
      Components.Builder newComponents = Components.newBuilder();
      String inputId = getOnlyElement(gbk.getInputsMap().values());

      // Add the GBKO transform
      String kwiCollectionId =
          uniqueId(String.format("%s.%s", inputId, "kwi"), components::containsPcollections);
      {
        PCollection input = components.getPcollectionsOrThrow(inputId);
        Coder inputCoder = components.getCodersOrThrow(input.getCoderId());
        KvCoderComponents kvComponents = ModelCoders.getKvCoderComponents(inputCoder);
        String windowCoderId =
            components
                .getWindowingStrategiesOrThrow(input.getWindowingStrategyId())
                .getWindowCoderId();
        // This coder isn't actually required for the pipeline to function properly - the KWIs can
        // be passed around as pure java objects with no coding of the values, but it approximates
        // a full pipeline.
        Coder kwiCoder =
            Coder.newBuilder()
                .setSpec(
                    SdkFunctionSpec.newBuilder()
                        .setSpec(FunctionSpec.newBuilder().setUrn("beam:direct:keyedworkitem:v1")))
                .addAllComponentCoderIds(
                    ImmutableList.of(
                        kvComponents.keyCoderId(), kvComponents.valueCoderId(), windowCoderId))
                .build();
        String kwiCoderId =
            uniqueId(
                String.format("kwi(%s:%s)", kvComponents.keyCoderId(), kvComponents.valueCoderId()),
                components::containsCoders);
        // The kwi PCollection has the same WindowingStrategy as the input, as no merging will
        // have been performed, so elements remain in their original windows
        PCollection kwi =
            input.toBuilder().setUniqueName(kwiCollectionId).setCoderId(kwiCoderId).build();
        String gbkoId = uniqueId(String.format("%s/GBKO", gbkId), components::containsTransforms);
        PTransform gbko =
            PTransform.newBuilder()
                .setUniqueName(String.format("%s/GBKO", gbk.getUniqueName()))
                .putAllInputs(gbk.getInputsMap())
                .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GBKO_URN))
                .putOutputs("output", kwiCollectionId)
                .build();

        newTransform.addSubtransforms(gbkoId);
        newComponents
            .putCoders(kwiCoderId, kwiCoder)
            .putPcollections(kwiCollectionId, kwi)
            .putTransforms(gbkoId, gbko);
      }

      // Add the GABW transform
      {
        String gabwId = uniqueId(String.format("%s/GABW", gbkId), components::containsTransforms);
        PTransform gabw =
            PTransform.newBuilder()
                .setUniqueName(String.format("%s/GABW", gbk.getUniqueName()))
                .putInputs("input", kwiCollectionId)
                .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GABW_URN))
                .putAllOutputs(gbk.getOutputsMap())
                .build();
        newTransform.addSubtransforms(gabwId);
        newComponents.putTransforms(gabwId, gabw);
      }

      return MessageWithComponents.newBuilder()
          .setPtransform(newTransform)
          .setComponents(newComponents)
          .build();
    }
  }

  /**
   * Replaces the {@link PTransformTranslation#SPLITTABLE_PROCESS_KEYED_URN} with a {@link
   * DirectGroupByKey#DIRECT_GBKO_URN} (construct keyed work items) followed by a {@link
   * SplittableRemoteStageEvaluatorFactory#FEED_SDF_URN} (convert the keyed work items to
   * element/restriction pairs that later go into {@link
   * PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN}).
   */
  @VisibleForTesting
  static class SplittableProcessKeyedReplacer implements TransformReplacement {
    @Override
    public MessageWithComponents getReplacement(String spkId, ComponentsOrBuilder components) {
      PTransform spk = components.getTransformsOrThrow(spkId);
      checkArgument(
          PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN.equals(spk.getSpec().getUrn()),
          "URN must be %s, got %s",
          PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
          spk.getSpec().getUrn());

      Components.Builder newComponents = Components.newBuilder();
      newComponents.putAllCoders(components.getCodersMap());

      Builder newPTransform = spk.toBuilder();

      String inputId = getOnlyElement(spk.getInputsMap().values());
      PCollection input = components.getPcollectionsOrThrow(inputId);

      // This is a Coder<KV<String, KV<ElementT, RestrictionT>>>
      Coder inputCoder = components.getCodersOrThrow(input.getCoderId());
      KvCoderComponents kvComponents = ModelCoders.getKvCoderComponents(inputCoder);
      String windowCoderId =
          components
              .getWindowingStrategiesOrThrow(input.getWindowingStrategyId())
              .getWindowCoderId();

      // === Construct a raw GBK returning KeyedWorkItem's ===
      String kwiCollectionId =
          uniqueId(String.format("%s.kwi", spkId), components::containsPcollections);
      {
        // This coder isn't actually required for the pipeline to function properly - the KWIs can
        // be passed around as pure java objects with no coding of the values, but it approximates a
        // full pipeline.
        Coder kwiCoder =
            Coder.newBuilder()
                .setSpec(
                    SdkFunctionSpec.newBuilder()
                        .setSpec(FunctionSpec.newBuilder().setUrn("beam:direct:keyedworkitem:v1")))
                .addAllComponentCoderIds(
                    ImmutableList.of(
                        kvComponents.keyCoderId(), kvComponents.valueCoderId(), windowCoderId))
                .build();
        String kwiCoderId =
            uniqueId(
                String.format(
                    "keyed_work_item(%s:%s)",
                    kvComponents.keyCoderId(), kvComponents.valueCoderId()),
                components::containsCoders);

        PCollection kwiCollection =
            input.toBuilder().setUniqueName(kwiCollectionId).setCoderId(kwiCoderId).build();
        String rawGbkId =
            uniqueId(String.format("%s/RawGBK", spkId), components::containsTransforms);
        PTransform rawGbk =
            PTransform.newBuilder()
                .setUniqueName(String.format("%s/RawGBK", spk.getUniqueName()))
                .putAllInputs(spk.getInputsMap())
                .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GBKO_URN))
                .putOutputs("output", kwiCollectionId)
                .build();

        newComponents
            .putCoders(kwiCoderId, kwiCoder)
            .putPcollections(kwiCollectionId, kwiCollection)
            .putTransforms(rawGbkId, rawGbk);
        newPTransform.addSubtransforms(rawGbkId);
      }

      // === Construct a "Feed SDF" operation that converts KWI to KV<ElementT, RestrictionT> ===
      String feedSDFCollectionId =
          uniqueId(String.format("%s.feed", spkId), components::containsPcollections);
      {
        String elementRestrictionCoderId = kvComponents.valueCoderId();
        String feedSDFCoderId =
            LengthPrefixUnknownCoders.addLengthPrefixedCoder(
                elementRestrictionCoderId, newComponents, false);

        PCollection feedSDFCollection =
            input.toBuilder().setUniqueName(feedSDFCollectionId).setCoderId(feedSDFCoderId).build();
        String feedSDFId =
            uniqueId(String.format("%s/FeedSDF", spkId), components::containsTransforms);
        PTransform feedSDF =
            PTransform.newBuilder()
                .setUniqueName(String.format("%s/FeedSDF", spk.getUniqueName()))
                .putInputs("input", kwiCollectionId)
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(SplittableRemoteStageEvaluatorFactory.FEED_SDF_URN))
                .putOutputs("output", feedSDFCollectionId)
                .build();

        newComponents
            .putPcollections(feedSDFCollectionId, feedSDFCollection)
            .putTransforms(feedSDFId, feedSDF);
        newPTransform.addSubtransforms(feedSDFId);
      }

      // === Construct the SPLITTABLE_PROCESS_ELEMENTS operation
      {
        String runSDFId =
            uniqueId(String.format("%s/RunSDF", spkId), components::containsTransforms);
        PTransform runSDF =
            PTransform.newBuilder()
                .setUniqueName(String.format("%s/RunSDF", spk.getUniqueName()))
                .putInputs("input", feedSDFCollectionId)
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN)
                        .setPayload(spk.getSpec().getPayload()))
                .putAllOutputs(spk.getOutputsMap())
                .build();
        newComponents.putTransforms(runSDFId, runSDF);
        newPTransform.addSubtransforms(runSDFId);
      }

      return MessageWithComponents.newBuilder()
          .setPtransform(newPTransform.build())
          .setComponents(newComponents)
          .build();
    }
  }

  /**
   * Finds FEED_SDF nodes followed by an ExecutableStage and replaces them by a single {@link
   * SplittableRemoteStageEvaluatorFactory#URN} stage that feeds the ExecutableStage knowing that
   * the first instruction in the stage is an SDF.
   */
  private static Pipeline foldFeedSDFIntoExecutableStage(Pipeline p) {
    Pipeline.Builder newPipeline = p.toBuilder();
    Components.Builder newPipelineComponents = newPipeline.getComponentsBuilder();

    QueryablePipeline q = QueryablePipeline.forPipeline(p);
    String feedSdfUrn = SplittableRemoteStageEvaluatorFactory.FEED_SDF_URN;
    List<PTransformNode> feedSDFNodes =
        q.getTransforms().stream()
            .filter(node -> node.getTransform().getSpec().getUrn().equals(feedSdfUrn))
            .collect(Collectors.toList());
    Map<String, PTransformNode> stageToFeeder = Maps.newHashMap();
    for (PTransformNode node : feedSDFNodes) {
      PCollectionNode output = Iterables.getOnlyElement(q.getOutputPCollections(node));
      PTransformNode consumer = Iterables.getOnlyElement(q.getPerElementConsumers(output));
      String consumerUrn = consumer.getTransform().getSpec().getUrn();
      checkState(
          consumerUrn.equals(ExecutableStage.URN),
          "Expected all FeedSDF nodes to be consumed by an ExecutableStage, "
              + "but %s is consumed by %s which is %s",
          node.getId(),
          consumer.getId(),
          consumerUrn);
      stageToFeeder.put(consumer.getId(), node);
    }

    // Copy over root transforms except for the excluded FEED_SDF transforms.
    Set<String> feedSDFIds =
        feedSDFNodes.stream().map(PTransformNode::getId).collect(Collectors.toSet());
    newPipeline.clearRootTransformIds();
    for (String rootId : p.getRootTransformIdsList()) {
      if (!feedSDFIds.contains(rootId)) {
        newPipeline.addRootTransformIds(rootId);
      }
    }
    // Copy over all transforms, except FEED_SDF transforms are skipped, and ExecutableStage's
    // feeding from them are replaced.
    for (PTransformNode node : q.getTransforms()) {
      if (feedSDFNodes.contains(node)) {
        // These transforms are skipped and handled separately.
        continue;
      }
      if (!stageToFeeder.containsKey(node.getId())) {
        // This transform is unchanged
        newPipelineComponents.putTransforms(node.getId(), node.getTransform());
        continue;
      }
      // "node" is an ExecutableStage transform feeding from an SDF.
      PTransformNode feedSDFNode = stageToFeeder.get(node.getId());
      PCollectionNode rawGBKOutput =
          Iterables.getOnlyElement(q.getPerElementInputPCollections(feedSDFNode));

      // Replace the ExecutableStage transform.
      newPipelineComponents.putTransforms(
          node.getId(),
          node.getTransform()
              .toBuilder()
              .mergeSpec(
                  // Change URN from ExecutableStage.URN to URN of the ULR's splittable executable
                  // stage evaluator.
                  FunctionSpec.newBuilder()
                      .setUrn(SplittableRemoteStageEvaluatorFactory.URN)
                      .build())
              .putInputs(
                  // The splittable executable stage now reads from the raw GBK, instead of
                  // from the now non-existent FEED_SDF.
                  Iterables.getOnlyElement(node.getTransform().getInputsMap().keySet()),
                  rawGBKOutput.getId())
              .build());
    }

    return newPipeline.build();
  }

  private enum EnvironmentType {
    DOCKER,
    IN_PROCESS
  }
}
