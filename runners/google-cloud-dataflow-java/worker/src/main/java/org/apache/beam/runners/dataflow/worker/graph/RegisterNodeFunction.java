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
package org.apache.beam.runners.dataflow.worker.graph;

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forSideInputInfos;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SyntheticComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.CombinePhase;
import org.apache.beam.runners.dataflow.worker.DataflowPortabilityPCollectionView;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RegisterRequestNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;

/**
 * Converts a {@link Network} representation of {@link MapTask} destined for the SDK harness into an
 * {@link Node} containing {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest}.
 *
 * <p>Testing of all the layers of translation are performed via local service runner tests.
 */
public class RegisterNodeFunction implements Function<MutableNetwork<Node, Edge>, Node> {
  /** Must match declared fields within {@code ProcessBundleHandler}. */
  private static final String DATA_INPUT_URN = "beam:source:runner:0.1";

  private static final String DATA_OUTPUT_URN = "beam:sink:runner:0.1";
  private static final String JAVA_SOURCE_URN = "beam:source:java:0.1";

  public static final String COMBINE_PER_KEY_URN =
      BeamUrns.getUrn(StandardPTransforms.Composites.COMBINE_PER_KEY);
  public static final String COMBINE_PRECOMBINE_URN =
      BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_PRECOMBINE);
  public static final String COMBINE_MERGE_URN =
      BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_MERGE_ACCUMULATORS);
  public static final String COMBINE_EXTRACT_URN =
      BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_EXTRACT_OUTPUTS);
  public static final String COMBINE_GROUPED_VALUES_URN =
      BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_GROUPED_VALUES);

  private static final String SERIALIZED_SOURCE = "serialized_source";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final IdGenerator idGenerator;
  private final Endpoints.ApiServiceDescriptor stateApiServiceDescriptor;
  private final @Nullable RunnerApi.Pipeline pipeline;

  /**
   * Returns a {@link RegisterNodeFunction} for a portable Pipeline. UDF-bearing transform payloads
   * will be looked up in the portable pipeline.
   */
  public static RegisterNodeFunction forPipeline(
      RunnerApi.Pipeline pipeline,
      IdGenerator idGenerator,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor) {
    return new RegisterNodeFunction(pipeline, idGenerator, stateApiServiceDescriptor);
  }

  /**
   * Returns a {@link RegisterNodeFunction} without a portable Pipeline. Not all SDKs provide a
   * portable pipeline yet. Each SDK can provide the pipeline and adjust their translations and
   * harnesses, then this method should be removed.
   */
  public static RegisterNodeFunction withoutPipeline(
      IdGenerator idGenerator, Endpoints.ApiServiceDescriptor stateApiServiceDescriptor) {
    return new RegisterNodeFunction(null, idGenerator, stateApiServiceDescriptor);
  }

  private RegisterNodeFunction(
      @Nullable RunnerApi.Pipeline pipeline,
      IdGenerator idGenerator,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor) {
    this.pipeline = pipeline;
    this.idGenerator = idGenerator;
    this.stateApiServiceDescriptor = stateApiServiceDescriptor;
  }

  @Override
  public Node apply(MutableNetwork<Node, Edge> input) {
    for (Node node : input.nodes()) {
      if (node instanceof RemoteGrpcPortNode
          || node instanceof ParallelInstructionNode
          || node instanceof InstructionOutputNode) {
        continue;
      }
      throw new IllegalArgumentException(
          String.format("Network contains unknown type of node: %s", input));
    }

    // Fix all non output nodes to have named edges.
    for (Node node : input.nodes()) {
      if (node instanceof InstructionOutputNode) {
        continue;
      }
      for (Node successor : input.successors(node)) {
        for (Edge edge : input.edgesConnecting(node, successor)) {
          if (edge instanceof DefaultEdge) {
            input.removeEdge(edge);
            input.addEdge(
                node,
                successor,
                MultiOutputInfoEdge.create(new MultiOutputInfo().setTag(idGenerator.getId())));
          }
        }
      }
    }

    // We start off by replacing all edges within the graph with edges that have the named
    // outputs from the predecessor step. For ParallelInstruction Source nodes and RemoteGrpcPort
    // nodes this is a generated port id. All ParDoInstructions will have already

    ProcessBundleDescriptor.Builder processBundleDescriptor =
        ProcessBundleDescriptor.newBuilder()
            .setId(idGenerator.getId())
            .setStateApiServiceDescriptor(stateApiServiceDescriptor);

    // For intermediate PCollections we fabricate, we make a bogus WindowingStrategy
    // TODO: create a correct windowing strategy, including coders and environment
    SdkComponents sdkComponents = SdkComponents.create(pipeline.getComponents());

    // Default to use the Java environment if pipeline doesn't have environment specified.
    if (pipeline.getComponents().getEnvironmentsMap().isEmpty()) {
      sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    }

    String fakeWindowingStrategyId = "fakeWindowingStrategy" + idGenerator.getId();
    try {
      RunnerApi.MessageWithComponents fakeWindowingStrategyProto =
          WindowingStrategyTranslation.toMessageProto(
              WindowingStrategy.globalDefault(), sdkComponents);
      processBundleDescriptor
          .putWindowingStrategies(
              fakeWindowingStrategyId, fakeWindowingStrategyProto.getWindowingStrategy())
          .putAllCoders(fakeWindowingStrategyProto.getComponents().getCodersMap())
          .putAllEnvironments(fakeWindowingStrategyProto.getComponents().getEnvironmentsMap());
    } catch (IOException exc) {
      throw new RuntimeException("Could not convert default windowing stratey to proto", exc);
    }

    Map<Node, String> nodesToPCollections = new HashMap<>();
    ImmutableMap.Builder<String, NameContext> ptransformIdToNameContexts = ImmutableMap.builder();
    ImmutableMap.Builder<String, Iterable<SideInputInfo>> ptransformIdToSideInputInfos =
        ImmutableMap.builder();
    ImmutableMap.Builder<String, Iterable<PCollectionView<?>>> ptransformIdToPCollectionViews =
        ImmutableMap.builder();
    ImmutableMap.Builder<String, NameContext> pcollectionIdToNameContexts = ImmutableMap.builder();

    // For each instruction output node:
    // 1. Generate new Coder and register it with SDKComponents and ProcessBundleDescriptor.
    // 2. Generate new PCollectionId and register it with ProcessBundleDescriptor.
    for (InstructionOutputNode node :
        Iterables.filter(input.nodes(), InstructionOutputNode.class)) {
      InstructionOutput instructionOutput = node.getInstructionOutput();

      String coderId = "generatedCoder" + idGenerator.getId();
      try (ByteString.Output output = ByteString.newOutput()) {
        try {
          Coder<?> javaCoder =
              CloudObjects.coderFromCloudObject(CloudObject.fromSpec(instructionOutput.getCodec()));
          sdkComponents.registerCoder(javaCoder);
          RunnerApi.Coder coderProto = CoderTranslation.toProto(javaCoder, sdkComponents);
          processBundleDescriptor.putCoders(coderId, coderProto);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Unable to encode coder %s for output %s",
                  instructionOutput.getCodec(), instructionOutput),
              e);
        } catch (Exception e) {
          // Coder probably wasn't a java coder
          OBJECT_MAPPER.writeValue(output, instructionOutput.getCodec());
          processBundleDescriptor.putCoders(
              coderId,
              RunnerApi.Coder.newBuilder()
                  .setSpec(RunnerApi.FunctionSpec.newBuilder().setPayload(output.toByteString()))
                  .build());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to encode coder %s for output %s",
                instructionOutput.getCodec(), instructionOutput),
            e);
      }

      // Generate new PCollection ID and map it to relevant node.
      // Will later be used to fill PTransform inputs/outputs information.
      String pcollectionId = "generatedPcollection" + idGenerator.getId();
      processBundleDescriptor.putPcollections(
          pcollectionId,
          RunnerApi.PCollection.newBuilder()
              .setCoderId(coderId)
              .setWindowingStrategyId(fakeWindowingStrategyId)
              .build());
      nodesToPCollections.put(node, pcollectionId);
      pcollectionIdToNameContexts.put(
          pcollectionId,
          NameContext.create(
              null,
              instructionOutput.getOriginalName(),
              instructionOutput.getSystemName(),
              instructionOutput.getName()));
    }
    processBundleDescriptor.putAllCoders(sdkComponents.toComponents().getCodersMap());

    for (ParallelInstructionNode node :
        Iterables.filter(input.nodes(), ParallelInstructionNode.class)) {
      ParallelInstruction parallelInstruction = node.getParallelInstruction();
      String ptransformId = "generatedPtransform" + idGenerator.getId();
      ptransformIdToNameContexts.put(
          ptransformId,
          NameContext.create(
              null,
              parallelInstruction.getOriginalName(),
              parallelInstruction.getSystemName(),
              parallelInstruction.getName()));

      RunnerApi.PTransform.Builder pTransform = RunnerApi.PTransform.newBuilder();
      RunnerApi.FunctionSpec.Builder transformSpec = RunnerApi.FunctionSpec.newBuilder();

      if (parallelInstruction.getParDo() != null) {
        ParDoInstruction parDoInstruction = parallelInstruction.getParDo();
        CloudObject userFnSpec = CloudObject.fromSpec(parDoInstruction.getUserFn());
        String userFnClassName = userFnSpec.getClassName();

        if ("CombineValuesFn".equals(userFnClassName) || "KeyedCombineFn".equals(userFnClassName)) {
          transformSpec = transformCombineValuesFnToFunctionSpec(userFnSpec);
          ptransformIdToPCollectionViews.put(ptransformId, Collections.emptyList());
        } else {
          String parDoPTransformId = getString(userFnSpec, PropertyNames.SERIALIZED_FN);

          RunnerApi.PTransform parDoPTransform =
              pipeline.getComponents().getTransformsOrDefault(parDoPTransformId, null);

          // TODO: only the non-null branch should exist; for migration ease only
          if (parDoPTransform != null) {
            checkArgument(
                parDoPTransform
                    .getSpec()
                    .getUrn()
                    .equals(PTransformTranslation.PAR_DO_TRANSFORM_URN),
                "Found transform \"%s\" for ParallelDo instruction, "
                    + " but that transform had unexpected URN \"%s\" (expected \"%s\")",
                parDoPTransformId,
                parDoPTransform.getSpec().getUrn(),
                PTransformTranslation.PAR_DO_TRANSFORM_URN);

            RunnerApi.ParDoPayload parDoPayload;
            try {
              parDoPayload =
                  RunnerApi.ParDoPayload.parseFrom(parDoPTransform.getSpec().getPayload());
            } catch (InvalidProtocolBufferException exc) {
              throw new RuntimeException("ParDo did not have a ParDoPayload", exc);
            }

            ImmutableList.Builder<PCollectionView<?>> pcollectionViews = ImmutableList.builder();
            for (Map.Entry<String, SideInput> sideInputEntry :
                parDoPayload.getSideInputsMap().entrySet()) {
              pcollectionViews.add(
                  transformSideInputForRunner(
                      pipeline,
                      parDoPTransform,
                      sideInputEntry.getKey(),
                      sideInputEntry.getValue()));
              transformSideInputForSdk(
                  pipeline,
                  parDoPTransform,
                  sideInputEntry.getKey(),
                  processBundleDescriptor,
                  pTransform);
            }
            ptransformIdToPCollectionViews.put(ptransformId, pcollectionViews.build());

            transformSpec
                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                .setPayload(parDoPayload.toByteString());
          } else {
            // legacy path - bytes are the FunctionSpec's payload field, basically, and
            // SDKs expect it in the PTransform's payload field
            byte[] userFnBytes = getBytes(userFnSpec, PropertyNames.SERIALIZED_FN);
            transformSpec
                .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
                .setPayload(ByteString.copyFrom(userFnBytes));
          }

          // Add side input information for batch pipelines
          if (parDoInstruction.getSideInputs() != null) {
            ptransformIdToSideInputInfos.put(
                ptransformId, forSideInputInfos(parDoInstruction.getSideInputs(), true));
          }
        }
      } else if (parallelInstruction.getRead() != null) {
        ReadInstruction readInstruction = parallelInstruction.getRead();
        CloudObject sourceSpec =
            CloudObject.fromSpec(
                CloudSourceUtils.flattenBaseSpecs(readInstruction.getSource()).getSpec());
        // TODO: Need to plumb through the SDK specific function spec.
        transformSpec.setUrn(JAVA_SOURCE_URN);
        try {
          byte[] serializedSource =
              Base64.getDecoder().decode(getString(sourceSpec, SERIALIZED_SOURCE));
          ByteString sourceByteString = ByteString.copyFrom(serializedSource);
          transformSpec.setPayload(sourceByteString);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Unable to process Read %s", parallelInstruction), e);
        }
      } else if (parallelInstruction.getFlatten() != null) {
        transformSpec.setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN);
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown type of ParallelInstruction %s", parallelInstruction));
      }

      for (Node predecessorOutput : input.predecessors(node)) {
        pTransform.putInputs(
            "generatedInput" + idGenerator.getId(), nodesToPCollections.get(predecessorOutput));
      }

      for (Edge edge : input.outEdges(node)) {
        Node nodeOutput = input.incidentNodes(edge).target();
        MultiOutputInfoEdge edge2 = (MultiOutputInfoEdge) edge;
        pTransform.putOutputs(
            edge2.getMultiOutputInfo().getTag(), nodesToPCollections.get(nodeOutput));
      }

      pTransform.setSpec(transformSpec);
      processBundleDescriptor.putTransforms(ptransformId, pTransform.build());
    }

    // Add the PTransforms representing the remote gRPC nodes
    for (RemoteGrpcPortNode node : Iterables.filter(input.nodes(), RemoteGrpcPortNode.class)) {

      RunnerApi.PTransform.Builder pTransform = RunnerApi.PTransform.newBuilder();

      Set<Node> predecessors = input.predecessors(node);
      Set<Node> successors = input.successors(node);
      if (predecessors.isEmpty() && !successors.isEmpty()) {
        pTransform.putOutputs(
            "generatedOutput" + idGenerator.getId(),
            nodesToPCollections.get(Iterables.getOnlyElement(successors)));
        pTransform.setSpec(
            RunnerApi.FunctionSpec.newBuilder()
                .setUrn(DATA_INPUT_URN)
                .setPayload(node.getRemoteGrpcPort().toByteString())
                .build());
      } else if (!predecessors.isEmpty() && successors.isEmpty()) {
        pTransform.putInputs(
            "generatedInput" + idGenerator.getId(),
            nodesToPCollections.get(Iterables.getOnlyElement(predecessors)));
        pTransform.setSpec(
            RunnerApi.FunctionSpec.newBuilder()
                .setUrn(DATA_OUTPUT_URN)
                .setPayload(node.getRemoteGrpcPort().toByteString())
                .build());
      } else {
        throw new IllegalStateException(
            "Expected either one input OR one output "
                + "InstructionOutputNode for this RemoteGrpcPortNode");
      }
      processBundleDescriptor.putTransforms(node.getPrimitiveTransformId(), pTransform.build());
    }

    return RegisterRequestNode.create(
        RegisterRequest.newBuilder().addProcessBundleDescriptor(processBundleDescriptor).build(),
        ptransformIdToNameContexts.build(),
        ptransformIdToSideInputInfos.build(),
        ptransformIdToPCollectionViews.build(),
        pcollectionIdToNameContexts.build());
  }

  /**
   * Transforms a CombineValuesFn {@link ParDoInstruction} to an Apache Beam {@link
   * RunnerApi.FunctionSpec}.
   */
  private RunnerApi.FunctionSpec.Builder transformCombineValuesFnToFunctionSpec(
      CloudObject userFn) {
    // Grab the Combine PTransform. This transform is the composite PTransform representing the
    // entire CombinePerKey, and it contains the CombinePayload we need.
    String combinePTransformId = getString(userFn, PropertyNames.SERIALIZED_FN);

    RunnerApi.PTransform combinePerKeyPTransform =
        pipeline.getComponents().getTransformsOrDefault(combinePTransformId, null);
    checkArgument(
        combinePerKeyPTransform != null,
        "Transform with id \"%s\" not found in pipeline.",
        combinePTransformId);

    checkArgument(
        combinePerKeyPTransform.getSpec().getUrn().equals(COMBINE_PER_KEY_URN),
        "Found transform \"%s\" for Combine instruction, "
            + "but that transform had unexpected URN \"%s\" (expected \"%s\")",
        combinePerKeyPTransform,
        combinePerKeyPTransform.getSpec().getUrn(),
        COMBINE_PER_KEY_URN);

    RunnerApi.CombinePayload combinePayload;
    try {
      combinePayload =
          RunnerApi.CombinePayload.parseFrom(combinePerKeyPTransform.getSpec().getPayload());
    } catch (InvalidProtocolBufferException exc) {
      throw new RuntimeException("Combine did not have a CombinePayload", exc);
    }

    String phase = getString(userFn, WorkerPropertyNames.PHASE, CombinePhase.ALL);
    String urn;

    switch (phase) {
      case CombinePhase.ALL:
        urn = COMBINE_GROUPED_VALUES_URN;
        break;
      case CombinePhase.ADD:
        urn = COMBINE_PRECOMBINE_URN;
        break;
      case CombinePhase.MERGE:
        urn = COMBINE_MERGE_URN;
        break;
      case CombinePhase.EXTRACT:
        urn = COMBINE_EXTRACT_URN;
        break;
      default:
        throw new RuntimeException("Encountered unknown Combine Phase: " + phase);
    }
    return RunnerApi.FunctionSpec.newBuilder()
        .setUrn(urn)
        .setPayload(combinePayload.toByteString());
  }

  /**
   * Returns an artificial PCollectionView that can be used to fulfill API requirements of a {@link
   * SideInputReader} when used inside the Dataflow runner harness.
   *
   * <p>Generates length prefixed coder variants suitable to be used within the Dataflow Runner
   * harness so that encoding and decoding values matches the length prefixing that occurred when
   * materializing the side input.
   */
  public static final PCollectionView<?> transformSideInputForRunner(
      RunnerApi.Pipeline pipeline,
      RunnerApi.PTransform parDoPTransform,
      String sideInputTag,
      RunnerApi.SideInput sideInput) {
    checkArgument(
        Materializations.MULTIMAP_MATERIALIZATION_URN.equals(sideInput.getAccessPattern().getUrn()),
        "This handler is only capable of dealing with %s materializations "
            + "but was asked to handle %s for PCollectionView with tag %s.",
        Materializations.MULTIMAP_MATERIALIZATION_URN,
        sideInput.getAccessPattern().getUrn(),
        sideInputTag);
    String sideInputPCollectionId = parDoPTransform.getInputsOrThrow(sideInputTag);
    RunnerApi.PCollection sideInputPCollection =
        pipeline.getComponents().getPcollectionsOrThrow(sideInputPCollectionId);
    try {
      FullWindowedValueCoder<KV<Object, Object>> runnerSideInputCoder =
          (FullWindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(
                  PipelineNode.pCollection(sideInputPCollectionId, sideInputPCollection),
                  pipeline.getComponents());

      return DataflowPortabilityPCollectionView.with(
          new TupleTag<>(sideInputTag), runnerSideInputCoder);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to translate proto to coder", e);
    }
  }

  /**
   * Modifies the process bundle descriptor and updates the PTransform that the SDK harness will see
   * with length prefixed coders used on the side input PCollection and windowing strategy.
   */
  private static final void transformSideInputForSdk(
      RunnerApi.Pipeline pipeline,
      RunnerApi.PTransform originalPTransform,
      String sideInputTag,
      ProcessBundleDescriptor.Builder processBundleDescriptor,
      RunnerApi.PTransform.Builder updatedPTransform) {

    RunnerApi.PCollection sideInputPCollection =
        pipeline
            .getComponents()
            .getPcollectionsOrThrow(originalPTransform.getInputsOrThrow(sideInputTag));
    RunnerApi.WindowingStrategy sideInputWindowingStrategy =
        pipeline
            .getComponents()
            .getWindowingStrategiesOrThrow(sideInputPCollection.getWindowingStrategyId());

    // TODO: We should not length prefix the window or key for the SDK side since the
    // key and window are already length delimited via protobuf itself. But we need to
    // maintain the length prefixing within the Runner harness to match the bytes that were
    // materialized to the side input sink.

    // We take the original pipeline coders and add any coders we have added when processing side
    // inputs before building new length prefixed variants.
    RunnerApi.Components.Builder componentsBuilder = pipeline.getComponents().toBuilder();
    componentsBuilder.putAllCoders(processBundleDescriptor.getCodersMap());

    String updatedSdkSideInputCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(
            sideInputPCollection.getCoderId(), componentsBuilder, false);
    String updatedSdkSideInputWindowCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(
            sideInputWindowingStrategy.getWindowCoderId(), componentsBuilder, false);

    processBundleDescriptor.putAllCoders(componentsBuilder.getCodersMap());
    String updatedSdkWindowingStrategyId =
        SyntheticComponents.uniqueId(
            sideInputPCollection.getWindowingStrategyId() + "-runner_generated",
            processBundleDescriptor.getWindowingStrategiesMap().keySet()::contains);
    processBundleDescriptor.putWindowingStrategies(
        updatedSdkWindowingStrategyId,
        sideInputWindowingStrategy
            .toBuilder()
            .setWindowCoderId(updatedSdkSideInputWindowCoderId)
            .build());
    RunnerApi.PCollection updatedSdkSideInputPcollection =
        sideInputPCollection
            .toBuilder()
            .setCoderId(updatedSdkSideInputCoderId)
            .setWindowingStrategyId(updatedSdkWindowingStrategyId)
            .build();

    // Replace the contents of the PCollection with the updated side input PCollection
    // specification and insert it into the update PTransform.
    processBundleDescriptor.putPcollections(
        originalPTransform.getInputsOrThrow(sideInputTag), updatedSdkSideInputPcollection);
    updatedPTransform.putInputs(sideInputTag, originalPTransform.getInputsOrThrow(sideInputTag));
  }
}
