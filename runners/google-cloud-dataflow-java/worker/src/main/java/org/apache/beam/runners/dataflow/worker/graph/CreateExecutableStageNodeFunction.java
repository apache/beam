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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.runners.dataflow.util.Structs.getString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.CombinePhase;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutableStageNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Converts a {@link Network} representation of {@link MapTask} destined for the SDK harness into a
 * {@link Node} containing an {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 */
public class CreateExecutableStageNodeFunction
    implements Function<MutableNetwork<Node, Edge>, Node> {
  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";

  private static final String DATA_OUTPUT_URN = "urn:org.apache.beam:sink:runner:0.1";
  private static final String JAVA_SOURCE_URN = "urn:org.apache.beam:source:java:0.1";

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
  private final @Nullable RunnerApi.Pipeline pipeline;

  public CreateExecutableStageNodeFunction(RunnerApi.Pipeline pipeline, IdGenerator idGenerator) {
    this.pipeline = pipeline;
    this.idGenerator = idGenerator;
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

    RunnerApi.Components.Builder componentsBuilder = RunnerApi.Components.newBuilder();
    componentsBuilder.mergeFrom(this.pipeline.getComponents());

    // We start off by replacing all edges within the graph with edges that have the named
    // outputs from the predecessor step. For ParallelInstruction Source nodes and RemoteGrpcPort
    // nodes this is a generated port id. All ParDoInstructions will have already

    // For intermediate PCollections we fabricate, we make a bogus WindowingStrategy
    // TODO: create a correct windowing strategy, including coders and environment
    // An SdkFunctionSpec is invalid without a working environment reference. We can revamp that
    // when we inline SdkFunctionSpec and FunctionSpec, both slated for inlining wherever they occur

    // Default to use the Java environment if pipeline doesn't have environment specified.
    if (pipeline.getComponents().getEnvironmentsMap().isEmpty()) {
      String envId = Environments.JAVA_SDK_HARNESS_ENVIRONMENT.getUrn() + idGenerator.getId();
      componentsBuilder.putEnvironments(envId, Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    }

    // Use default WindowingStrategy as the fake one.
    // TODO: should get real WindowingStategy from pipeline proto.
    String fakeWindowingStrategyId = "fakeWindowingStrategy" + idGenerator.getId();
    SdkComponents sdkComponents = SdkComponents.create(pipeline.getComponents());
    try {
      RunnerApi.MessageWithComponents fakeWindowingStrategyProto =
          WindowingStrategyTranslation.toMessageProto(
              WindowingStrategy.globalDefault(), sdkComponents);
      componentsBuilder.putWindowingStrategies(
          fakeWindowingStrategyId, fakeWindowingStrategyProto.getWindowingStrategy());
      componentsBuilder.putAllCoders(fakeWindowingStrategyProto.getComponents().getCodersMap());
      componentsBuilder.putAllEnvironments(
          fakeWindowingStrategyProto.getComponents().getEnvironmentsMap());
    } catch (IOException exc) {
      throw new RuntimeException("Could not convert default windowing stratey to proto", exc);
    }

    Map<Node, String> nodesToPCollections = new HashMap<>();
    ImmutableMap.Builder<String, NameContext> ptransformIdToNameContexts = ImmutableMap.builder();
    // A field of ExecutableStage which includes the PCollection goes to worker side.
    Set<PCollectionNode> executableStageOutputs = new HashSet<>();
    // A field of ExecutableStage which includes the PCollection goes to runner side.
    Set<PCollectionNode> executableStageInputs = new HashSet<>();

    for (InstructionOutputNode node :
        Iterables.filter(input.nodes(), InstructionOutputNode.class)) {
      InstructionOutput instructionOutput = node.getInstructionOutput();

      String coderId = "generatedCoder" + idGenerator.getId();
      try (ByteString.Output output = ByteString.newOutput()) {
        try {
          Coder<?> javaCoder =
              CloudObjects.coderFromCloudObject(CloudObject.fromSpec(instructionOutput.getCodec()));
          Coder<?> elementCoder = ((WindowedValueCoder<?>) javaCoder).getValueCoder();
          sdkComponents.registerCoder(elementCoder);
          RunnerApi.Coder coderProto = CoderTranslation.toProto(elementCoder, sdkComponents);
          componentsBuilder.putCoders(coderId, coderProto);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Unable to encode coder %s for output %s",
                  instructionOutput.getCodec(), instructionOutput),
              e);
        } catch (Exception e) {
          // Coder probably wasn't a java coder
          OBJECT_MAPPER.writeValue(output, instructionOutput.getCodec());
          componentsBuilder.putCoders(
              coderId,
              RunnerApi.Coder.newBuilder()
                  .setSpec(
                      RunnerApi.SdkFunctionSpec.newBuilder()
                          .setSpec(
                              RunnerApi.FunctionSpec.newBuilder()
                                  .setPayload(output.toByteString())))
                  .build());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to encode coder %s for output %s",
                instructionOutput.getCodec(), instructionOutput),
            e);
      }

      String pcollectionId = "generatedPcollection" + idGenerator.getId();
      RunnerApi.PCollection pCollection =
          RunnerApi.PCollection.newBuilder()
              .setCoderId(coderId)
              .setWindowingStrategyId(fakeWindowingStrategyId)
              .build();
      nodesToPCollections.put(node, pcollectionId);
      componentsBuilder.putPcollections(pcollectionId, pCollection);

      // Check whether this output collection has consumers from worker side when "use_executable_stage_bundle_execution"
      // is set
      if (input.successors(node).stream().anyMatch(RemoteGrpcPortNode.class::isInstance)) {
        executableStageOutputs.add(PipelineNode.pCollection(pcollectionId, pCollection));
      }
      if (input.predecessors(node).stream().anyMatch(RemoteGrpcPortNode.class::isInstance)) {
        executableStageInputs.add(PipelineNode.pCollection(pcollectionId, pCollection));
      }
    }

    componentsBuilder.putAllCoders(sdkComponents.toComponents().getCodersMap());
    Set<PTransformNode> executableStageTransforms = new HashSet<>();

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

        if (userFnClassName.equals("CombineValuesFn") || userFnClassName.equals("KeyedCombineFn")) {
          transformSpec = transformCombineValuesFnToFunctionSpec(userFnSpec);
        } else {
          String parDoPTransformId = getString(userFnSpec, PropertyNames.SERIALIZED_FN);

          RunnerApi.PTransform parDoPTransform =
              pipeline == null
                  ? null
                  : pipeline.getComponents().getTransformsOrDefault(parDoPTransformId, null);

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

            transformSpec
                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                .setPayload(parDoPayload.toByteString());
          } else {
            // legacy path - bytes are the SdkFunctionSpec's payload field, basically, and
            // SDKs expect it in the PTransform's payload field
            byte[] userFnBytes = getBytes(userFnSpec, PropertyNames.SERIALIZED_FN);
            transformSpec
                .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
                .setPayload(ByteString.copyFrom(userFnBytes));
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
      executableStageTransforms.add(PipelineNode.pTransform(ptransformId, pTransform.build()));
    }

    PCollectionNode executableInput = executableStageInputs.iterator().next();
    RunnerApi.Components executableStageComponents = componentsBuilder.build();

    // Get Environment from ptransform, otherwise, use JAVA_SDK_HARNESS_ENVIRONMENT as default.
    Environment executableStageEnv =
        getEnvironmentFromPTransform(executableStageComponents, executableStageTransforms);
    if (executableStageEnv == null) {
      executableStageEnv = Environments.JAVA_SDK_HARNESS_ENVIRONMENT;
    }

    Set<SideInputReference> executableStageSideInputs = new HashSet<>();
    Set<TimerReference> executableStageTimers = new HashSet<>();
    Set<UserStateReference> executableStageUserStateReference = new HashSet<>();
    ExecutableStage executableStage =
        ImmutableExecutableStage.ofFullComponents(
            executableStageComponents,
            executableStageEnv,
            executableInput,
            executableStageSideInputs,
            executableStageUserStateReference,
            executableStageTimers,
            executableStageTransforms,
            executableStageOutputs);
    return ExecutableStageNode.create(executableStage, ptransformIdToNameContexts.build());
  }

  private Environment getEnvironmentFromPTransform(
      RunnerApi.Components components, Set<PTransformNode> sdkTransforms) {
    RehydratedComponents sdkComponents = RehydratedComponents.forComponents(components);
    Environment env = null;
    for (PTransformNode pTransformNode : sdkTransforms) {
      env = Environments.getEnvironment(pTransformNode.getTransform(), sdkComponents).orElse(null);
      if (env != null) {
        break;
      }
    }

    return env;
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
}
