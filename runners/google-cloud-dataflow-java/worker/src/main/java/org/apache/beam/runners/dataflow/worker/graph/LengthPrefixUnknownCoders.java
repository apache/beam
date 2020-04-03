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

import com.google.api.client.json.GenericJson;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Source;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Networks.TypeSafeNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/** Utility for replacing or wrapping unknown coders with {@link LengthPrefixCoder}. */
public class LengthPrefixUnknownCoders {
  private static final ImmutableSet<String> WELL_KNOWN_CODER_TYPES =
      ImmutableSet.of(
          "kind:global_window",
          "kind:interval_window",
          "kind:length_prefix",
          "kind:windowed_value",
          "kind:bytes",
          // Equivalent to beam:coder:varint:v1 (64 bit) which is different then
          // kind:var_int32 (32 bit)
          "kind:varint",
          // TODO: Remove TimerOrElementCoder as it is not truly a well known type.
          // This is to get WindowingWindmillReader to work correctly. It is not expected
          // that the SDK harness should ever see this type.
          "com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder",
          // DFE supplied kinds
          "kind:pair",
          "kind:stream",
          "kind:ism_record",
          "kind:fixed_big_endian_int32",
          "kind:fixed_big_endian_int64",
          "kind:var_int32",
          "kind:void",
          "org.apache.beam.sdk.coders.DoubleCoder",
          "org.apache.beam.sdk.coders.StringUtf8Coder");

  private static final String LENGTH_PREFIX_CODER_TYPE = "kind:length_prefix";

  @VisibleForTesting
  static final ImmutableSet<String> MERGE_WINDOWS_DO_FN =
      ImmutableSet.of("MergeBucketsDoFn", "MergeWindowsDoFn");

  @VisibleForTesting
  static final LengthPrefixCoder<byte[]> LENGTH_PREFIXED_BYTE_ARRAY_CODER =
      LengthPrefixCoder.of(ByteArrayCoder.of());

  /**
   * Replaces unknown coders on every {@link InstructionOutputNode} and {@link
   * ParallelInstructionNode} for the runner portion of a {@link MutableNetwork}, with a {@link
   * org.apache.beam.sdk.coders.LengthPrefixCoder LengthPrefixCoder&lt;T&gt;}, where {@code T} is a
   * {@link org.apache.beam.sdk.coders.ByteArrayCoder}.
   */
  public static MutableNetwork<Node, Edge> andReplaceForRunnerNetwork(
      MutableNetwork<Node, Edge> network) {
    Networks.replaceDirectedNetworkNodes(network, andReplaceForInstructionOutputNode());
    Networks.replaceDirectedNetworkNodes(network, andReplaceForParallelInstructionNode());
    return network;
  }

  /**
   * Wraps unknown coders on every {@link InstructionOutputNode} for the SDK portion of a {@link
   * MutableNetwork} that is a predecessor or successor of a {@link RemoteGrpcPortNode}, with a
   * {@link org.apache.beam.sdk.coders.LengthPrefixCoder}.
   */
  public static MutableNetwork<Node, Edge> forSdkNetwork(MutableNetwork<Node, Edge> network) {
    Networks.replaceDirectedNetworkNodes(network, forInstructionOutputNode(network));
    return network;
  }

  /**
   * Wraps unknown coders on every {@link SideInputInfo} with length prefixes and also replaces the
   * wrapped coder with a byte array coder if requested.
   */
  public static List<SideInputInfo> forSideInputInfos(
      List<SideInputInfo> sideInputInfos, boolean replaceWithByteArrayCoder) {
    ImmutableList.Builder<SideInputInfo> updatedSideInputInfos = ImmutableList.builder();
    for (SideInputInfo sideInputInfo : sideInputInfos) {
      try {
        SideInputInfo updatedSideInputInfo = clone(sideInputInfo, SideInputInfo.class);
        for (Source source : updatedSideInputInfo.getSources()) {
          source.setCodec(forCodec(source.getCodec(), replaceWithByteArrayCoder));
        }
        updatedSideInputInfos.add(updatedSideInputInfo);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Failed to replace unknown coder with " + "LengthPrefixCoder for : {%s}",
                sideInputInfo),
            e);
      }
    }
    return updatedSideInputInfos.build();
  }

  /**
   * Wraps unknown coders on the given {@link InstructionOutputNode} with a {@link
   * org.apache.beam.sdk.coders.LengthPrefixCoder}.
   */
  @VisibleForTesting
  static Function<Node, Node> forInstructionOutputNode(MutableNetwork<Node, Edge> network) {
    return new TypeSafeNodeFunction<InstructionOutputNode>(InstructionOutputNode.class) {
      @Override
      public InstructionOutputNode typedApply(InstructionOutputNode input) {
        InstructionOutput cloudOutput = input.getInstructionOutput();
        if (network.predecessors(input).stream().anyMatch(RemoteGrpcPortNode.class::isInstance)
            || network.successors(input).stream().anyMatch(RemoteGrpcPortNode.class::isInstance)) {
          try {
            cloudOutput = forInstructionOutput(cloudOutput, false /* only wrap unknown coders */);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to replace unknown coder with " + "LengthPrefixCoder for : {%s}",
                    input.getInstructionOutput()),
                e);
          }
        }
        return InstructionOutputNode.create(cloudOutput, input.getPcollectionId());
      }
    };
  }

  /**
   * Replace unknown coders on the given {@link InstructionOutputNode} with {@link
   * org.apache.beam.sdk.coders.LengthPrefixCoder LengthPrefixCoder&lt;T&gt;} where {@code T} is a
   * {@link org.apache.beam.sdk.coders.ByteArrayCoder}.
   */
  private static Function<Node, Node> andReplaceForInstructionOutputNode() {
    return new TypeSafeNodeFunction<InstructionOutputNode>(InstructionOutputNode.class) {
      @Override
      public Node typedApply(InstructionOutputNode input) {
        InstructionOutput instructionOutput = input.getInstructionOutput();
        try {
          instructionOutput =
              forInstructionOutput(
                  instructionOutput, true /* replace unknown coder with byte array coder */);
        } catch (Exception e) {
          throw new RuntimeException(
              String.format(
                  "Failed to replace unknown coder with " + "LengthPrefixCoder for : {%s}",
                  input.getInstructionOutput()),
              e);
        }
        return InstructionOutputNode.create(instructionOutput, input.getPcollectionId());
      }
    };
  }

  /**
   * Replace unknown coders on the given {@link ParallelInstructionNode} with {@link
   * org.apache.beam.sdk.coders.LengthPrefixCoder LengthPrefixCoder&lt;T&gt;} where {@code T} is a
   * {@link org.apache.beam.sdk.coders.ByteArrayCoder}.
   */
  private static Function<Node, Node> andReplaceForParallelInstructionNode() {
    return new TypeSafeNodeFunction<ParallelInstructionNode>(ParallelInstructionNode.class) {
      @Override
      public Node typedApply(ParallelInstructionNode input) {
        ParallelInstruction instruction = input.getParallelInstruction();
        Nodes.ExecutionLocation location = input.getExecutionLocation();
        try {
          instruction =
              forParallelInstruction(
                  instruction, true /* replace unknown coder with byte array coder */);
        } catch (Exception e) {
          throw new RuntimeException(
              String.format(
                  "Failed to replace unknown coder with " + "LengthPrefixCoder for : {%s}",
                  input.getParallelInstruction()),
              e);
        }
        return ParallelInstructionNode.create(instruction, location);
      }
    };
  }

  /**
   * Recursively traverse the coder tree and wrap the first unknown coder in every branch with a
   * {@link LengthPrefixCoder} unless an ancestor coder is itself a {@link LengthPrefixCoder}. If
   * {@code replaceWithByteArrayCoder} is set, then replace that unknown coder with a {@link
   * ByteArrayCoder}.
   *
   * @param codec cloud representation of the {@link org.apache.beam.sdk.coders.Coder}.
   * @param replaceWithByteArrayCoder whether to replace an unknown coder with a {@link
   *     ByteArrayCoder}.
   */
  @VisibleForTesting
  static Map<String, Object> forCodec(
      Map<String, Object> codec, boolean replaceWithByteArrayCoder) {
    String coderType = (String) codec.get(PropertyNames.OBJECT_TYPE_NAME);

    // Handle well known coders.
    if (LENGTH_PREFIX_CODER_TYPE.equals(coderType)) {
      if (replaceWithByteArrayCoder) {
        return CloudObjects.asCloudObject(
            LENGTH_PREFIXED_BYTE_ARRAY_CODER, /*sdkComponents=*/ null);
      }
      return codec;
    } else if (WELL_KNOWN_CODER_TYPES.contains(coderType)) {
      // The runner knows about these types and can instantiate them so handle their
      // component encodings.
      Map<String, Object> prefixedCodec = new HashMap<>(codec);

      // Recursively replace component encodings
      if (codec.containsKey(PropertyNames.COMPONENT_ENCODINGS)) {
        List<Map<String, Object>> prefixedComponents = new ArrayList<>();
        for (Map<String, Object> component :
            (Iterable<Map<String, Object>>) codec.get(PropertyNames.COMPONENT_ENCODINGS)) {
          prefixedComponents.add(forCodec(component, replaceWithByteArrayCoder));
        }
        prefixedCodec.put(PropertyNames.COMPONENT_ENCODINGS, prefixedComponents);
      }
      return prefixedCodec;
    }

    // Wrap unknown coders with length prefix coder.
    if (replaceWithByteArrayCoder) {
      return CloudObjects.asCloudObject(LENGTH_PREFIXED_BYTE_ARRAY_CODER, /*sdkComponents=*/ null);
    } else {
      Map<String, Object> prefixedCodec = new HashMap<>();
      prefixedCodec.put(PropertyNames.OBJECT_TYPE_NAME, LENGTH_PREFIX_CODER_TYPE);
      prefixedCodec.put(PropertyNames.COMPONENT_ENCODINGS, ImmutableList.of(codec));
      return prefixedCodec;
    }
  }

  /**
   * Wrap unknown coders with a {@link LengthPrefixCoder} for the given {@link InstructionOutput}.
   */
  @VisibleForTesting
  static InstructionOutput forInstructionOutput(
      InstructionOutput input, boolean replaceWithByteArrayCoder) throws Exception {
    InstructionOutput cloudOutput = clone(input, InstructionOutput.class);
    cloudOutput.setCodec(forCodec(cloudOutput.getCodec(), replaceWithByteArrayCoder));
    return cloudOutput;
  }

  /**
   * Wrap unknown coders with a {@link LengthPrefixCoder} for the given {@link ParallelInstruction}.
   */
  @VisibleForTesting
  static ParallelInstruction forParallelInstruction(
      ParallelInstruction input, boolean replaceWithByteArrayCoder) throws Exception {
    try {
      ParallelInstruction instruction = clone(input, ParallelInstruction.class);
      if (instruction.getRead() != null) {
        Source cloudSource = instruction.getRead().getSource();
        cloudSource.setCodec(forCodec(cloudSource.getCodec(), replaceWithByteArrayCoder));
      } else if (instruction.getWrite() != null) {
        com.google.api.services.dataflow.model.Sink cloudSink = instruction.getWrite().getSink();
        cloudSink.setCodec(forCodec(cloudSink.getCodec(), replaceWithByteArrayCoder));
      } else if (instruction.getParDo() != null) {
        instruction.setParDo(
            forParDoInstruction(instruction.getParDo(), replaceWithByteArrayCoder));
      } else if (instruction.getPartialGroupByKey() != null) {
        PartialGroupByKeyInstruction pgbk = instruction.getPartialGroupByKey();
        pgbk.setInputElementCodec(forCodec(pgbk.getInputElementCodec(), replaceWithByteArrayCoder));
      } else if (instruction.getFlatten() != null) {
        // FlattenInstructions have no codecs to wrap.
      } else {
        throw new RuntimeException("Unknown parallel instruction: " + input);
      }
      return instruction;
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to replace unknown coder with " + "LengthPrefixCoder for : {%s}", input),
          e);
    }
  }

  /**
   * Wrap unknown coders with a {@link LengthPrefixCoder} for the given {@link ParDoInstruction}.
   */
  private static ParDoInstruction forParDoInstruction(
      ParDoInstruction input, boolean replaceWithByteArrayCoder) throws Exception {
    ParDoInstruction parDo = input;
    CloudObject userFn = CloudObject.fromSpec(parDo.getUserFn());
    String parDoFnType = userFn.getClassName();
    if (MERGE_WINDOWS_DO_FN.contains(parDoFnType)) {
      parDo = clone(input, ParDoInstruction.class);
      Map<String, Object> inputCoderObject =
          Structs.getObject(parDo.getUserFn(), WorkerPropertyNames.INPUT_CODER);
      parDo
          .getUserFn()
          .put(
              WorkerPropertyNames.INPUT_CODER,
              forCodec(inputCoderObject, replaceWithByteArrayCoder));
    } else if ("CreateIsmShardKeyAndSortKeyDoFn".equals(parDoFnType)
        || "ToIsmRecordForMultimapDoFn".equals(parDoFnType)
        || "StreamingPCollectionViewWriterDoFn".equals(parDoFnType)) {
      parDo = clone(input, ParDoInstruction.class);
      Map<String, Object> inputCoderObject =
          Structs.getObject(parDo.getUserFn(), PropertyNames.ENCODING);
      parDo
          .getUserFn()
          .put(PropertyNames.ENCODING, forCodec(inputCoderObject, replaceWithByteArrayCoder));
    }
    // TODO: Handle other types of ParDos.
    return parDo;
  }

  /** Clone a cloud object by converting it to json string and back. */
  @VisibleForTesting
  static <T extends GenericJson> T clone(T cloudObject, Class<T> type) throws IOException {
    return cloudObject.getFactory().fromString(cloudObject.toString(), type);
  }
}
