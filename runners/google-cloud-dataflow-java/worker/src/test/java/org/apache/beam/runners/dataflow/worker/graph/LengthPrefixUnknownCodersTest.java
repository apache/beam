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

import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.LENGTH_PREFIXED_BYTE_ARRAY_CODER;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.andReplaceForRunnerNetwork;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forCodec;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forInstructionOutput;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forInstructionOutputNode;
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forParallelInstruction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Sink;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link LengthPrefixUnknownCoders}. */
@RunWith(JUnit4.class)
public class LengthPrefixUnknownCodersTest {
  private static final Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
      WindowedValue.getFullCoder(
          KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<String, Integer>>> prefixedWindowedValueCoder =
      WindowedValue.getFullCoder(
          KvCoder.of(
              LengthPrefixCoder.of(StringUtf8Coder.of()), LengthPrefixCoder.of(VarIntCoder.of())),
          GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<byte[], byte[]>>>
      prefixedAndReplacedWindowedValueCoder =
          WindowedValue.getFullCoder(
              KvCoder.of(LENGTH_PREFIXED_BYTE_ARRAY_CODER, LENGTH_PREFIXED_BYTE_ARRAY_CODER),
              GlobalWindow.Coder.INSTANCE);

  private static final String MERGE_BUCKETS_DO_FN = "MergeBucketsDoFn";
  private ParallelInstruction instruction;
  private InstructionOutputNode instructionOutputNode;
  private MutableNetwork<Node, Edge> network;
  @Mock private RemoteGrpcPortNode grpcPortNode;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    instruction = new ParallelInstruction();
    instruction.setFactory(new JacksonFactory());
    instructionOutputNode = createInstructionOutputNode("parDo.out", windowedValueCoder);
  }

  /** Test wrapping unknown coders with {@code LengthPrefixCoder} */
  @Test
  public void testLengthPrefixUnknownCoders() throws Exception {
    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder), false);
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder), lengthPrefixedCoderCloudObject);
  }

  /** Test bypassing unknown coders that are already wrapped with {@code LengthPrefixCoder} */
  @Test
  public void testLengthPrefixForLengthPrefixCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), LengthPrefixCoder.of(VarIntCoder.of())),
            GlobalWindow.Coder.INSTANCE);

    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder), false);

    Coder<WindowedValue<KV<String, Integer>>> expectedCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(
                LengthPrefixCoder.of(StringUtf8Coder.of()), LengthPrefixCoder.of(VarIntCoder.of())),
            GlobalWindow.Coder.INSTANCE);

    assertEquals(CloudObjects.asCloudObject(expectedCoder), lengthPrefixedCoderCloudObject);
  }

  /** Test replacing unknown coders with {@code LengthPrefixCoder<ByteArray>} */
  @Test
  public void testLengthPrefixAndReplaceUnknownCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(LengthPrefixCoder.of(StringUtf8Coder.of()), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);

    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder), true);

    assertEquals(
        CloudObjects.asCloudObject(prefixedAndReplacedWindowedValueCoder),
        lengthPrefixedCoderCloudObject);
  }

  @Test
  public void testLengthPrefixInstructionOutputCoder() throws Exception {
    InstructionOutput output = new InstructionOutput();
    output.setCodec(CloudObjects.asCloudObject(windowedValueCoder));
    output.setFactory(new JacksonFactory());

    InstructionOutput prefixedOutput = forInstructionOutput(output, false);
    assertEquals(CloudObjects.asCloudObject(prefixedWindowedValueCoder), prefixedOutput.getCodec());
    // Should not mutate the instruction.
    assertEquals(output.getCodec(), CloudObjects.asCloudObject(windowedValueCoder));
  }

  @Test
  public void testLengthPrefixReadInstructionCoder() throws Exception {
    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(
        new Source().setCodec(CloudObjects.asCloudObject(windowedValueCoder)));
    instruction.setRead(readInstruction);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder),
        prefixedInstruction.getRead().getSource().getCodec());
    // Should not mutate the instruction.
    assertEquals(
        readInstruction.getSource().getCodec(), CloudObjects.asCloudObject(windowedValueCoder));
  }

  @Test
  public void testLengthPrefixWriteInstructionCoder() throws Exception {
    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setSink(new Sink().setCodec(CloudObjects.asCloudObject(windowedValueCoder)));
    instruction.setWrite(writeInstruction);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder),
        prefixedInstruction.getWrite().getSink().getCodec());
    // Should not mutate the instruction.
    assertEquals(
        CloudObjects.asCloudObject(windowedValueCoder), writeInstruction.getSink().getCodec());
  }

  @Test
  public void testLengthPrefixParDoInstructionCoder() throws Exception {
    ParDoInstruction parDo = new ParDoInstruction();
    CloudObject spec = CloudObject.forClassName(MERGE_BUCKETS_DO_FN);
    spec.put(WorkerPropertyNames.INPUT_CODER, CloudObjects.asCloudObject(windowedValueCoder));
    parDo.setUserFn(spec);
    instruction.setParDo(parDo);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder),
        prefixedInstruction.getParDo().getUserFn().get(WorkerPropertyNames.INPUT_CODER));
    // Should not mutate the instruction.
    assertEquals(
        CloudObjects.asCloudObject(windowedValueCoder),
        parDo.getUserFn().get(WorkerPropertyNames.INPUT_CODER));
  }

  @Test
  public void testClone() throws Exception {
    ParallelInstruction copy =
        LengthPrefixUnknownCoders.clone(instruction, ParallelInstruction.class);
    assertNotSame(instruction, copy);
    assertEquals(instruction, copy);
  }

  @Test
  public void testLengthPrefixAndReplaceForRunnerNetwork() {
    Node readNode = createReadNode("Read", "Source", windowedValueCoder);
    Edge readNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out", windowedValueCoder);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(readNode);
    network.addNode(readNodeOut);
    network.addEdge(readNode, readNodeOut, readNodeEdge);

    ParallelInstructionNode prefixedReadNode =
        createReadNode("Read", "Source", prefixedAndReplacedWindowedValueCoder);
    InstructionOutputNode prefixedReadNodeOut =
        createInstructionOutputNode("Read.out", prefixedAndReplacedWindowedValueCoder);

    MutableNetwork<Node, Edge> prefixedNetwork = andReplaceForRunnerNetwork(network);

    Set prefixedInstructions = new HashSet<>();
    for (Node node : prefixedNetwork.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        prefixedInstructions.add(((ParallelInstructionNode) node).getParallelInstruction());
      } else if (node instanceof InstructionOutputNode) {
        prefixedInstructions.add(((InstructionOutputNode) node).getInstructionOutput());
      }
    }

    Set expectedInstructions =
        ImmutableSet.of(
            prefixedReadNode.getParallelInstruction(), prefixedReadNodeOut.getInstructionOutput());

    assertEquals(expectedInstructions, prefixedInstructions);
  }

  @Test
  public void testLengthPrefixForInstructionOutputNodeWithGrpcNodeSuccessor() {
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(instructionOutputNode);
    network.addNode(grpcPortNode);
    network.addEdge(grpcPortNode, instructionOutputNode, DefaultEdge.create());
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder),
        ((InstructionOutputNode) forInstructionOutputNode(network).apply(instructionOutputNode))
            .getInstructionOutput()
            .getCodec());
  }

  @Test
  public void testLengthPrefixForInstructionOutputNodeWithGrpcNodePredecessor() {
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(instructionOutputNode);
    network.addNode(grpcPortNode);
    network.addEdge(instructionOutputNode, grpcPortNode, DefaultEdge.create());
    assertEquals(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder),
        ((InstructionOutputNode) forInstructionOutputNode(network).apply(instructionOutputNode))
            .getInstructionOutput()
            .getCodec());
  }

  @Test
  public void testLengthPrefixForInstructionOutputNodeWithNonGrpcNodeNeighbor() {
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    ParallelInstructionNode readNode = createReadNode("read", "source", windowedValueCoder);
    network.addNode(instructionOutputNode);
    network.addNode(readNode);
    network.addEdge(readNode, instructionOutputNode, DefaultEdge.create());
    assertEquals(
        CloudObjects.asCloudObject(windowedValueCoder),
        ((InstructionOutputNode) forInstructionOutputNode(network).apply(instructionOutputNode))
            .getInstructionOutput()
            .getCodec());
  }

  @Test
  public void testLengthPrefixForSideInputInfos() {
    List<SideInputInfo> prefixedSideInputInfos =
        LengthPrefixUnknownCoders.forSideInputInfos(
            ImmutableList.of(
                createSideInputInfosWithCoders(windowedValueCoder, prefixedWindowedValueCoder)),
            false);
    assertEquals(
        ImmutableList.of(
            createSideInputInfosWithCoders(prefixedWindowedValueCoder, prefixedWindowedValueCoder)),
        prefixedSideInputInfos);

    List<SideInputInfo> prefixedAndReplacedSideInputInfos =
        LengthPrefixUnknownCoders.forSideInputInfos(
            ImmutableList.of(
                createSideInputInfosWithCoders(windowedValueCoder, prefixedWindowedValueCoder)),
            true);
    assertEquals(
        ImmutableList.of(
            createSideInputInfosWithCoders(
                prefixedAndReplacedWindowedValueCoder, prefixedAndReplacedWindowedValueCoder)),
        prefixedAndReplacedSideInputInfos);
  }

  private static SideInputInfo createSideInputInfosWithCoders(Coder<?>... coders) {
    SideInputInfo sideInputInfo = new SideInputInfo().setSources(new ArrayList<>());
    sideInputInfo.setFactory(new JacksonFactory());
    for (Coder<?> coder : coders) {
      Source source = new Source().setCodec(CloudObjects.asCloudObject(coder));
      source.setFactory(new JacksonFactory());
      sideInputInfo.getSources().add(source);
    }
    return sideInputInfo;
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  private static ParallelInstructionNode createReadNode(
      String name, String readClassName, Coder<?> coder) {
    ParallelInstruction parallelInstruction =
        new ParallelInstruction()
            .setName(name)
            .setRead(
                new ReadInstruction()
                    .setSource(
                        new Source()
                            .setCodec(CloudObjects.asCloudObject(coder))
                            .setSpec(CloudObject.forClassName(readClassName))));

    parallelInstruction.setFactory(new JacksonFactory());
    return ParallelInstructionNode.create(parallelInstruction, Nodes.ExecutionLocation.UNKNOWN);
  }

  private static InstructionOutputNode createInstructionOutputNode(String name, Coder<?> coder) {
    InstructionOutput instructionOutput =
        new InstructionOutput().setName(name).setCodec(CloudObjects.asCloudObject(coder));
    instructionOutput.setFactory(new JacksonFactory());
    return InstructionOutputNode.create(instructionOutput);
  }
}
