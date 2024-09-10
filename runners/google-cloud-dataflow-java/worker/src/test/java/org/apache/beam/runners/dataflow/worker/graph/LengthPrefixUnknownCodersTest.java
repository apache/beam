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
import static org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders.forParallelInstruction;
import static org.apache.beam.runners.dataflow.worker.testing.GenericJsonAssert.assertEqualsAsJson;
import static org.apache.beam.runners.dataflow.worker.testing.GenericJsonMatcher.jsonOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Sink;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.NetworkBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/** Tests for {@link LengthPrefixUnknownCoders}. */
@RunWith(JUnit4.class)
public class LengthPrefixUnknownCodersTest {
  private static final Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
      WindowedValue.getFullCoder(
          KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<String, Integer>>> prefixedWindowedValueCoder =
      WindowedValue.getFullCoder(
          KvCoder.of(StringUtf8Coder.of(), LengthPrefixCoder.of(VarIntCoder.of())),
          GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<String, byte[]>>>
      prefixedAndReplacedWindowedValueCoder =
          WindowedValue.getFullCoder(
              KvCoder.of(StringUtf8Coder.of(), LENGTH_PREFIXED_BYTE_ARRAY_CODER),
              GlobalWindow.Coder.INSTANCE);

  private static final String MERGE_BUCKETS_DO_FN = "MergeBucketsDoFn";
  private ParallelInstruction instruction;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    instruction = new ParallelInstruction();
    instruction.setFactory(new GsonFactory());
  }

  /** Test wrapping unknown coders with {@code LengthPrefixCoder} */
  @Test
  public void testLengthPrefixUnknownCoders() throws Exception {
    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null), false);
    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder, /*sdkComponents=*/ null),
        lengthPrefixedCoderCloudObject);
  }

  /** Test bypassing unknown coders that are already wrapped with {@code LengthPrefixCoder} */
  @Test
  public void testLengthPrefixForLengthPrefixCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), LengthPrefixCoder.of(VarIntCoder.of())),
            GlobalWindow.Coder.INSTANCE);

    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null), false);

    Coder<WindowedValue<KV<String, Integer>>> expectedCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), LengthPrefixCoder.of(VarIntCoder.of())),
            GlobalWindow.Coder.INSTANCE);

    assertEqualsAsJson(
        CloudObjects.asCloudObject(expectedCoder, /*sdkComponents=*/ null),
        lengthPrefixedCoderCloudObject);
  }

  /** Test replacing unknown coders with {@code LengthPrefixCoder<ByteArray>} */
  @Test
  public void testLengthPrefixAndReplaceUnknownCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), GlobalWindow.Coder.INSTANCE);

    Map<String, Object> lengthPrefixedCoderCloudObject =
        forCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null), true);

    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedAndReplacedWindowedValueCoder, /*sdkComponents=*/ null),
        lengthPrefixedCoderCloudObject);
  }

  @Test
  public void testLengthPrefixInstructionOutputCoder() throws Exception {
    InstructionOutput output = new InstructionOutput();
    output.setCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null));
    output.setFactory(new GsonFactory());

    InstructionOutput prefixedOutput = forInstructionOutput(output, false);
    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder, /*sdkComponents=*/ null),
        prefixedOutput.getCodec());
    // Should not mutate the instruction.
    assertEqualsAsJson(
        output.getCodec(), CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null));
  }

  @Test
  public void testLengthPrefixReadInstructionCoder() throws Exception {
    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(
        new Source()
            .setCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null)));
    instruction.setRead(readInstruction);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder, /*sdkComponents=*/ null),
        prefixedInstruction.getRead().getSource().getCodec());
    // Should not mutate the instruction.
    assertEqualsAsJson(
        readInstruction.getSource().getCodec(),
        CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null));
  }

  @Test
  public void testLengthPrefixWriteInstructionCoder() throws Exception {
    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setSink(
        new Sink()
            .setCodec(CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null)));
    instruction.setWrite(writeInstruction);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder, /*sdkComponents=*/ null),
        prefixedInstruction.getWrite().getSink().getCodec());
    // Should not mutate the instruction.
    assertEqualsAsJson(
        CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null),
        writeInstruction.getSink().getCodec());
  }

  @Test
  public void testLengthPrefixParDoInstructionCoder() throws Exception {
    ParDoInstruction parDo = new ParDoInstruction();
    CloudObject spec = CloudObject.forClassName(MERGE_BUCKETS_DO_FN);
    spec.put(
        WorkerPropertyNames.INPUT_CODER,
        CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null));
    parDo.setUserFn(spec);
    instruction.setParDo(parDo);

    ParallelInstruction prefixedInstruction = forParallelInstruction(instruction, false);
    assertEqualsAsJson(
        CloudObjects.asCloudObject(prefixedWindowedValueCoder, /*sdkComponents=*/ null),
        prefixedInstruction.getParDo().getUserFn().get(WorkerPropertyNames.INPUT_CODER));
    // Should not mutate the instruction.
    assertEqualsAsJson(
        CloudObjects.asCloudObject(windowedValueCoder, /*sdkComponents=*/ null),
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
  public void testLengthPrefixAndReplaceForRunnerNetwork() throws Exception {
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

    ImmutableSet.Builder<GenericJson> prefixedInstructions = ImmutableSet.builder();
    for (Node node : prefixedNetwork.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        prefixedInstructions.add(((ParallelInstructionNode) node).getParallelInstruction());
      } else if (node instanceof InstructionOutputNode) {
        prefixedInstructions.add(((InstructionOutputNode) node).getInstructionOutput());
      }
    }

    assertThat(
        prefixedInstructions.build(),
        containsInAnyOrder(
            jsonOf(prefixedReadNodeOut.getInstructionOutput()),
            jsonOf(prefixedReadNode.getParallelInstruction())));
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed().allowsSelfLoops(false).allowsParallelEdges(true).build();
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
                            .setCodec(CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null))
                            .setSpec(CloudObject.forClassName(readClassName))));

    parallelInstruction.setFactory(new GsonFactory());
    return ParallelInstructionNode.create(parallelInstruction, Nodes.ExecutionLocation.UNKNOWN);
  }

  private static InstructionOutputNode createInstructionOutputNode(String name, Coder<?> coder) {
    InstructionOutput instructionOutput =
        new InstructionOutput()
            .setName(name)
            .setCodec(CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null));
    instructionOutput.setFactory(new GsonFactory());
    return InstructionOutputNode.create(instructionOutput, "fakeId");
  }
}
