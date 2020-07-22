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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.FetchAndFilterStreamingSideInputsNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence.Wrapper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.ImmutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InsertFetchAndFilterStreamingSideInputNodes}. */
@RunWith(JUnit4.class)
public class InsertFetchAndFilterStreamingSideInputNodesTest {

  @Test
  public void testWithoutPipeline() throws Exception {
    Node unknown = createParDoNode("parDoId");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(unknown);

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = InsertFetchAndFilterStreamingSideInputNodes.with(null).forNetwork(network);

    assertThatNetworksAreIdentical(inputNetwork, network);
  }

  @Test
  public void testSdkParDoWithSideInput() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> pc = p.apply(Create.of("a", "b", "c"));
    PCollectionView<List<String>> pcView = pc.apply(View.asList());
    pc.apply(ParDo.of(new TestDoFn(pcView)).withSideInputs(pcView));
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);

    Node predecessor = createParDoNode("predecessor");
    InstructionOutputNode mainInput =
        InstructionOutputNode.create(new InstructionOutput(), "fakeId");
    Node sideInputParDo = createParDoNode(findParDoWithSideInput(pipeline));

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(predecessor);
    network.addNode(mainInput);
    network.addNode(sideInputParDo);
    network.addEdge(predecessor, mainInput, DefaultEdge.create());
    network.addEdge(mainInput, sideInputParDo, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = InsertFetchAndFilterStreamingSideInputNodes.with(pipeline).forNetwork(network);

    Node mainInputClone = InstructionOutputNode.create(mainInput.getInstructionOutput(), "fakeId");
    Node fetchAndFilter =
        FetchAndFilterStreamingSideInputsNode.create(
            pcView.getWindowingStrategyInternal(),
            ImmutableMap.of(
                pcView,
                ParDoTranslation.translateWindowMappingFn(
                    pcView.getWindowMappingFn(),
                    SdkComponents.create(PipelineOptionsFactory.create()))),
            NameContextsForTests.nameContextForTest());

    MutableNetwork<Node, Edge> expectedNetwork = createEmptyNetwork();
    expectedNetwork.addNode(predecessor);
    expectedNetwork.addNode(mainInputClone);
    expectedNetwork.addNode(fetchAndFilter);
    expectedNetwork.addNode(mainInput);
    expectedNetwork.addNode(sideInputParDo);
    expectedNetwork.addEdge(predecessor, mainInputClone, DefaultEdge.create());
    expectedNetwork.addEdge(mainInputClone, fetchAndFilter, DefaultEdge.create());
    expectedNetwork.addEdge(fetchAndFilter, mainInput, DefaultEdge.create());
    expectedNetwork.addEdge(mainInput, sideInputParDo, DefaultEdge.create());

    assertThatNetworksAreIdentical(expectedNetwork, network);
  }

  @Test
  public void testSdkParDoWithoutSideInput() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> pc = p.apply(Create.of("a", "b", "c"));
    pc.apply(ParDo.of(new TestDoFn(null)));
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);

    Node predecessor = createParDoNode("predecessor");
    Node mainInput = InstructionOutputNode.create(new InstructionOutput(), "fakeId");
    Node sideInputParDo = createParDoNode("noSideInput");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(predecessor);
    network.addNode(mainInput);
    network.addNode(sideInputParDo);
    network.addEdge(predecessor, mainInput, DefaultEdge.create());
    network.addEdge(mainInput, sideInputParDo, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = InsertFetchAndFilterStreamingSideInputNodes.with(pipeline).forNetwork(network);

    assertThatNetworksAreIdentical(inputNetwork, network);
  }

  private String findParDoWithSideInput(RunnerApi.Pipeline pipeline) {
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        pipeline.getComponents().getTransformsMap().entrySet()) {
      if (!PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(entry.getValue().getSpec().getUrn())) {
        continue;
      }
      try {
        ParDoPayload payload = ParDoPayload.parseFrom(entry.getValue().getSpec().getPayload());
        if (!payload.getSideInputsMap().isEmpty()) {
          return entry.getKey();
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(String.format("Failed to parse PTransform %s", entry));
      }
    }
    throw new IllegalStateException("No side input ptransform found");
  }

  private static class TestDoFn extends DoFn<String, Iterable<String>> {
    private final @Nullable PCollectionView<List<String>> pCollectionView;

    private TestDoFn(@Nullable PCollectionView<List<String>> pCollectionView) {
      this.pCollectionView = pCollectionView;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {}
  }

  private static final class NodeEquivalence extends Equivalence<Node> {
    static final NodeEquivalence INSTANCE = new NodeEquivalence();

    @Override
    protected boolean doEquivalent(Node a, Node b) {
      if (a instanceof FetchAndFilterStreamingSideInputsNode
          && b instanceof FetchAndFilterStreamingSideInputsNode) {
        FetchAndFilterStreamingSideInputsNode nodeA = (FetchAndFilterStreamingSideInputsNode) a;
        FetchAndFilterStreamingSideInputsNode nodeB = (FetchAndFilterStreamingSideInputsNode) b;
        Map.Entry<PCollectionView<?>, FunctionSpec> nodeAEntry =
            Iterables.getOnlyElement(nodeA.getPCollectionViewsToWindowMappingFns().entrySet());
        Map.Entry<PCollectionView<?>, FunctionSpec> nodeBEntry =
            Iterables.getOnlyElement(nodeB.getPCollectionViewsToWindowMappingFns().entrySet());
        return Objects.equals(
                nodeAEntry.getKey().getTagInternal(), nodeBEntry.getKey().getTagInternal())
            && Objects.equals(nodeAEntry.getValue(), nodeBEntry.getValue());
      } else if (a instanceof InstructionOutputNode && b instanceof InstructionOutputNode) {
        return Objects.equals(
            ((InstructionOutputNode) a).getInstructionOutput(),
            ((InstructionOutputNode) b).getInstructionOutput());
      } else {
        return a.equals(b); // Make sure that other nodes haven't been modified
      }
    }

    @Override
    protected int doHash(Node n) {
      return n.hashCode();
    }
  }

  /**
   * Asserts that the structure and nodes of two graphs are identical except for the deduced
   * ExecutionLocations, and that all paths through the graph still exist.
   */
  private void assertThatNetworksAreIdentical(
      Network<Node, Edge> oldNetwork, Network<Node, Edge> newNetwork) {
    // Assert that both networks still have same number of nodes and edges.
    assertEquals(oldNetwork.nodes().size(), newNetwork.nodes().size());
    assertEquals(oldNetwork.edges().size(), newNetwork.edges().size());

    // Assert that all paths still exist with identical nodes in each path.
    List<List<Wrapper<Node>>> oldPaths = allPathsWithWrappedNodes(oldNetwork);
    List<List<Equivalence.Wrapper<Node>>> newPaths = allPathsWithWrappedNodes(newNetwork);
    assertThat(oldPaths, containsInAnyOrder(newPaths.toArray()));
  }

  private List<List<Equivalence.Wrapper<Node>>> allPathsWithWrappedNodes(
      Network<Node, Edge> network) {
    List<List<Node>> paths = Networks.allPathsFromRootsToLeaves(network);
    List<List<Equivalence.Wrapper<Node>>> wrappedPaths = new ArrayList<>();
    for (List<Node> path : paths) {
      List<Equivalence.Wrapper<Node>> wrappedPath = new ArrayList<>();
      for (Node node : path) {
        wrappedPath.add(NodeEquivalence.INSTANCE.wrap(node));
      }
      wrappedPaths.add(wrappedPath);
    }

    return wrappedPaths;
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  private static ParallelInstructionNode createParDoNode(String parDoId) {
    CloudObject userFn = CloudObject.forClassName("DoFn");
    userFn.put(PropertyNames.SERIALIZED_FN, parDoId);
    return ParallelInstructionNode.create(
        new ParallelInstruction().setParDo(new ParDoInstruction().setUserFn(userFn)),
        ExecutionLocation.SDK_HARNESS);
  }
}
