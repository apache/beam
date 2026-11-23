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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Networks.TypeSafeNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OperationNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OutputReceiverNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.FlattenOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.RawWindowedByteSizeObservable;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.util.common.worker.StageTrampoline;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WriteOperation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues.FullWindowedValueCoder;
import org.apache.beam.sdk.values.WindowedValues.ParamWindowedValueCoder;
import org.apache.beam.sdk.values.WindowedValues.WindowedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a {@link DataflowMapTaskExecutor} from a {@link MapTask} definition. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class IntrinsicMapTaskExecutorFactory implements DataflowMapTaskExecutorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(IntrinsicMapTaskExecutorFactory.class);

  public static IntrinsicMapTaskExecutorFactory defaultFactory() {
    return new IntrinsicMapTaskExecutorFactory();
  }

  private IntrinsicMapTaskExecutorFactory() {}

  /**
   * Creates a new {@link DataflowMapTaskExecutor} from the given {@link MapTask} definition using
   * the provided {@link ReaderFactory}.
   */
  @Override
  public DataflowMapTaskExecutor create(
      MutableNetwork<Node, Edge> network,
      PipelineOptions options,
      String stageName,
      ReaderFactory readerFactory,
      SinkFactory sinkFactory,
      DataflowExecutionContext<?> executionContext,
      CounterSet counterSet,
      IdGenerator idGenerator) {

    // Swap out all the InstructionOutput nodes with OutputReceiver nodes
    boolean isStreaming = options.as(StreamingOptions.class).isStreaming();
    Networks.replaceDirectedNetworkNodes(
        network, createOutputReceiversTransform(stageName, counterSet, isStreaming));

    // Swap out all the ParallelInstruction nodes with Operation nodes. While updating the network,
    // we keep track of
    // the created Operations so that if an exception is encountered we can properly abort started
    // operations.
    ArrayList<Operation> createdOperations = new ArrayList<>();
    try {
      Networks.replaceDirectedNetworkNodes(
          network,
          createOperationTransformForParallelInstructionNodes(
              stageName,
              network,
              options,
              readerFactory,
              sinkFactory,
              executionContext,
              createdOperations));
    } catch (RuntimeException exn) {
      for (Operation o : createdOperations) {
        try {
          o.abort();
        } catch (Exception exn2) {
          exn.addSuppressed(exn2);
        }
      }
      throw exn;
    }

    // Collect all the operations within the network and attach all the operations as receivers
    // to preceding output receivers.
    List<Node> topoOrder = new ArrayList<>();
    for (Node node : Networks.topologicalOrder(network)) {
      topoOrder.add(node);
    }
    List<Operation> topoSortedOperations = new ArrayList<>();
    for (OperationNode node : Iterables.filter(topoOrder, OperationNode.class)) {
      topoSortedOperations.add(node.getOperation());

      for (Node predecessor :
          Iterables.filter(network.predecessors(node), OutputReceiverNode.class)) {
        ((OutputReceiverNode) predecessor)
            .getOutputReceiver()
            .addOutput((Receiver) node.getOperation());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("Map task network: {}", Networks.toDot(network));
    }

    IntrinsicMapTaskExecutor executor =
        IntrinsicMapTaskExecutor.withSharedCounterSet(
            topoSortedOperations, counterSet, executionContext.getExecutionStateTracker());
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    int minCodegenSteps = FusedSegmentCompiler.resolveMinCodegenChainSteps(experiments);
    List<CandidateCompiledChain> compilableChains =
        findCompilableChains(network, topoOrder, minCodegenSteps);
    installStageTrampolineIfNeeded(
        network, topoOrder, topoSortedOperations, executor, options, compilableChains);
    installCompiledSegmentsIfPossible(compilableChains);
    return executor;
  }

  private static final class CandidateCompiledChain {
    final FastStatelessParDoFn<?, ?> headFn;
    final OperationNode headNode;
    final List<FastStatelessParDoFn.ChainLink> links;
    final List<OutputReceiverNode> internalEdgeNodes;

    CandidateCompiledChain(
        FastStatelessParDoFn<?, ?> headFn,
        OperationNode headNode,
        List<FastStatelessParDoFn.ChainLink> links,
        List<OutputReceiverNode> internalEdgeNodes) {
      this.headFn = headFn;
      this.headNode = headNode;
      this.links = links;
      this.internalEdgeNodes = internalEdgeNodes;
    }
  }

  @SuppressWarnings({"unchecked", "nullness"})
  private static List<CandidateCompiledChain> findCompilableChains(
      Network<Node, Edge> network, List<Node> topoOrder, int minCodegenSteps) {
    if (minCodegenSteps <= 1) {
      if (minCodegenSteps <= 0) {
        return Collections.emptyList();
      }
      minCodegenSteps = 2;
    }
    List<CandidateCompiledChain> chains = new ArrayList<>();
    Set<OperationNode> visitedOps = new HashSet<>();
    for (Node node : topoOrder) {
      if (!(node instanceof OperationNode)) {
        continue;
      }
      OperationNode headNode = (OperationNode) node;
      if (visitedOps.contains(headNode)) {
        continue;
      }
      FastStatelessParDoFn<?, ?> headFn = extractCompilableParDoFn(headNode);
      if (headFn == null) {
        continue;
      }

      List<FastStatelessParDoFn.ChainLink> links = new ArrayList<>();
      List<OutputReceiverNode> edgeNodes = new ArrayList<>();
      List<OperationNode> memberNodes = new ArrayList<>();
      memberNodes.add(headNode);
      OperationNode curOpNode = headNode;
      while (network.outDegree(curOpNode) == 1) {
        Node succ = Iterables.getOnlyElement(network.successors(curOpNode));
        if (!(succ instanceof OutputReceiverNode)) {
          break;
        }
        OutputReceiverNode outNode = (OutputReceiverNode) succ;
        OutputReceiver outRecv = outNode.getOutputReceiver();
        if (!outRecv.hasSimpleSinglePath()
            || network.inDegree(outNode) != 1
            || network.outDegree(outNode) != 1) {
          break;
        }
        Node nextCandidate = Iterables.getOnlyElement(network.successors(outNode));
        if (!(nextCandidate instanceof OperationNode)
            || network.inDegree(nextCandidate) != 1
            || visitedOps.contains(nextCandidate)) {
          break;
        }
        OperationNode nextOpNode = (OperationNode) nextCandidate;
        FastStatelessParDoFn<?, ?> nextFn = extractCompilableParDoFn(nextOpNode);
        if (nextFn == null) {
          break;
        }
        ParDoOperation nextParDoOp = (ParDoOperation) nextOpNode.getOperation();
        OperationContext.FastProcessContext fastCtx = nextParDoOp.getFastProcessContext();
        if (fastCtx == null) {
          break;
        }

        links.add(new FastStatelessParDoFn.ChainLink(outRecv.getOnlyCounter(), fastCtx, nextFn));
        edgeNodes.add(outNode);
        memberNodes.add(nextOpNode);
        curOpNode = nextOpNode;
      }

      visitedOps.addAll(memberNodes);
      if (1 + links.size() >= minCodegenSteps) {
        chains.add(new CandidateCompiledChain(headFn, headNode, links, edgeNodes));
      }
    }
    return chains;
  }

  private static void installCompiledSegmentsIfPossible(
      List<CandidateCompiledChain> compilableChains) {
    for (CandidateCompiledChain chain : compilableChains) {
      // Re-scan if StageTrampoline inserted an intra-compiled cut on a deep chain
      List<FastStatelessParDoFn.ChainLink> subLinks = new ArrayList<>();
      FastStatelessParDoFn<?, ?> curHead = chain.headFn;
      for (int i = 0; i < chain.links.size(); i++) {
        OutputReceiver edgeRecv = chain.internalEdgeNodes.get(i).getOutputReceiver();
        FastStatelessParDoFn.ChainLink link = chain.links.get(i);
        if (!edgeRecv.hasSimpleSinglePath()) {
          if (!subLinks.isEmpty()) {
            curHead.setDownstreamChain(new ArrayList<>(subLinks));
          }
          subLinks.clear();
          curHead = link.targetFn;
        } else {
          subLinks.add(link);
        }
      }
      if (!subLinks.isEmpty()) {
        curHead.setDownstreamChain(subLinks);
      }
    }
  }

  @SuppressWarnings("nullness")
  private static FastStatelessParDoFn<?, ?> extractCompilableParDoFn(OperationNode opNode) {
    Operation op = opNode.getOperation();
    if (!(op instanceof ParDoOperation)) {
      return null;
    }
    ParDoOperation parDoOp = (ParDoOperation) op;
    if (parDoOp.getFastProcessContext() == null) {
      return null;
    }
    try {
      ParDoFn fn = parDoOp.getFn();
      if (fn instanceof FastStatelessParDoFn) {
        FastStatelessParDoFn<?, ?> fastFn = (FastStatelessParDoFn<?, ?>) fn;
        if (fastFn.canCompileInSegment()) {
          return fastFn;
        }
      }
    } catch (Exception ignored) {
      // ignore
    }
    return null;
  }

  static final int DEFAULT_TRAMPOLINE_FUSION_THRESHOLD = 90;
  static final int DEFAULT_COMPILED_TRAMPOLINE_THRESHOLD = 115;
  private static final String TRAMPOLINE_THRESHOLD_PREFIX = "trampoline_fusion_threshold=";
  private static final String DISABLE_TRAMPOLINE_EXPERIMENT = "disable_fusion_trampoline";

  static int resolveTrampolineThreshold(PipelineOptions options) {
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    if (experiments != null) {
      if (experiments.contains(DISABLE_TRAMPOLINE_EXPERIMENT)) {
        return 0;
      }
      for (String exp : experiments) {
        if (exp != null && exp.startsWith(TRAMPOLINE_THRESHOLD_PREFIX)) {
          return Integer.parseInt(exp.substring(TRAMPOLINE_THRESHOLD_PREFIX.length()));
        }
      }
    }
    return DEFAULT_TRAMPOLINE_FUSION_THRESHOLD;
  }

  private static boolean hasExplicitTrampolineThreshold(PipelineOptions options) {
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    if (experiments != null) {
      for (String exp : experiments) {
        if (exp != null && exp.startsWith(TRAMPOLINE_THRESHOLD_PREFIX)) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("nullness")
  private static void installStageTrampolineIfNeeded(
      Network<Node, Edge> network,
      List<Node> topoOrder,
      List<Operation> topoSortedOperations,
      IntrinsicMapTaskExecutor executor,
      PipelineOptions options,
      List<CandidateCompiledChain> compilableChains) {
    int threshold = resolveTrampolineThreshold(options);
    if (threshold <= 0) {
      return;
    }
    int compiledThreshold =
        hasExplicitTrampolineThreshold(options) ? threshold : DEFAULT_COMPILED_TRAMPOLINE_THRESHOLD;
    Map<OutputReceiverNode, FastStatelessParDoFn<?, ?>> eligibleCompiledEdges = new HashMap<>();
    Set<OperationNode> eligibleCompiledHeads = new HashSet<>();
    for (CandidateCompiledChain c : compilableChains) {
      eligibleCompiledHeads.add(c.headNode);
      for (int i = 0; i < c.internalEdgeNodes.size(); i++) {
        eligibleCompiledEdges.put(c.internalEdgeNodes.get(i), c.links.get(i).targetFn);
      }
    }

    Map<Node, Integer> depthAtNode = new HashMap<>(topoOrder.size() * 2);
    Map<OperationNode, Integer> compiledTargetWeight = new HashMap<>();
    List<OutputReceiver> cutReceivers = new ArrayList<>();

    for (Node node : topoOrder) {
      if (node instanceof OperationNode) {
        OperationNode opNode = (OperationNode) node;
        Operation op = opNode.getOperation();
        int inDepth = 0;
        for (Node pred : network.predecessors(node)) {
          Integer d = depthAtNode.get(pred);
          if (d != null && d > inDepth) {
            inDepth = d;
          }
        }
        Integer overrideWeight = compiledTargetWeight.get(opNode);
        int selfWeight =
            overrideWeight != null
                ? overrideWeight
                : estimateOperationFrameWeight(op, eligibleCompiledHeads.contains(opNode));
        depthAtNode.put(node, inDepth + selfWeight);
      } else if (node instanceof OutputReceiverNode) {
        OutputReceiverNode outNode = (OutputReceiverNode) node;
        int producerDepth = 0;
        boolean producerIsFlatten = false;
        for (Node pred : network.predecessors(outNode)) {
          Integer d = depthAtNode.get(pred);
          if (d != null && d > producerDepth) {
            producerDepth = d;
          }
          if (pred instanceof OperationNode
              && ((OperationNode) pred).getOperation() instanceof FlattenOperation) {
            producerIsFlatten = true;
          }
        }
        if (producerIsFlatten) {
          // Never cut immediately after Flatten so predecessor OutputReceiver lazy byte observers
          // remain atomic across Flatten into the consuming operation.
          depthAtNode.put(outNode, producerDepth + 1);
        } else {
          FastStatelessParDoFn<?, ?> compiledNext = eligibleCompiledEdges.get(outNode);
          if (compiledNext != null) {
            int linkWeight = compiledNext.compiledLinkFrameWeight();
            if (producerDepth + linkWeight > compiledThreshold) {
              cutReceivers.add(outNode.getOutputReceiver());
              depthAtNode.put(outNode, 1);
            } else {
              OperationNode nextOpNode =
                  (OperationNode) Iterables.getOnlyElement(network.successors(outNode));
              compiledTargetWeight.put(nextOpNode, linkWeight);
              depthAtNode.put(outNode, producerDepth);
            }
          } else {
            int edgeWeight = network.outDegree(outNode) > 1 ? 2 : 1;
            int consumerWeight =
                maxNonFlattenConsumerWeight(network, outNode, eligibleCompiledHeads);
            if (consumerWeight > 0
                && !isWriteOnlyEdge(network, outNode)
                && producerDepth + edgeWeight + consumerWeight > threshold) {
              cutReceivers.add(outNode.getOutputReceiver());
              depthAtNode.put(outNode, 1);
            } else {
              depthAtNode.put(outNode, producerDepth + edgeWeight);
            }
          }
        }
      }
    }

    if (cutReceivers.isEmpty()) {
      return;
    }

    StageTrampoline trampoline = new StageTrampoline(cutReceivers);
    executor.setStageTrampoline(trampoline);
    for (Operation op : topoSortedOperations) {
      if (op instanceof ReadOperation) {
        ((ReadOperation) op).setStageTrampoline(trampoline);
      } else if (op instanceof ParDoOperation) {
        try {
          ParDoFn fn = ((ParDoOperation) op).getFn();
          if (fn instanceof BatchModeUngroupingParDoFn) {
            ((BatchModeUngroupingParDoFn<?, ?>) fn).setStageTrampoline(trampoline);
          }
        } catch (Exception ignored) {
          // ParDoOperation.getFn() simply returns the field
        }
      }
    }
  }

  private static int maxNonFlattenConsumerWeight(
      Network<Node, Edge> network,
      OutputReceiverNode outNode,
      Set<OperationNode> eligibleCompiledHeads) {
    int maxWeight = 0;
    for (Node succ : network.successors(outNode)) {
      if (succ instanceof OperationNode) {
        OperationNode succOpNode = (OperationNode) succ;
        Operation op = succOpNode.getOperation();
        if (op instanceof FlattenOperation) {
          for (Node flatOut : network.successors(succ)) {
            if (flatOut instanceof OutputReceiverNode) {
              maxWeight =
                  Math.max(
                      maxWeight,
                      maxNonFlattenConsumerWeight(
                          network, (OutputReceiverNode) flatOut, eligibleCompiledHeads));
            }
          }
        } else {
          maxWeight =
              Math.max(
                  maxWeight,
                  estimateOperationFrameWeight(op, eligibleCompiledHeads.contains(succOpNode)));
        }
      }
    }
    return maxWeight;
  }

  private static boolean isWriteOnlyEdge(Network<Node, Edge> network, OutputReceiverNode outNode) {
    boolean sawWrite = false;
    for (Node succ : network.successors(outNode)) {
      if (!(succ instanceof OperationNode)
          || !(((OperationNode) succ).getOperation() instanceof WriteOperation)) {
        return false;
      }
      sawWrite = true;
    }
    return sawWrite;
  }

  private static int estimateOperationFrameWeight(Operation op, boolean isCompiledHead) {
    if (op instanceof FlattenOperation) {
      return 1;
    }
    if (op instanceof ReadOperation || op instanceof WriteOperation) {
      return 2;
    }
    if (op instanceof ParDoOperation) {
      try {
        ParDoFn fn = ((ParDoOperation) op).getFn();
        if (fn instanceof FastStatelessParDoFn) {
          int w = ((FastStatelessParDoFn<?, ?>) fn).estimatedFrameWeight();
          // FastStatelessParDoFn.estimatedFrameWeight() returns 2/4 when compilable; add +1 when
          // not acting as an active compiled segment head.
          return isCompiledHead
              ? w
              : (w <= 4 && ((FastStatelessParDoFn<?, ?>) fn).canCompileInSegment() ? w + 1 : w);
        }
      } catch (Exception ignored) {
        // fall through
      }
      return 8;
    }
    return 6;
  }

  /**
   * Creates an {@link Operation} from the given {@link ParallelInstruction} definition using the
   * provided {@link ReaderFactory}.
   */
  Function<Node, Node> createOperationTransformForParallelInstructionNodes(
      final String stageName,
      final Network<Node, Edge> network,
      final PipelineOptions options,
      final ReaderFactory readerFactory,
      final SinkFactory sinkFactory,
      final DataflowExecutionContext<?> executionContext,
      final List<Operation> createdOperations) {

    return new TypeSafeNodeFunction<ParallelInstructionNode>(ParallelInstructionNode.class) {
      @Override
      public Node typedApply(ParallelInstructionNode node) {
        ParallelInstruction instruction = node.getParallelInstruction();
        NameContext nameContext =
            NameContext.create(
                stageName,
                instruction.getOriginalName(),
                instruction.getSystemName(),
                instruction.getName());
        OperationNode result;
        try {
          DataflowOperationContext context = executionContext.createOperationContext(nameContext);
          if (instruction.getRead() != null) {
            result =
                createReadOperation(
                    network, node, options, readerFactory, executionContext, context);
          } else if (instruction.getWrite() != null) {
            result = createWriteOperation(node, options, sinkFactory, executionContext, context);
          } else if (instruction.getParDo() != null) {
            result = createParDoOperation(network, node, options, executionContext, context);
          } else if (instruction.getPartialGroupByKey() != null) {
            result =
                createPartialGroupByKeyOperation(network, node, options, executionContext, context);
          } else if (instruction.getFlatten() != null) {
            result = createFlattenOperation(network, node, context);
          } else {
            throw new IllegalArgumentException(
                String.format("Unexpected instruction: %s", instruction));
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        createdOperations.add(result.getOperation());
        return result;
      }
    };
  }

  OperationNode createReadOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      ReaderFactory readerFactory,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    ReadInstruction read = instruction.getRead();

    Source cloudSource = CloudSourceUtils.flattenBaseSpecs(read.getSource());
    CloudObject sourceSpec = CloudObject.fromSpec(cloudSource.getSpec());
    Coder<?> coder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudSource.getCodec()));
    NativeReader<?> reader =
        readerFactory.create(sourceSpec, coder, options, executionContext, operationContext);
    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(ReadOperation.create(reader, receivers, operationContext));
  }

  OperationNode createWriteOperation(
      ParallelInstructionNode node,
      PipelineOptions options,
      SinkFactory sinkFactory,
      DataflowExecutionContext executionContext,
      DataflowOperationContext context)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    WriteInstruction write = instruction.getWrite();
    Coder<?> coder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(write.getSink().getCodec()));
    CloudObject cloudSink = CloudObject.fromSpec(write.getSink().getSpec());
    Sink<?> sink = sinkFactory.create(cloudSink, coder, options, executionContext, context);
    return OperationNode.create(WriteOperation.create(sink, EMPTY_OUTPUT_RECEIVER_ARRAY, context));
  }

  private ParDoFnFactory parDoFnFactory = new DefaultParDoFnFactory();

  private OperationNode createParDoOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    ParDoInstruction parDo = instruction.getParDo();

    TupleTag<?> mainOutputTag = tupleTag(parDo.getMultiOutputInfos().get(0));
    ImmutableMap.Builder<TupleTag<?>, Integer> outputTagsToReceiverIndicesBuilder =
        ImmutableMap.builder();
    int successorOffset = 0;
    for (Node successor : network.successors(node)) {
      for (Edge edge : network.edgesConnecting(node, successor)) {
        outputTagsToReceiverIndicesBuilder.put(
            tupleTag(((MultiOutputInfoEdge) edge).getMultiOutputInfo()), successorOffset);
      }
      successorOffset += 1;
    }
    ParDoFn fn =
        parDoFnFactory.create(
            options,
            CloudObject.fromSpec(parDo.getUserFn()),
            parDo.getSideInputs(),
            mainOutputTag,
            outputTagsToReceiverIndicesBuilder.build(),
            executionContext,
            operationContext);

    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(new ParDoOperation(fn, receivers, operationContext));
  }

  private static <V> TupleTag<V> tupleTag(MultiOutputInfo multiOutputInfo) {
    return new TupleTag<>(multiOutputInfo.getTag());
  }

  <K> OperationNode createPartialGroupByKeyOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    PartialGroupByKeyInstruction pgbk = instruction.getPartialGroupByKey();
    OutputReceiver[] receivers = getOutputReceivers(network, node);

    Coder<?> windowedCoder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(pgbk.getInputElementCodec()));
    if (!(windowedCoder instanceof WindowedValueCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "unexpected kind of input coder for PartialGroupByKeyOperation: %s", windowedCoder));
    }
    Coder<?> elemCoder = ((WindowedValueCoder<?>) windowedCoder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "unexpected kind of input element coder for PartialGroupByKeyOperation: %s",
              elemCoder));
    }

    @SuppressWarnings("unchecked")
    KvCoder<K, ?> keyedElementCoder = (KvCoder<K, ?>) elemCoder;

    CloudObject cloudUserFn =
        pgbk.getValueCombiningFn() != null
            ? CloudObject.fromSpec(pgbk.getValueCombiningFn())
            : null;
    ParDoFn fn =
        PartialGroupByKeyParDoFns.create(
            options,
            keyedElementCoder,
            cloudUserFn,
            pgbk.getSideInputs(),
            Arrays.<Receiver>asList(receivers),
            executionContext,
            operationContext);

    return OperationNode.create(new ParDoOperation(fn, receivers, operationContext));
  }

  OperationNode createFlattenOperation(
      Network<Node, Edge> network, ParallelInstructionNode node, OperationContext context) {
    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(new FlattenOperation(receivers, context));
  }

  /**
   * Returns a function which can convert {@link InstructionOutput}s into {@link OutputReceiver}s.
   */
  static Function<Node, Node> createOutputReceiversTransform(
      final String stageName, final CounterFactory counterFactory, final boolean isStreaming) {
    return new TypeSafeNodeFunction<InstructionOutputNode>(InstructionOutputNode.class) {
      @Override
      public Node typedApply(InstructionOutputNode input) {
        InstructionOutput cloudOutput = input.getInstructionOutput();
        OutputReceiver outputReceiver = new OutputReceiver();
        Coder<?> coder =
            CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudOutput.getCodec()));

        ElementCounter outputCounter =
            DataflowOutputCounter.create(
                cloudOutput.getName(),
                new ElementByteSizeObservableCoder<>(coder),
                counterFactory,
                NameContext.create(
                    stageName,
                    cloudOutput.getOriginalName(),
                    cloudOutput.getSystemName(),
                    cloudOutput.getName()),
                isStreaming);
        outputReceiver.addOutputCounter(outputCounter);

        return OutputReceiverNode.create(outputReceiver, coder, input.getPcollectionId());
      }
    };
  }

  private static final OutputReceiver[] EMPTY_OUTPUT_RECEIVER_ARRAY = new OutputReceiver[0];

  static OutputReceiver[] getOutputReceivers(Network<Node, Edge> network, Node node) {
    int outDegree = network.outDegree(node);
    if (outDegree == 0) {
      return EMPTY_OUTPUT_RECEIVER_ARRAY;
    }

    OutputReceiver[] receivers = new OutputReceiver[outDegree];
    Iterator<Node> receiverNodes = network.successors(node).iterator();
    int i = 0;
    do {
      receivers[i] = ((OutputReceiverNode) receiverNodes.next()).getOutputReceiver();
      i += 1;
    } while (receiverNodes.hasNext());

    return receivers;
  }

  /** Adapts a Coder to the ElementByteSizeObservable interface. */
  @SuppressWarnings({"unchecked", "nullness"})
  public static class ElementByteSizeObservableCoder<T>
      implements ElementByteSizeObservable<T>, RawWindowedByteSizeObservable {
    final Coder<T> coder;
    private final FullWindowedValueCoder<Object> fullWvCoder;
    private final ParamWindowedValueCoder<Object> paramWvCoder;

    public ElementByteSizeObservableCoder(Coder<T> coder) {
      this.coder = coder;
      Class<?> cls = coder.getClass();
      this.fullWvCoder =
          (cls == FullWindowedValueCoder.class) ? (FullWindowedValueCoder<Object>) coder : null;
      this.paramWvCoder =
          (cls == ParamWindowedValueCoder.class) ? (ParamWindowedValueCoder<Object>) coder : null;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return coder.isRegisterByteSizeObserverCheap(value);
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      coder.registerByteSizeObserver(value, observer);
    }

    @Override
    public boolean isRegisterRawByteSizeObserverCheap(
        Object rawValue, WindowedValue<?> templateElem) {
      if (fullWvCoder != null || paramWvCoder != null) {
        // WindowedValueCoder inherits Coder.isRegisterByteSizeObserverCheap -> false
        return false;
      }
      WindowedValue<?> materialized =
          (rawValue == templateElem.getValue()) ? templateElem : templateElem.withValue(rawValue);
      return coder.isRegisterByteSizeObserverCheap((T) materialized);
    }

    @Override
    public void registerRawByteSizeObserver(
        Object rawValue, WindowedValue<?> templateElem, ElementByteSizeObserver observer)
        throws Exception {
      if (fullWvCoder != null) {
        InstantCoder.of().registerByteSizeObserver(templateElem.getTimestamp(), observer);
        fullWvCoder.getWindowsCoder().registerByteSizeObserver(templateElem.getWindows(), observer);
        PaneInfoCoder.INSTANCE.registerByteSizeObserver(templateElem.getPaneInfo(), observer);
        fullWvCoder.getValueCoder().registerByteSizeObserver(rawValue, observer);
        return;
      }
      if (paramWvCoder != null) {
        paramWvCoder.getValueCoder().registerByteSizeObserver(rawValue, observer);
        return;
      }
      WindowedValue<?> materialized =
          (rawValue == templateElem.getValue()) ? templateElem : templateElem.withValue(rawValue);
      coder.registerByteSizeObserver((T) materialized, observer);
    }
  }
}
