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
package org.apache.beam.runners.twister2;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.tset.TBaseGraph;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.BaseTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.twister2.translators.functions.DoFnFunction;
import org.apache.beam.runners.twister2.translators.functions.Twister2SinkFunction;

/**
 * The Twister2 worker that will execute the job logic once the job is submitted from the run
 * method.
 */
public class BeamBatchWorker implements Serializable, BatchTSetIWorker {

  private static final String SIDEINPUTS = "sideInputs";
  private static final String LEAVES = "leaves";
  private static final String GRAPH = "graph";
  private HashMap<String, BatchTSet<?>> sideInputDataSets;
  private Set<TSet> leaves;

  @Override
  public void execute(BatchTSetEnvironment env) {
    Config config = env.getConfig();
    Map<String, String> sideInputIds = (LinkedHashMap<String, String>) config.get(SIDEINPUTS);
    Set<String> leaveIds = (Set<String>) config.get(LEAVES);
    TBaseGraph graph = (TBaseGraph) config.get(GRAPH);
    env.settBaseGraph(graph);
    setupTSets(env, sideInputIds, leaveIds);
    resetEnv(env, graph);
    executePipeline(env);
  }

  /**
   * resets the proper TSetEnvironment in all the TSets in the graph that was sent over the wire.
   *
   * @param env the TSetEnvironment to be set
   * @param graph the graph that contains all the TSets.
   */
  private void resetEnv(BatchTSetEnvironment env, TBaseGraph graph) {
    Set<TBase> nodes = graph.getNodes();
    for (TBase node : nodes) {
      if (node instanceof BaseTSet) {
        ((BaseTSet) node).setTSetEnv(env);
      } else if (node instanceof BaseTLink) {
        ((BaseTLink) node).setTSetEnv(env);
      } else {
        throw new IllegalStateException("node must be either of type BaseTSet or BaseTLink");
      }
    }
  }

  /**
   * Extract the sideInput TSets and the Leaves from the graph.
   *
   * @param env BatchTSetEnvironment which contains the tSet Graph
   * @param sideInputIds the id's of the side Inputs
   * @param leaveIds the id's of the leaf
   */
  private void setupTSets(
      BatchTSetEnvironment env, Map<String, String> sideInputIds, Set<String> leaveIds) {
    sideInputDataSets = new LinkedHashMap<>();
    leaves = new HashSet<>();

    // reset sources, so that the graph does not have two source objects
    // created during deserialization
    Set<BuildableTSet> newSources = new HashSet<>();
    for (BuildableTSet source : env.getGraph().getSources()) {
      newSources.add((BuildableTSet) env.getGraph().getNodeById(source.getId()));
    }
    env.getGraph().setSources(newSources);

    for (Map.Entry<String, String> entry : sideInputIds.entrySet()) {
      BatchTSet curr = (BatchTSet) env.getGraph().getNodeById(entry.getValue());
      sideInputDataSets.put(entry.getKey(), curr);
    }
    for (String leaveId : leaveIds) {
      leaves.add((TSet) env.getGraph().getNodeById(leaveId));
    }
  }

  public void executePipeline(BatchTSetEnvironment env) {
    Map<String, CachedTSet> sideInputTSets = new HashMap<>();
    for (Map.Entry<String, BatchTSet<?>> sides : sideInputDataSets.entrySet()) {
      BatchTSet<?> sideTSet = sides.getValue();
      addInputs((BaseTSet) sideTSet, sideInputTSets);
      CachedTSet tempCache = (CachedTSet) sideTSet.cache();
      sideInputTSets.put(sides.getKey(), tempCache);
    }
    for (TSet leaf : leaves) {
      SinkTSet sinkTSet = (SinkTSet) leaf.direct().sink(new Twister2SinkFunction());
      addInputs(sinkTSet, sideInputTSets);
      eval(env, sinkTSet);
    }
  }

  /** Adds all the side inputs into the sink test so it is available from the DoFn's. */
  private void addInputs(BaseTSet sinkTSet, Map<String, CachedTSet> sideInputTSets) {
    if (sideInputTSets.isEmpty()) {
      return;
    }

    TBaseGraph graph = sinkTSet.getTBaseGraph();
    TBase currNode = null;
    Deque<TBase> deque = new ArrayDeque<>();
    deque.add(sinkTSet);
    while (!deque.isEmpty()) {
      currNode = deque.remove();
      deque.addAll(graph.getPredecessors(currNode));
      if (currNode instanceof ComputeTSet) {
        if (((ComputeTSet) currNode).getComputeFunc() instanceof DoFnFunction) {
          Set<String> sideInputKeys =
              ((DoFnFunction) ((ComputeTSet) currNode).getComputeFunc()).getSideInputKeys();
          for (String sideInputKey : sideInputKeys) {
            if (!sideInputTSets.containsKey(sideInputKey)) {
              throw new IllegalStateException("Side input not found for key " + sideInputKey);
            }
            ((ComputeTSet) currNode).addInput(sideInputKey, sideInputTSets.get(sideInputKey));
          }
        }
      }
    }
  }

  public void eval(BatchTSetEnvironment env, SinkTSet<?> tSet) {
    env.run(tSet);
  }
}
