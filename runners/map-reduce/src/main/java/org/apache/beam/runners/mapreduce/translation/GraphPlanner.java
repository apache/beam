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
package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Class that optimizes the initial graph to a fused graph.
 */
public class GraphPlanner {


  public GraphPlanner() {
  }

  public Graphs.FusedGraph plan(Graph<Graphs.Step, Graphs.Tag> initGraph) {
    Graphs.FusedGraph fusedGraph = new Graphs.FusedGraph();
    // Convert from the list of steps to Graphs.
    for (Graphs.Step step : Lists.reverse(initGraph.getSteps())) {
      Graphs.FusedStep fusedStep = new Graphs.FusedStep();
      fusedStep.addStep(step);
      fusedGraph.addFusedStep(fusedStep);

      tryFuse(fusedGraph, fusedStep);
    }
    return fusedGraph;
  }

  private void tryFuse(Graphs.FusedGraph fusedGraph, Graphs.FusedStep fusedStep) {
    if (fusedStep.getOutputTags().size() != 1) {
      return;
    }
    Graphs.Tag outTag = Iterables.getOnlyElement(fusedStep.getOutputTags());
    if (fusedGraph.getConsumers(outTag).size() != 1) {
      return;
    }
    Graphs.FusedStep consumer = Iterables.getOnlyElement(fusedGraph.getConsumers(outTag));
    if (fusedStep.containsGroupByKey() && consumer.containsGroupByKey()) {
      return;
    }
    fusedGraph.merge(fusedStep, consumer);
  }
}
