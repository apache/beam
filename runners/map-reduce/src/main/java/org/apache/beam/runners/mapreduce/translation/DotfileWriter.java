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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * Class that outputs {@link Graph} to dot file.
 */
public class DotfileWriter {

  public static <StepT extends Graph.AbstractStep, TagT extends Graph.AbstractTag>
  String toDotfile(Graphs.FusedGraph fusedGraph) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ndigraph G {\n");

    Map<Graphs.FusedStep, String> fusedStepToId = Maps.newHashMap();
    int i = 0;
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      String clusterId = String.format("cluster_%d", i++);
      sb.append(String.format("  subgraph \"%s\" {\n", clusterId));
      sb.append(String.format("    \"%s\" [shape=point style=invis];\n", clusterId));
      fusedStepToId.put(fusedStep, clusterId);

      Set<String> nodeDefines = Sets.newHashSet();
      for (Graphs.Step step : fusedStep.getSteps()) {
        nodeDefines.add(String.format("    \"%s\" [shape=box];\n", step.getFullName()));
        for (Graph.AbstractTag inTag : fusedStep.getInputTags(step)) {
          nodeDefines.add(String.format("    \"%s\" [shape=ellipse];\n", inTag));
        }
        for (Graph.AbstractTag outTag : fusedStep.getOutputTags(step)) {
          nodeDefines.add(String.format("    \"%s\" [shape=ellipse];\n", outTag));
        }
      }
      for (String str : nodeDefines) {
        sb.append(str);
      }
      sb.append(String.format("  }"));
    }
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      // Edges within fused steps.
      for (Graphs.Step step : fusedStep.getSteps()) {
        for (Graph.AbstractTag inTag : fusedStep.getInputTags(step)) {
          sb.append(String.format("  \"%s\" -> \"%s\";\n", inTag, step));
        }
        for (Graph.AbstractTag outTag : fusedStep.getOutputTags(step)) {
          sb.append(String.format("  \"%s\" -> \"%s\";\n", step, outTag));
        }
      }

      // Edges between sub-graphs.
      for (Graphs.Tag inTag : fusedGraph.getInputTags(fusedStep)) {
        sb.append(String.format("  \"%s\" -> \"%s\";\n", inTag, fusedStepToId.get(fusedStep)));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  public static String toDotfile(Graphs.FusedStep fusedStep) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ndigraph G {\n");
    for (Graphs.Step step : fusedStep.getSteps()) {
      sb.append(String.format("  \"%s\" [shape=box];\n", step.getFullName()));
      for (Graph.AbstractTag inTag : fusedStep.getInputTags(step)) {
        sb.append(String.format("  \"%s\" -> \"%s\";\n", inTag, step));
      }
      for (Graph.AbstractTag outTag : fusedStep.getOutputTags(step)) {
        sb.append(String.format("  \"%s\" -> \"%s\";\n", step, outTag));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }
}
