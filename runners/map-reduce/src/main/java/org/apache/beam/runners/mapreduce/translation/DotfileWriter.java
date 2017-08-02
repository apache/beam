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

/**
 * Class that outputs {@link Graph} to dot file.
 */
public class DotfileWriter {

  public static <StepT extends Graph.AbstractStep<TagT>, TagT extends Graph.AbstractTag>
  String toDotfile(Graphs.FusedGraph fusedGraph) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ndigraph G {\n");

    int i = 0;
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      sb.append(String.format("  subgraph \"cluster_%d\" {\n", i++));
      for (Graphs.Step step : fusedStep.getSteps()) {
        sb.append(String.format("    \"%s\" [shape=box];\n", step.getFullName()));
        for (Graph.AbstractTag outTag : step.getOutputTags()) {
          sb.append(String.format("    \"%s\" [shape=ellipse];\n", outTag));
        }
      }
      sb.append(String.format("  }"));
    }
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      for (Graphs.Step step : fusedStep.getSteps()) {
        for (Graph.AbstractTag inTag : step.getInputTags()) {
          sb.append(String.format("  \"%s\" -> \"%s\";\n", inTag, step));
        }
        for (Graph.AbstractTag outTag : step.getOutputTags()) {
          sb.append(String.format("  \"%s\" -> \"%s\";\n", step, outTag));
        }
      }
    }
    sb.append("}\n");
    return sb.toString();
  }
}
