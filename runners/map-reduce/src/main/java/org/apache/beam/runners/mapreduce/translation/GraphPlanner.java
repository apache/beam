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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Class that optimizes the initial graph to a fused graph.
 */
public class GraphPlanner {


  public GraphPlanner() {
  }

  public Graphs.FusedGraph plan(Graphs.FusedGraph fusedGraph) {
    // Attach writes/reads on fusion boundaries.
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      for (Graphs.Tag tag : fusedGraph.getOutputTags(fusedStep)) {
        List<Graphs.FusedStep> consumers = fusedGraph.getConsumers(tag);
        if (consumers.isEmpty()) {
          continue;
        }
        Graphs.Step producer = fusedStep.getProducer(tag);
        if (producer.getOperation() instanceof ViewOperation) {
          continue;
        }
        String tagName = tag.getName();
        String fileName = tagName.replaceAll("[^A-Za-z0-9]", "0");
        fusedStep.addStep(
            Graphs.Step.of(
                tagName + "/Write",
                new FileWriteOperation(fileName, tag.getCoder())),
            ImmutableList.of(tag),
            ImmutableList.<Graphs.Tag>of());

        String readStepName = tagName + "/Read";
        Graphs.Tag readOutput = Graphs.Tag.of(
            readStepName + ".out", new TupleTag<>(), tag.getCoder());
        for (Graphs.FusedStep consumer : consumers) {
          // Re-direct tag to readOutput.
          List<Graphs.Step> receivers = consumer.getConsumers(tag);
          for (Graphs.Step step : receivers) {
            consumer.addEdge(readOutput, step);
          }
          consumer.removeTag(tag);
          consumer.addStep(
              Graphs.Step.of(
                  readStepName,
                  new FileReadOperation(fusedStep.getStageId(), fileName, tag.getCoder())),
              ImmutableList.<Graphs.Tag>of(),
              ImmutableList.of(readOutput));
        }
      }
    }

    return fusedGraph;
  }
}
