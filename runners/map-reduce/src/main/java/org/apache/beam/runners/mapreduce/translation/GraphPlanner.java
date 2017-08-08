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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.mapreduce.MapReducePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Class that optimizes the initial graph to a fused graph.
 */
public class GraphPlanner {

  private final MapReducePipelineOptions options;

  public GraphPlanner(MapReducePipelineOptions options) {
    this.options = checkNotNull(options, "options");
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
        if (producer.getOperation() instanceof FileWriteOperation) {
          continue;
        }
        String tagName = tag.getName();
        String fileName = ConfigurationUtils.toFileName(tagName);

        // TODO: should not hard-code windows coder.
        WindowedValue.WindowedValueCoder<?> writeValueCoder = WindowedValue.getFullCoder(
            tag.getCoder(), WindowingStrategy.globalDefault().getWindowFn().windowCoder());

        fusedStep.addStep(
            Graphs.Step.of(
                tagName + "/Write",
                new FileWriteOperation(fileName, writeValueCoder)),
            ImmutableList.of(tag),
            ImmutableList.<Graphs.Tag>of());

        String readStepName = tagName + "/Read";
        Graphs.Tag readOutput = Graphs.Tag.of(
            readStepName + ".out", tag.getTupleTag(), tag.getCoder());
        for (Graphs.FusedStep consumer : consumers) {
          // Re-direct tag to readOutput.
          List<Graphs.Step> receivers = consumer.getConsumers(tag);
          for (Graphs.Step step : receivers) {
            consumer.addEdge(readOutput, step);
          }
          consumer.removeTag(tag);

          String filePath = ConfigurationUtils.getFileOutputPath(
              options.getFileOutputDir(), fusedStep.getStageId(), fileName);
          consumer.addStep(
              Graphs.Step.of(
                  readStepName,
                  new FileReadOperation(filePath, tag.getCoder(), tag.getTupleTag())),
              ImmutableList.<Graphs.Tag>of(),
              ImmutableList.of(readOutput));
        }
      }
    }

    // Insert PartitionOperation
    for (final Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      List<Graphs.Step> readSteps = fusedStep.getStartSteps();

      List<ReadOperation> readOperations = new ArrayList<>();
      List<Graphs.Tag> readOutTags = new ArrayList<>();
      List<TupleTag<?>> readOutTupleTags = new ArrayList<>();
      StringBuilder partitionStepName = new StringBuilder();
      for (Graphs.Step step : readSteps) {
        checkState(step.getOperation() instanceof ReadOperation);
        readOperations.add(((ReadOperation) step.getOperation()));
        Graphs.Tag tag = Iterables.getOnlyElement(fusedStep.getOutputTags(step));
        readOutTags.add(tag);
        readOutTupleTags.add(tag.getTupleTag());
        partitionStepName.append(step.getFullName());

        fusedStep.removeStep(step);
      }
      if (partitionStepName.length() > 0) {
        partitionStepName.deleteCharAt(partitionStepName.length() - 1);
      }

      Graphs.Step partitionStep = Graphs.Step.of(
          partitionStepName.toString(), new PartitionOperation(readOperations, readOutTupleTags));
      fusedStep.addStep(partitionStep, ImmutableList.<Graphs.Tag>of(), readOutTags);
    }

    // Setup side inputs
    for (final Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      for (Graphs.Step step : fusedStep.getSteps()) {
        if (!(step.getOperation() instanceof ParDoOperation)) {
          continue;
        }
        ParDoOperation parDo = (ParDoOperation) step.getOperation();
        List<Graphs.Tag> sideInputTags = parDo.getSideInputTags();
        if (sideInputTags.size() == 0) {
          continue;
        }
        Map<TupleTag<?>, String> tupleTagToFilePath = Maps.newHashMap();
        for (Graphs.Tag sideInTag : sideInputTags) {
          tupleTagToFilePath.put(
              sideInTag.getTupleTag(),
              ConfigurationUtils.getFileOutputPath(
                  options.getFileOutputDir(),
                  fusedGraph.getProducer(sideInTag).getStageId(),
                  ConfigurationUtils.toFileName(sideInTag.getName())));
        }
        parDo.setupSideInput(tupleTagToFilePath);
      }
    }
    return fusedGraph;
  }
}
