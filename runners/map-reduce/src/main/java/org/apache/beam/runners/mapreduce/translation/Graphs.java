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

import com.google.auto.value.AutoValue;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Class that defines graph vertices.
 */
public class Graphs {

  private Graphs() {}

  public static class FusedGraph {
    private final Graph<FusedStep, Tag> graph;
    private int stageId = 0;

    public FusedGraph() {
      this.graph = new Graph<>();
    }

    public FusedGraph(Graph<Graphs.Step, Tag> initGraph) {
      this.graph = new Graph<>();

      // Convert from the list of steps to Graphs.
      for (Graphs.Step step : Lists.reverse(initGraph.getSteps())) {
        tryFuse(step, initGraph.getInputTags(step), initGraph.getOutputTags(step));
      }
      // Remove unused external tags.
      for (FusedStep fusedStep : graph.getSteps()) {
        for (Tag outTag : graph.getOutputTags(fusedStep)) {
          if (graph.getConsumers(outTag).isEmpty()) {
            graph.removeTag(outTag);
          }
        }
      }
    }

    public void tryFuse(
        Graphs.Step step,
        List<Graphs.Tag> inTags,
        List<Graphs.Tag> outTags) {
      if (canFuse(step, inTags, outTags)) {
        Graphs.Tag outTag = Iterables.getOnlyElement(outTags);
        Graphs.FusedStep consumer = Iterables.getOnlyElement(graph.getConsumers(outTag));
        consumer.addStep(step, inTags, outTags);
        for (Graphs.Tag in : inTags) {
          graph.addEdge(in, consumer);
        }
        graph.removeTag(outTag);
        graph.addEdge(consumer, outTag);
      } else {
        Graphs.FusedStep newFusedStep = new Graphs.FusedStep(stageId++);
        newFusedStep.addStep(step, inTags, outTags);
        graph.addStep(newFusedStep, inTags, outTags);
      }
    }

    private boolean canFuse(
        Graphs.Step step,
        List<Graphs.Tag> inTags,
        List<Graphs.Tag> outTags) {
      if (step.getOperation() instanceof FileWriteOperation) {
        return false;
      }
      if (outTags.size() != 1) {
        return false;
      }
      Graphs.Tag outTag = Iterables.getOnlyElement(outTags);
      if (graph.getConsumers(outTag).size() != 1) {
        return false;
      }
      Graphs.FusedStep consumer = Iterables.getOnlyElement(graph.getConsumers(outTag));
      if (consumer.containsGroupByKey() && step.getOperation() instanceof GroupByKeyOperation) {
        return false;
      }
      return true;
    }

    public FusedStep getProducer(Tag tag) {
      return graph.getProducer(tag);
    }

    public List<FusedStep> getConsumers(Tag tag) {
      return graph.getConsumers(tag);
    }

    public List<FusedStep> getFusedSteps() {
      return graph.getSteps();
    }

    public List<Tag> getInputTags(FusedStep fusedStep) {
      return graph.getInputTags(fusedStep);
    }

    public List<Tag> getOutputTags(FusedStep fusedStep) {
      return graph.getOutputTags(fusedStep);
    }
  }

  public static class FusedStep extends Graph.AbstractStep {
    private final int stageId;
    private final Graph<Step, Tag> steps;
    private Step groupByKeyStep;

    public FusedStep(int stageid) {
      this.stageId = stageid;
      this.steps = new Graph<>();
      this.groupByKeyStep = null;
    }

    public int getStageId() {
      return stageId;
    }

    public List<Tag> getInputTags(Step step) {
      return steps.getInputTags(step);
    }

    public List<Tag> getOutputTags(Step step) {
      return steps.getOutputTags(step);
    }

    public void addStep(Step step, List<Tag> inTags, List<Tag> outTags) {
      steps.addStep(step, inTags, outTags);
      if (step.getOperation() instanceof GroupByKeyOperation) {
        groupByKeyStep = step;
      }
    }

    public void removeStep(Step step) {
      steps.removeStep(step);
    }

    public void removeTag(Tag tag) {
      steps.removeTag(tag);
    }

    public void addEdge(Tag inTag, Step step) {
      steps.addEdge(inTag, step);
    }

    public void addEdge(Step step, Tag outTag) {
      steps.addEdge(step, outTag);
    }

    public void removeEdge(Tag inTag, Step step) {
      steps.removeEdge(inTag, step);
    }

    public void removeEdge(Step step, Tag outTag) {
      steps.removeEdge(step, outTag);
    }

    public Step getProducer(Tag tag) {
      return steps.getProducer(tag);
    }

    public List<Step> getConsumers(Tag tag) {
      return steps.getConsumers(tag);
    }

    public List<Step> getSteps() {
      return steps.getSteps();
    }

    public List<Step> getStartSteps() {
      return steps.getStartSteps();
    }

    public boolean containsGroupByKey() {
      return groupByKeyStep != null;
    }

    @Nullable
    public Step getGroupByKeyStep() {
      return groupByKeyStep;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Step step : steps.getSteps()) {
        sb.append(step.getFullName() + "|");
      }
      if (sb.length() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }
      return sb.toString();
    }
  }

  @AutoValue
  public abstract static class Step extends Graph.AbstractStep {
    abstract String getFullName();
    // TODO: remove public
    public abstract Operation getOperation();

    public static Step of(String fullName, Operation operation) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graphs_Step(
          fullName, operation);
    }

    @Override
    public String toString() {
      return getFullName();
    }
  }

  @AutoValue
  public abstract static class Tag extends Graph.AbstractTag implements Serializable {
    abstract String getName();
    abstract TupleTag<?> getTupleTag();
    abstract Coder<?> getCoder();

    @Override
    public String toString() {
      return getName();
    }

    public static Tag of(String name, TupleTag<?> tupleTag, Coder<?> coder) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graphs_Tag(
          name, tupleTag, coder);
    }
  }
}
