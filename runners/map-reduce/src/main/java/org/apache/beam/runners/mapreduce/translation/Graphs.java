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

    public FusedGraph() {
      this.graph = new Graph<>();
    }

    public void addFusedStep(FusedStep fusedStep) {
      graph.addStep(fusedStep);
    }

    public void merge(FusedStep src, FusedStep dest) {
      for (Step step : src.steps.getSteps()) {
        dest.addStep(step);
      }
      for (Tag inTag : src.getInputTags()) {
        graph.addEdge(inTag, dest);
      }
      for (Tag outTag : src.getOutputTags()) {
        graph.addEdge(dest, outTag);
      }
      graph.removeStep(src);
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
  }

  public static class FusedStep extends Graph.AbstractStep<Tag> {
    private final Graph<Step, Tag> steps;
    private Step groupByKeyStep;

    public FusedStep() {
      this.steps = new Graph<>();
      this.groupByKeyStep = null;
    }

    @Override
    public List<Tag> getInputTags() {
      return steps.getInputTags();
    }

    @Override
    public List<Tag> getOutputTags() {
      return steps.getOutputTags();
    }

    public void addStep(Step step) {
      steps.addStep(step);
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

    public String getFullName() {
      return toString();
    }
  }

  @AutoValue
  public abstract static class Step extends Graph.AbstractStep<Tag> {
    abstract String getFullName();
    // TODO: remove public
    public abstract Operation getOperation();

    public static Step of(
        String fullName,
        Operation operation,
        List<Tag> inputTags,
        List<Tag> outputTags) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graphs_Step(
          inputTags, outputTags, fullName, operation);
    }
  }

  @AutoValue
  public abstract static class Tag extends Graph.AbstractTag {
    abstract TupleTag<?> getTupleTag();
    abstract Coder<?> getCoder();

    public static Tag of(TupleTag<?> tupleTag, Coder<?> coder) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graphs_Tag(
          tupleTag, coder);
    }
  }
}
