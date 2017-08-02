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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.mapreduce.MapReducePipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Class that maintains contexts during translation.
 */
public class TranslationContext {

  private final UserGraphContext userGraphContext;
  private final Graph<Graphs.Step, Graphs.Tag> initGraph;

  public TranslationContext(MapReducePipelineOptions options) {
    this.userGraphContext = new UserGraphContext(options);
    this.initGraph = new Graph<>();
  }

  public UserGraphContext getUserGraphContext() {
    return userGraphContext;
  }

  public void addInitStep(Graphs.Step step) {
    initGraph.addStep(step);
  }

  /**
   * Returns {@link Graphs.Step steps} in reverse topological order.
   */
  public Graph<Graphs.Step, Graphs.Tag> getInitGraph() {
    return initGraph;
  }

  /**
   * Context of user graph.
   */
  public static class UserGraphContext {
    private final MapReducePipelineOptions options;
    private final Map<PValue, TupleTag<?>> pValueToTupleTag;
    private TransformHierarchy.Node currentNode;

    public UserGraphContext(MapReducePipelineOptions options) {
      this.options = checkNotNull(options, "options");
      this.pValueToTupleTag = Maps.newHashMap();
      this.currentNode = null;
    }

    public MapReducePipelineOptions getOptions() {
      return options;
    }

    public void setCurrentNode(TransformHierarchy.Node node) {
      this.currentNode = node;
      for (Map.Entry<TupleTag<?>, PValue> entry : currentNode.getOutputs().entrySet()) {
        pValueToTupleTag.put(entry.getValue(), entry.getKey());
      }
    }

    public String getStepName() {
      return currentNode.getFullName();
    }

    public PValue getInput() {
      return Iterables.get(currentNode.getInputs().values(), 0);
    }

    public PValue getOutput() {
      return Iterables.get(currentNode.getOutputs().values(), 0);
    }

    public List<Graphs.Tag> getInputTags() {
      return FluentIterable.from(currentNode.getInputs().values())
          .transform(new Function<PValue, Graphs.Tag>() {
            @Override
            public Graphs.Tag apply(PValue pValue) {
              checkState(
                  pValueToTupleTag.containsKey(pValue),
                  String.format("Failed to find TupleTag for pValue: %s.", pValue));
              PCollection<?> pc = (PCollection<?>) pValue;
              return Graphs.Tag.of(
                  pc.getName(), pValueToTupleTag.get(pValue), pc.getCoder());
            }})
          .toList();
    }

    public List<Graphs.Tag> getOutputTags() {
      return FluentIterable.from(currentNode.getOutputs().entrySet())
          .transform(new Function<Map.Entry<TupleTag<?>, PValue>, Graphs.Tag>() {
            @Override
            public Graphs.Tag apply(Map.Entry<TupleTag<?>, PValue> entry) {
              PCollection<?> pc = (PCollection<?>) entry.getValue();
              return Graphs.Tag.of(pc.getName(), entry.getKey(), pc.getCoder());
            }})
          .toList();
    }

    public TupleTag<?> getOnlyOutputTag() {
      return Iterables.getOnlyElement(currentNode.getOutputs().keySet());
    }
  }
}
