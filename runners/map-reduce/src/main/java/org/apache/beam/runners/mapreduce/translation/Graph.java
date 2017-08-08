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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;

/**
 * Graph that represents a Beam DAG.
 */
public class Graph<StepT extends Graph.AbstractStep, TagT extends Graph.AbstractTag> {

  public final MutableGraph<Vertex> graph;

  public Graph() {
    this.graph = GraphBuilder.directed()
        .allowsSelfLoops(false)
        .nodeOrder(ElementOrder.insertion())
        .build();
  }

  /**
   * Adds {@link StepT} to this {@link Graph}.
   */
  public void addStep(StepT step, List<TagT> inTags, List<TagT> outTags) {
    graph.addNode(step);
    Set<Vertex> nodes = graph.nodes();
    for (TagT tag : inTags) {
      if (!nodes.contains(tag)) {
        graph.addNode(tag);
      }
      graph.putEdge(tag, step);
    }
    for (TagT tag : outTags) {
      if (!nodes.contains(tag)) {
        graph.addNode(tag);
      }
      graph.putEdge(step, tag);
    }
  }

  public void removeStep(StepT step) {
    graph.removeNode(step);
  }

  public void removeTag(TagT tag) {
    graph.removeNode(tag);
  }

  public void addEdge(TagT inTag, StepT step) {
    graph.putEdge(inTag, step);
  }

  public void addEdge(StepT step, TagT outTag) {
    graph.putEdge(step, outTag);
  }

  public void removeEdge(TagT inTag, StepT step) {
    graph.removeEdge(inTag, step);
  }

  public void removeEdge(StepT step, TagT outTag) {
    graph.removeEdge(step, outTag);
  }

  public List<StepT> getSteps() {
    return castToStepList(FluentIterable.from(graph.nodes())
        .filter(new Predicate<Vertex>() {
          @Override
          public boolean apply(Vertex input) {
            return input instanceof AbstractStep;
          }}))
        .toSortedList(new Comparator<StepT>() {
          @Override
          public int compare(StepT left, StepT right) {
            if (left.equals(right)) {
              return 0;
            } else if (com.google.common.graph.Graphs.reachableNodes(graph, left).contains(right)) {
              return -1;
            } else {
              return 1;
            }
          }
        });
  }

  public List<StepT> getStartSteps() {
    return castToStepList(FluentIterable.from(graph.nodes())
        .filter(new Predicate<Vertex>() {
          @Override
          public boolean apply(Vertex input) {
            return input instanceof AbstractStep && graph.inDegree(input) == 0;
          }}))
        .toList();
  }

  public StepT getProducer(TagT tag) {
    if (contains(tag)) {
      return (StepT) Iterables.getOnlyElement(graph.predecessors(tag));
    } else {
      return null;
    }
  }

  public List<StepT> getConsumers(TagT tag) {
    if (contains(tag)) {
      return castToStepList(graph.successors(tag)).toList();
    } else {
      return Collections.emptyList();
    }
  }

  public List<TagT> getInputTags(StepT step) {
    if (contains(step)) {
      return castToTagList(graph.predecessors(step)).toList();
    } else {
      return Collections.emptyList();
    }
  }

  public List<TagT> getOutputTags(StepT step) {
    if (contains(step)) {
      return castToTagList(graph.successors(step)).toList();
    } else {
      return Collections.emptyList();
    }
  }

  private boolean contains(Vertex node) {
    return graph.nodes().contains(node);
  }

  private FluentIterable<StepT> castToStepList(Iterable<Vertex> vertices) {
    return FluentIterable.from(vertices)
        .transform(new Function<Vertex, StepT>() {
          @Override
          public StepT apply(Vertex input) {
            return (StepT) input;
          }});
  }

  private FluentIterable<TagT> castToTagList(Iterable<Vertex> vertices) {
    return FluentIterable.from(vertices)
        .transform(new Function<Vertex, TagT>() {
          @Override
          public TagT apply(Vertex input) {
            return (TagT) input;
          }});
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Graph) {
      Graph other = (Graph) obj;
      return com.google.common.graph.Graphs.equivalent(this.graph, other.graph);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getClass(), graph.nodes());
  }

  /**
   * Vertex interface of this Graph.
   */
  interface Vertex {
  }

  public abstract static class AbstractStep implements Vertex {
  }

  public abstract static class AbstractTag implements Vertex {
  }
}
