package org.apache.beam.runners.core.construction.graph;

import java.util.NavigableSet;
import java.util.Set;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GreedyPipelineFuser_DescendantConsumers extends GreedyPipelineFuser.DescendantConsumers {

  private final Set<PipelineNode.PTransformNode> unfusedNodes;

  private final NavigableSet<GreedyPipelineFuser.CollectionConsumer> fusibleConsumers;

  AutoValue_GreedyPipelineFuser_DescendantConsumers(
      Set<PipelineNode.PTransformNode> unfusedNodes,
      NavigableSet<GreedyPipelineFuser.CollectionConsumer> fusibleConsumers) {
    if (unfusedNodes == null) {
      throw new NullPointerException("Null unfusedNodes");
    }
    this.unfusedNodes = unfusedNodes;
    if (fusibleConsumers == null) {
      throw new NullPointerException("Null fusibleConsumers");
    }
    this.fusibleConsumers = fusibleConsumers;
  }

  @Override
  Set<PipelineNode.PTransformNode> getUnfusedNodes() {
    return unfusedNodes;
  }

  @Override
  NavigableSet<GreedyPipelineFuser.CollectionConsumer> getFusibleConsumers() {
    return fusibleConsumers;
  }

  @Override
  public String toString() {
    return "DescendantConsumers{"
        + "unfusedNodes=" + unfusedNodes + ", "
        + "fusibleConsumers=" + fusibleConsumers
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GreedyPipelineFuser.DescendantConsumers) {
      GreedyPipelineFuser.DescendantConsumers that = (GreedyPipelineFuser.DescendantConsumers) o;
      return this.unfusedNodes.equals(that.getUnfusedNodes())
          && this.fusibleConsumers.equals(that.getFusibleConsumers());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= unfusedNodes.hashCode();
    h$ *= 1000003;
    h$ ^= fusibleConsumers.hashCode();
    return h$;
  }

}
