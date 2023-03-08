package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GreedyPipelineFuser_CollectionConsumer extends GreedyPipelineFuser.CollectionConsumer {

  private final PipelineNode.PCollectionNode consumedCollection;

  private final PipelineNode.PTransformNode consumingTransform;

  AutoValue_GreedyPipelineFuser_CollectionConsumer(
      PipelineNode.PCollectionNode consumedCollection,
      PipelineNode.PTransformNode consumingTransform) {
    if (consumedCollection == null) {
      throw new NullPointerException("Null consumedCollection");
    }
    this.consumedCollection = consumedCollection;
    if (consumingTransform == null) {
      throw new NullPointerException("Null consumingTransform");
    }
    this.consumingTransform = consumingTransform;
  }

  @Override
  PipelineNode.PCollectionNode consumedCollection() {
    return consumedCollection;
  }

  @Override
  PipelineNode.PTransformNode consumingTransform() {
    return consumingTransform;
  }

  @Override
  public String toString() {
    return "CollectionConsumer{"
        + "consumedCollection=" + consumedCollection + ", "
        + "consumingTransform=" + consumingTransform
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GreedyPipelineFuser.CollectionConsumer) {
      GreedyPipelineFuser.CollectionConsumer that = (GreedyPipelineFuser.CollectionConsumer) o;
      return this.consumedCollection.equals(that.consumedCollection())
          && this.consumingTransform.equals(that.consumingTransform());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= consumedCollection.hashCode();
    h$ *= 1000003;
    h$ ^= consumingTransform.hashCode();
    return h$;
  }

}
