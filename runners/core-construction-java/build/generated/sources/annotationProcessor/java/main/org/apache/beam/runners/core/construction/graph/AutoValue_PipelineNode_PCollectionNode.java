package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PipelineNode_PCollectionNode extends PipelineNode.PCollectionNode {

  private final String id;

  private final RunnerApi.PCollection PCollection;

  AutoValue_PipelineNode_PCollectionNode(
      String id,
      RunnerApi.PCollection PCollection) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (PCollection == null) {
      throw new NullPointerException("Null PCollection");
    }
    this.PCollection = PCollection;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public RunnerApi.PCollection getPCollection() {
    return PCollection;
  }

  @Override
  public String toString() {
    return "PCollectionNode{"
        + "id=" + id + ", "
        + "PCollection=" + PCollection
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PipelineNode.PCollectionNode) {
      PipelineNode.PCollectionNode that = (PipelineNode.PCollectionNode) o;
      return this.id.equals(that.getId())
          && this.PCollection.equals(that.getPCollection());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= id.hashCode();
    h$ *= 1000003;
    h$ ^= PCollection.hashCode();
    return h$;
  }

}
