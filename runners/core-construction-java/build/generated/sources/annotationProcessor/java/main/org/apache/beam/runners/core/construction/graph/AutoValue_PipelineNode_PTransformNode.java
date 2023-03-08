package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PipelineNode_PTransformNode extends PipelineNode.PTransformNode {

  private final String id;

  private final RunnerApi.PTransform transform;

  AutoValue_PipelineNode_PTransformNode(
      String id,
      RunnerApi.PTransform transform) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (transform == null) {
      throw new NullPointerException("Null transform");
    }
    this.transform = transform;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public RunnerApi.PTransform getTransform() {
    return transform;
  }

  @Override
  public String toString() {
    return "PTransformNode{"
        + "id=" + id + ", "
        + "transform=" + transform
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PipelineNode.PTransformNode) {
      PipelineNode.PTransformNode that = (PipelineNode.PTransformNode) o;
      return this.id.equals(that.getId())
          && this.transform.equals(that.getTransform());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= id.hashCode();
    h$ *= 1000003;
    h$ ^= transform.hashCode();
    return h$;
  }

}
