package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimerReference extends TimerReference {

  private final PipelineNode.PTransformNode transform;

  private final String localName;

  AutoValue_TimerReference(
      PipelineNode.PTransformNode transform,
      String localName) {
    if (transform == null) {
      throw new NullPointerException("Null transform");
    }
    this.transform = transform;
    if (localName == null) {
      throw new NullPointerException("Null localName");
    }
    this.localName = localName;
  }

  @Override
  public PipelineNode.PTransformNode transform() {
    return transform;
  }

  @Override
  public String localName() {
    return localName;
  }

  @Override
  public String toString() {
    return "TimerReference{"
        + "transform=" + transform + ", "
        + "localName=" + localName
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimerReference) {
      TimerReference that = (TimerReference) o;
      return this.transform.equals(that.transform())
          && this.localName.equals(that.localName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= transform.hashCode();
    h$ *= 1000003;
    h$ ^= localName.hashCode();
    return h$;
  }

}
