package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SideInputReference extends SideInputReference {

  private final PipelineNode.PTransformNode transform;

  private final String localName;

  private final PipelineNode.PCollectionNode collection;

  AutoValue_SideInputReference(
      PipelineNode.PTransformNode transform,
      String localName,
      PipelineNode.PCollectionNode collection) {
    if (transform == null) {
      throw new NullPointerException("Null transform");
    }
    this.transform = transform;
    if (localName == null) {
      throw new NullPointerException("Null localName");
    }
    this.localName = localName;
    if (collection == null) {
      throw new NullPointerException("Null collection");
    }
    this.collection = collection;
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
  public PipelineNode.PCollectionNode collection() {
    return collection;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SideInputReference) {
      SideInputReference that = (SideInputReference) o;
      return this.transform.equals(that.transform())
          && this.localName.equals(that.localName())
          && this.collection.equals(that.collection());
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
    h$ *= 1000003;
    h$ ^= collection.hashCode();
    return h$;
  }

}
