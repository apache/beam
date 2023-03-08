package org.apache.beam.runners.core.construction.graph;

import java.util.Map;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_OutputDeduplicator_PTransformDeduplication extends OutputDeduplicator.PTransformDeduplication {

  private final PipelineNode.PTransformNode updatedTransform;

  private final Map<String, PipelineNode.PCollectionNode> originalToPartialPCollections;

  AutoValue_OutputDeduplicator_PTransformDeduplication(
      PipelineNode.PTransformNode updatedTransform,
      Map<String, PipelineNode.PCollectionNode> originalToPartialPCollections) {
    if (updatedTransform == null) {
      throw new NullPointerException("Null updatedTransform");
    }
    this.updatedTransform = updatedTransform;
    if (originalToPartialPCollections == null) {
      throw new NullPointerException("Null originalToPartialPCollections");
    }
    this.originalToPartialPCollections = originalToPartialPCollections;
  }

  @Override
  PipelineNode.PTransformNode getUpdatedTransform() {
    return updatedTransform;
  }

  @Override
  Map<String, PipelineNode.PCollectionNode> getOriginalToPartialPCollections() {
    return originalToPartialPCollections;
  }

  @Override
  public String toString() {
    return "PTransformDeduplication{"
        + "updatedTransform=" + updatedTransform + ", "
        + "originalToPartialPCollections=" + originalToPartialPCollections
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof OutputDeduplicator.PTransformDeduplication) {
      OutputDeduplicator.PTransformDeduplication that = (OutputDeduplicator.PTransformDeduplication) o;
      return this.updatedTransform.equals(that.getUpdatedTransform())
          && this.originalToPartialPCollections.equals(that.getOriginalToPartialPCollections());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= updatedTransform.hashCode();
    h$ *= 1000003;
    h$ ^= originalToPartialPCollections.hashCode();
    return h$;
  }

}
