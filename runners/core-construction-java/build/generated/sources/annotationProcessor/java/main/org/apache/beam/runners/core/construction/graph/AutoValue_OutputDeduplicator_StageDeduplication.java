package org.apache.beam.runners.core.construction.graph;

import java.util.Map;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_OutputDeduplicator_StageDeduplication extends OutputDeduplicator.StageDeduplication {

  private final ExecutableStage updatedStage;

  private final Map<String, PipelineNode.PCollectionNode> originalToPartialPCollections;

  AutoValue_OutputDeduplicator_StageDeduplication(
      ExecutableStage updatedStage,
      Map<String, PipelineNode.PCollectionNode> originalToPartialPCollections) {
    if (updatedStage == null) {
      throw new NullPointerException("Null updatedStage");
    }
    this.updatedStage = updatedStage;
    if (originalToPartialPCollections == null) {
      throw new NullPointerException("Null originalToPartialPCollections");
    }
    this.originalToPartialPCollections = originalToPartialPCollections;
  }

  @Override
  ExecutableStage getUpdatedStage() {
    return updatedStage;
  }

  @Override
  Map<String, PipelineNode.PCollectionNode> getOriginalToPartialPCollections() {
    return originalToPartialPCollections;
  }

  @Override
  public String toString() {
    return "StageDeduplication{"
        + "updatedStage=" + updatedStage + ", "
        + "originalToPartialPCollections=" + originalToPartialPCollections
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof OutputDeduplicator.StageDeduplication) {
      OutputDeduplicator.StageDeduplication that = (OutputDeduplicator.StageDeduplication) o;
      return this.updatedStage.equals(that.getUpdatedStage())
          && this.originalToPartialPCollections.equals(that.getOriginalToPartialPCollections());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= updatedStage.hashCode();
    h$ *= 1000003;
    h$ ^= originalToPartialPCollections.hashCode();
    return h$;
  }

}
