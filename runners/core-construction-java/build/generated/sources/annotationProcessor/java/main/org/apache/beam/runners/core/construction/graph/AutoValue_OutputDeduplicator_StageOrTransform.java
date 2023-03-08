package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_OutputDeduplicator_StageOrTransform extends OutputDeduplicator.StageOrTransform {

  private final @Nullable ExecutableStage stage;

  private final PipelineNode.@Nullable PTransformNode transform;

  AutoValue_OutputDeduplicator_StageOrTransform(
      @Nullable ExecutableStage stage,
      PipelineNode.@Nullable PTransformNode transform) {
    this.stage = stage;
    this.transform = transform;
  }

  @Override
  @Nullable ExecutableStage getStage() {
    return stage;
  }

  @Override
  PipelineNode.@Nullable PTransformNode getTransform() {
    return transform;
  }

  @Override
  public String toString() {
    return "StageOrTransform{"
        + "stage=" + stage + ", "
        + "transform=" + transform
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof OutputDeduplicator.StageOrTransform) {
      OutputDeduplicator.StageOrTransform that = (OutputDeduplicator.StageOrTransform) o;
      return (this.stage == null ? that.getStage() == null : this.stage.equals(that.getStage()))
          && (this.transform == null ? that.getTransform() == null : this.transform.equals(that.getTransform()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (stage == null) ? 0 : stage.hashCode();
    h$ *= 1000003;
    h$ ^= (transform == null) ? 0 : transform.hashCode();
    return h$;
  }

}
