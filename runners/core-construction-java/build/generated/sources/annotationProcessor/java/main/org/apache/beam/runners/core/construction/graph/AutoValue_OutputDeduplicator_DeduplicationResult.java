package org.apache.beam.runners.core.construction.graph;

import java.util.Map;
import java.util.Set;
import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_OutputDeduplicator_DeduplicationResult extends OutputDeduplicator.DeduplicationResult {

  private final RunnerApi.Components deduplicatedComponents;

  private final Set<PipelineNode.PTransformNode> introducedTransforms;

  private final Map<ExecutableStage, ExecutableStage> deduplicatedStages;

  private final Map<String, PipelineNode.PTransformNode> deduplicatedTransforms;

  AutoValue_OutputDeduplicator_DeduplicationResult(
      RunnerApi.Components deduplicatedComponents,
      Set<PipelineNode.PTransformNode> introducedTransforms,
      Map<ExecutableStage, ExecutableStage> deduplicatedStages,
      Map<String, PipelineNode.PTransformNode> deduplicatedTransforms) {
    if (deduplicatedComponents == null) {
      throw new NullPointerException("Null deduplicatedComponents");
    }
    this.deduplicatedComponents = deduplicatedComponents;
    if (introducedTransforms == null) {
      throw new NullPointerException("Null introducedTransforms");
    }
    this.introducedTransforms = introducedTransforms;
    if (deduplicatedStages == null) {
      throw new NullPointerException("Null deduplicatedStages");
    }
    this.deduplicatedStages = deduplicatedStages;
    if (deduplicatedTransforms == null) {
      throw new NullPointerException("Null deduplicatedTransforms");
    }
    this.deduplicatedTransforms = deduplicatedTransforms;
  }

  @Override
  RunnerApi.Components getDeduplicatedComponents() {
    return deduplicatedComponents;
  }

  @Override
  Set<PipelineNode.PTransformNode> getIntroducedTransforms() {
    return introducedTransforms;
  }

  @Override
  Map<ExecutableStage, ExecutableStage> getDeduplicatedStages() {
    return deduplicatedStages;
  }

  @Override
  Map<String, PipelineNode.PTransformNode> getDeduplicatedTransforms() {
    return deduplicatedTransforms;
  }

  @Override
  public String toString() {
    return "DeduplicationResult{"
        + "deduplicatedComponents=" + deduplicatedComponents + ", "
        + "introducedTransforms=" + introducedTransforms + ", "
        + "deduplicatedStages=" + deduplicatedStages + ", "
        + "deduplicatedTransforms=" + deduplicatedTransforms
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof OutputDeduplicator.DeduplicationResult) {
      OutputDeduplicator.DeduplicationResult that = (OutputDeduplicator.DeduplicationResult) o;
      return this.deduplicatedComponents.equals(that.getDeduplicatedComponents())
          && this.introducedTransforms.equals(that.getIntroducedTransforms())
          && this.deduplicatedStages.equals(that.getDeduplicatedStages())
          && this.deduplicatedTransforms.equals(that.getDeduplicatedTransforms());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= deduplicatedComponents.hashCode();
    h$ *= 1000003;
    h$ ^= introducedTransforms.hashCode();
    h$ *= 1000003;
    h$ ^= deduplicatedStages.hashCode();
    h$ *= 1000003;
    h$ ^= deduplicatedTransforms.hashCode();
    return h$;
  }

}
