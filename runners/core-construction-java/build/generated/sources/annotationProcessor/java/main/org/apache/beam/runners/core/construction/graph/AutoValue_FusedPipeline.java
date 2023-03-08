package org.apache.beam.runners.core.construction.graph;

import java.util.Set;
import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FusedPipeline extends FusedPipeline {

  private final RunnerApi.Components components;

  private final Set<ExecutableStage> fusedStages;

  private final Set<PipelineNode.PTransformNode> runnerExecutedTransforms;

  private final Set<String> requirements;

  AutoValue_FusedPipeline(
      RunnerApi.Components components,
      Set<ExecutableStage> fusedStages,
      Set<PipelineNode.PTransformNode> runnerExecutedTransforms,
      Set<String> requirements) {
    if (components == null) {
      throw new NullPointerException("Null components");
    }
    this.components = components;
    if (fusedStages == null) {
      throw new NullPointerException("Null fusedStages");
    }
    this.fusedStages = fusedStages;
    if (runnerExecutedTransforms == null) {
      throw new NullPointerException("Null runnerExecutedTransforms");
    }
    this.runnerExecutedTransforms = runnerExecutedTransforms;
    if (requirements == null) {
      throw new NullPointerException("Null requirements");
    }
    this.requirements = requirements;
  }

  @Override
  RunnerApi.Components getComponents() {
    return components;
  }

  @Override
  public Set<ExecutableStage> getFusedStages() {
    return fusedStages;
  }

  @Override
  public Set<PipelineNode.PTransformNode> getRunnerExecutedTransforms() {
    return runnerExecutedTransforms;
  }

  @Override
  public Set<String> getRequirements() {
    return requirements;
  }

  @Override
  public String toString() {
    return "FusedPipeline{"
        + "components=" + components + ", "
        + "fusedStages=" + fusedStages + ", "
        + "runnerExecutedTransforms=" + runnerExecutedTransforms + ", "
        + "requirements=" + requirements
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FusedPipeline) {
      FusedPipeline that = (FusedPipeline) o;
      return this.components.equals(that.getComponents())
          && this.fusedStages.equals(that.getFusedStages())
          && this.runnerExecutedTransforms.equals(that.getRunnerExecutedTransforms())
          && this.requirements.equals(that.getRequirements());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= components.hashCode();
    h$ *= 1000003;
    h$ ^= fusedStages.hashCode();
    h$ *= 1000003;
    h$ ^= runnerExecutedTransforms.hashCode();
    h$ *= 1000003;
    h$ ^= requirements.hashCode();
    return h$;
  }

}
