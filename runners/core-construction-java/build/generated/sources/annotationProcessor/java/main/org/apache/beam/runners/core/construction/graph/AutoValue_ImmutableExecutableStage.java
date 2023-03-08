package org.apache.beam.runners.core.construction.graph;

import java.util.Collection;
import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ImmutableExecutableStage extends ImmutableExecutableStage {

  private final RunnerApi.Components components;

  private final RunnerApi.Environment environment;

  private final PipelineNode.PCollectionNode inputPCollection;

  private final Collection<SideInputReference> sideInputs;

  private final Collection<UserStateReference> userStates;

  private final Collection<TimerReference> timers;

  private final Collection<PipelineNode.PTransformNode> transforms;

  private final Collection<PipelineNode.PCollectionNode> outputPCollections;

  private final Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting> wireCoderSettings;

  AutoValue_ImmutableExecutableStage(
      RunnerApi.Components components,
      RunnerApi.Environment environment,
      PipelineNode.PCollectionNode inputPCollection,
      Collection<SideInputReference> sideInputs,
      Collection<UserStateReference> userStates,
      Collection<TimerReference> timers,
      Collection<PipelineNode.PTransformNode> transforms,
      Collection<PipelineNode.PCollectionNode> outputPCollections,
      Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting> wireCoderSettings) {
    if (components == null) {
      throw new NullPointerException("Null components");
    }
    this.components = components;
    if (environment == null) {
      throw new NullPointerException("Null environment");
    }
    this.environment = environment;
    if (inputPCollection == null) {
      throw new NullPointerException("Null inputPCollection");
    }
    this.inputPCollection = inputPCollection;
    if (sideInputs == null) {
      throw new NullPointerException("Null sideInputs");
    }
    this.sideInputs = sideInputs;
    if (userStates == null) {
      throw new NullPointerException("Null userStates");
    }
    this.userStates = userStates;
    if (timers == null) {
      throw new NullPointerException("Null timers");
    }
    this.timers = timers;
    if (transforms == null) {
      throw new NullPointerException("Null transforms");
    }
    this.transforms = transforms;
    if (outputPCollections == null) {
      throw new NullPointerException("Null outputPCollections");
    }
    this.outputPCollections = outputPCollections;
    if (wireCoderSettings == null) {
      throw new NullPointerException("Null wireCoderSettings");
    }
    this.wireCoderSettings = wireCoderSettings;
  }

  @Override
  public RunnerApi.Components getComponents() {
    return components;
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public PipelineNode.PCollectionNode getInputPCollection() {
    return inputPCollection;
  }

  @Override
  public Collection<SideInputReference> getSideInputs() {
    return sideInputs;
  }

  @Override
  public Collection<UserStateReference> getUserStates() {
    return userStates;
  }

  @Override
  public Collection<TimerReference> getTimers() {
    return timers;
  }

  @Override
  public Collection<PipelineNode.PTransformNode> getTransforms() {
    return transforms;
  }

  @Override
  public Collection<PipelineNode.PCollectionNode> getOutputPCollections() {
    return outputPCollections;
  }

  @Override
  public Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting> getWireCoderSettings() {
    return wireCoderSettings;
  }

  @Override
  public String toString() {
    return "ImmutableExecutableStage{"
        + "components=" + components + ", "
        + "environment=" + environment + ", "
        + "inputPCollection=" + inputPCollection + ", "
        + "sideInputs=" + sideInputs + ", "
        + "userStates=" + userStates + ", "
        + "timers=" + timers + ", "
        + "transforms=" + transforms + ", "
        + "outputPCollections=" + outputPCollections + ", "
        + "wireCoderSettings=" + wireCoderSettings
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ImmutableExecutableStage) {
      ImmutableExecutableStage that = (ImmutableExecutableStage) o;
      return this.components.equals(that.getComponents())
          && this.environment.equals(that.getEnvironment())
          && this.inputPCollection.equals(that.getInputPCollection())
          && this.sideInputs.equals(that.getSideInputs())
          && this.userStates.equals(that.getUserStates())
          && this.timers.equals(that.getTimers())
          && this.transforms.equals(that.getTransforms())
          && this.outputPCollections.equals(that.getOutputPCollections())
          && this.wireCoderSettings.equals(that.getWireCoderSettings());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= components.hashCode();
    h$ *= 1000003;
    h$ ^= environment.hashCode();
    h$ *= 1000003;
    h$ ^= inputPCollection.hashCode();
    h$ *= 1000003;
    h$ ^= sideInputs.hashCode();
    h$ *= 1000003;
    h$ ^= userStates.hashCode();
    h$ *= 1000003;
    h$ ^= timers.hashCode();
    h$ *= 1000003;
    h$ ^= transforms.hashCode();
    h$ *= 1000003;
    h$ ^= outputPCollections.hashCode();
    h$ *= 1000003;
    h$ ^= wireCoderSettings.hashCode();
    return h$;
  }

}
