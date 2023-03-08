package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GreedyPipelineFuser_SiblingKey extends GreedyPipelineFuser.SiblingKey {

  private final PipelineNode.PCollectionNode inputCollection;

  private final RunnerApi.Environment env;

  AutoValue_GreedyPipelineFuser_SiblingKey(
      PipelineNode.PCollectionNode inputCollection,
      RunnerApi.Environment env) {
    if (inputCollection == null) {
      throw new NullPointerException("Null inputCollection");
    }
    this.inputCollection = inputCollection;
    if (env == null) {
      throw new NullPointerException("Null env");
    }
    this.env = env;
  }

  @Override
  PipelineNode.PCollectionNode getInputCollection() {
    return inputCollection;
  }

  @Override
  RunnerApi.Environment getEnv() {
    return env;
  }

  @Override
  public String toString() {
    return "SiblingKey{"
        + "inputCollection=" + inputCollection + ", "
        + "env=" + env
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GreedyPipelineFuser.SiblingKey) {
      GreedyPipelineFuser.SiblingKey that = (GreedyPipelineFuser.SiblingKey) o;
      return this.inputCollection.equals(that.getInputCollection())
          && this.env.equals(that.getEnv());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= inputCollection.hashCode();
    h$ *= 1000003;
    h$ ^= env.hashCode();
    return h$;
  }

}
