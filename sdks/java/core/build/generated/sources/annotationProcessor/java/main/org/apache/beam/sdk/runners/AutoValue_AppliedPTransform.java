package org.apache.beam.sdk.runners;

import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AppliedPTransform<InputT extends PInput, OutputT extends POutput, TransformT extends PTransform<? super InputT, OutputT>> extends AppliedPTransform<InputT, OutputT, TransformT> {

  private final String fullName;

  private final Map<TupleTag<?>, PCollection<?>> inputs;

  private final Map<TupleTag<?>, PCollection<?>> outputs;

  private final TransformT transform;

  private final ResourceHints resourceHints;

  private final Pipeline pipeline;

  AutoValue_AppliedPTransform(
      String fullName,
      Map<TupleTag<?>, PCollection<?>> inputs,
      Map<TupleTag<?>, PCollection<?>> outputs,
      TransformT transform,
      ResourceHints resourceHints,
      Pipeline pipeline) {
    if (fullName == null) {
      throw new NullPointerException("Null fullName");
    }
    this.fullName = fullName;
    if (inputs == null) {
      throw new NullPointerException("Null inputs");
    }
    this.inputs = inputs;
    if (outputs == null) {
      throw new NullPointerException("Null outputs");
    }
    this.outputs = outputs;
    if (transform == null) {
      throw new NullPointerException("Null transform");
    }
    this.transform = transform;
    if (resourceHints == null) {
      throw new NullPointerException("Null resourceHints");
    }
    this.resourceHints = resourceHints;
    if (pipeline == null) {
      throw new NullPointerException("Null pipeline");
    }
    this.pipeline = pipeline;
  }

  @Override
  public String getFullName() {
    return fullName;
  }

  @Override
  public Map<TupleTag<?>, PCollection<?>> getInputs() {
    return inputs;
  }

  @Override
  public Map<TupleTag<?>, PCollection<?>> getOutputs() {
    return outputs;
  }

  @Override
  public TransformT getTransform() {
    return transform;
  }

  @Override
  public ResourceHints getResourceHints() {
    return resourceHints;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public String toString() {
    return "AppliedPTransform{"
        + "fullName=" + fullName + ", "
        + "inputs=" + inputs + ", "
        + "outputs=" + outputs + ", "
        + "transform=" + transform + ", "
        + "resourceHints=" + resourceHints + ", "
        + "pipeline=" + pipeline
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AppliedPTransform) {
      AppliedPTransform<?, ?, ?> that = (AppliedPTransform<?, ?, ?>) o;
      return this.fullName.equals(that.getFullName())
          && this.inputs.equals(that.getInputs())
          && this.outputs.equals(that.getOutputs())
          && this.transform.equals(that.getTransform())
          && this.resourceHints.equals(that.getResourceHints())
          && this.pipeline.equals(that.getPipeline());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fullName.hashCode();
    h$ *= 1000003;
    h$ ^= inputs.hashCode();
    h$ *= 1000003;
    h$ ^= outputs.hashCode();
    h$ *= 1000003;
    h$ ^= transform.hashCode();
    h$ *= 1000003;
    h$ ^= resourceHints.hashCode();
    h$ *= 1000003;
    h$ ^= pipeline.hashCode();
    return h$;
  }

}
