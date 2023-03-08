package org.apache.beam.sdk.runners;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PTransformOverrideFactory_PTransformReplacement<InputT extends PInput, OutputT extends POutput> extends PTransformOverrideFactory.PTransformReplacement<InputT, OutputT> {

  private final InputT input;

  private final PTransform<InputT, OutputT> transform;

  AutoValue_PTransformOverrideFactory_PTransformReplacement(
      InputT input,
      PTransform<InputT, OutputT> transform) {
    if (input == null) {
      throw new NullPointerException("Null input");
    }
    this.input = input;
    if (transform == null) {
      throw new NullPointerException("Null transform");
    }
    this.transform = transform;
  }

  @Override
  public InputT getInput() {
    return input;
  }

  @Override
  public PTransform<InputT, OutputT> getTransform() {
    return transform;
  }

  @Override
  public String toString() {
    return "PTransformReplacement{"
        + "input=" + input + ", "
        + "transform=" + transform
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PTransformOverrideFactory.PTransformReplacement) {
      PTransformOverrideFactory.PTransformReplacement<?, ?> that = (PTransformOverrideFactory.PTransformReplacement<?, ?>) o;
      return this.input.equals(that.getInput())
          && this.transform.equals(that.getTransform());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= input.hashCode();
    h$ *= 1000003;
    h$ ^= transform.hashCode();
    return h$;
  }

}
