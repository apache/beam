package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_WatermarkEstimatorStateParameter extends DoFnSignature.Parameter.WatermarkEstimatorStateParameter {

  private final TypeDescriptor<?> estimatorStateT;

  AutoValue_DoFnSignature_Parameter_WatermarkEstimatorStateParameter(
      TypeDescriptor<?> estimatorStateT) {
    if (estimatorStateT == null) {
      throw new NullPointerException("Null estimatorStateT");
    }
    this.estimatorStateT = estimatorStateT;
  }

  @Override
  public TypeDescriptor<?> estimatorStateT() {
    return estimatorStateT;
  }

  @Override
  public String toString() {
    return "WatermarkEstimatorStateParameter{"
        + "estimatorStateT=" + estimatorStateT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.WatermarkEstimatorStateParameter) {
      DoFnSignature.Parameter.WatermarkEstimatorStateParameter that = (DoFnSignature.Parameter.WatermarkEstimatorStateParameter) o;
      return this.estimatorStateT.equals(that.estimatorStateT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= estimatorStateT.hashCode();
    return h$;
  }

}
