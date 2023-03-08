package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_WatermarkEstimatorParameter extends DoFnSignature.Parameter.WatermarkEstimatorParameter {

  private final TypeDescriptor<?> estimatorT;

  AutoValue_DoFnSignature_Parameter_WatermarkEstimatorParameter(
      TypeDescriptor<?> estimatorT) {
    if (estimatorT == null) {
      throw new NullPointerException("Null estimatorT");
    }
    this.estimatorT = estimatorT;
  }

  @Override
  public TypeDescriptor<?> estimatorT() {
    return estimatorT;
  }

  @Override
  public String toString() {
    return "WatermarkEstimatorParameter{"
        + "estimatorT=" + estimatorT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.WatermarkEstimatorParameter) {
      DoFnSignature.Parameter.WatermarkEstimatorParameter that = (DoFnSignature.Parameter.WatermarkEstimatorParameter) o;
      return this.estimatorT.equals(that.estimatorT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= estimatorT.hashCode();
    return h$;
  }

}
