package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_GetWatermarkEstimatorStateCoderMethod extends DoFnSignature.GetWatermarkEstimatorStateCoderMethod {

  private final Method targetMethod;

  private final TypeDescriptor<?> coderT;

  AutoValue_DoFnSignature_GetWatermarkEstimatorStateCoderMethod(
      Method targetMethod,
      TypeDescriptor<?> coderT) {
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (coderT == null) {
      throw new NullPointerException("Null coderT");
    }
    this.coderT = coderT;
  }

  @Override
  public Method targetMethod() {
    return targetMethod;
  }

  @Override
  public TypeDescriptor<?> coderT() {
    return coderT;
  }

  @Override
  public String toString() {
    return "GetWatermarkEstimatorStateCoderMethod{"
        + "targetMethod=" + targetMethod + ", "
        + "coderT=" + coderT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.GetWatermarkEstimatorStateCoderMethod) {
      DoFnSignature.GetWatermarkEstimatorStateCoderMethod that = (DoFnSignature.GetWatermarkEstimatorStateCoderMethod) o;
      return this.targetMethod.equals(that.targetMethod())
          && this.coderT.equals(that.coderT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= targetMethod.hashCode();
    h$ *= 1000003;
    h$ ^= coderT.hashCode();
    return h$;
  }

}
