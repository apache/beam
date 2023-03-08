package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_GetInitialWatermarkEstimatorStateMethod extends DoFnSignature.GetInitialWatermarkEstimatorStateMethod {

  private final Method targetMethod;

  private final TypeDescriptor<?> watermarkEstimatorStateT;

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  private final List<DoFnSignature.Parameter> extraParameters;

  AutoValue_DoFnSignature_GetInitialWatermarkEstimatorStateMethod(
      Method targetMethod,
      TypeDescriptor<?> watermarkEstimatorStateT,
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
      List<DoFnSignature.Parameter> extraParameters) {
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (watermarkEstimatorStateT == null) {
      throw new NullPointerException("Null watermarkEstimatorStateT");
    }
    this.watermarkEstimatorStateT = watermarkEstimatorStateT;
    this.windowT = windowT;
    if (extraParameters == null) {
      throw new NullPointerException("Null extraParameters");
    }
    this.extraParameters = extraParameters;
  }

  @Override
  public Method targetMethod() {
    return targetMethod;
  }

  @Override
  public TypeDescriptor<?> watermarkEstimatorStateT() {
    return watermarkEstimatorStateT;
  }

  @Override
  public @Nullable TypeDescriptor<? extends BoundedWindow> windowT() {
    return windowT;
  }

  @Override
  public List<DoFnSignature.Parameter> extraParameters() {
    return extraParameters;
  }

  @Override
  public String toString() {
    return "GetInitialWatermarkEstimatorStateMethod{"
        + "targetMethod=" + targetMethod + ", "
        + "watermarkEstimatorStateT=" + watermarkEstimatorStateT + ", "
        + "windowT=" + windowT + ", "
        + "extraParameters=" + extraParameters
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.GetInitialWatermarkEstimatorStateMethod) {
      DoFnSignature.GetInitialWatermarkEstimatorStateMethod that = (DoFnSignature.GetInitialWatermarkEstimatorStateMethod) o;
      return this.targetMethod.equals(that.targetMethod())
          && this.watermarkEstimatorStateT.equals(that.watermarkEstimatorStateT())
          && (this.windowT == null ? that.windowT() == null : this.windowT.equals(that.windowT()))
          && this.extraParameters.equals(that.extraParameters());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= targetMethod.hashCode();
    h$ *= 1000003;
    h$ ^= watermarkEstimatorStateT.hashCode();
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    h$ *= 1000003;
    h$ ^= extraParameters.hashCode();
    return h$;
  }

}
