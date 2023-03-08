package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_NewWatermarkEstimatorMethod extends DoFnSignature.NewWatermarkEstimatorMethod {

  private final Method targetMethod;

  private final TypeDescriptor<?> watermarkEstimatorT;

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  private final List<DoFnSignature.Parameter> extraParameters;

  AutoValue_DoFnSignature_NewWatermarkEstimatorMethod(
      Method targetMethod,
      TypeDescriptor<?> watermarkEstimatorT,
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
      List<DoFnSignature.Parameter> extraParameters) {
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (watermarkEstimatorT == null) {
      throw new NullPointerException("Null watermarkEstimatorT");
    }
    this.watermarkEstimatorT = watermarkEstimatorT;
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
  public TypeDescriptor<?> watermarkEstimatorT() {
    return watermarkEstimatorT;
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
    return "NewWatermarkEstimatorMethod{"
        + "targetMethod=" + targetMethod + ", "
        + "watermarkEstimatorT=" + watermarkEstimatorT + ", "
        + "windowT=" + windowT + ", "
        + "extraParameters=" + extraParameters
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.NewWatermarkEstimatorMethod) {
      DoFnSignature.NewWatermarkEstimatorMethod that = (DoFnSignature.NewWatermarkEstimatorMethod) o;
      return this.targetMethod.equals(that.targetMethod())
          && this.watermarkEstimatorT.equals(that.watermarkEstimatorT())
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
    h$ ^= watermarkEstimatorT.hashCode();
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    h$ *= 1000003;
    h$ ^= extraParameters.hashCode();
    return h$;
  }

}
