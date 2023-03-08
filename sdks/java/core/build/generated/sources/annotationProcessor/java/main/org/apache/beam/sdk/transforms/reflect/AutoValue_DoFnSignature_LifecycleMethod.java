package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_LifecycleMethod extends DoFnSignature.LifecycleMethod {

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  private final Method targetMethod;

  private final List<DoFnSignature.Parameter> extraParameters;

  AutoValue_DoFnSignature_LifecycleMethod(
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
      Method targetMethod,
      List<DoFnSignature.Parameter> extraParameters) {
    this.windowT = windowT;
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (extraParameters == null) {
      throw new NullPointerException("Null extraParameters");
    }
    this.extraParameters = extraParameters;
  }

  @Override
  public @Nullable TypeDescriptor<? extends BoundedWindow> windowT() {
    return windowT;
  }

  @Override
  public Method targetMethod() {
    return targetMethod;
  }

  @Override
  public List<DoFnSignature.Parameter> extraParameters() {
    return extraParameters;
  }

  @Override
  public String toString() {
    return "LifecycleMethod{"
        + "windowT=" + windowT + ", "
        + "targetMethod=" + targetMethod + ", "
        + "extraParameters=" + extraParameters
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.LifecycleMethod) {
      DoFnSignature.LifecycleMethod that = (DoFnSignature.LifecycleMethod) o;
      return (this.windowT == null ? that.windowT() == null : this.windowT.equals(that.windowT()))
          && this.targetMethod.equals(that.targetMethod())
          && this.extraParameters.equals(that.extraParameters());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    h$ *= 1000003;
    h$ ^= targetMethod.hashCode();
    h$ *= 1000003;
    h$ ^= extraParameters.hashCode();
    return h$;
  }

}
