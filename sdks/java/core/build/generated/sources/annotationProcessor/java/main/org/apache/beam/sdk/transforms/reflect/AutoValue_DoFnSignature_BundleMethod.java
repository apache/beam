package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_BundleMethod extends DoFnSignature.BundleMethod {

  private final Method targetMethod;

  private final List<DoFnSignature.Parameter> extraParameters;

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  AutoValue_DoFnSignature_BundleMethod(
      Method targetMethod,
      List<DoFnSignature.Parameter> extraParameters,
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT) {
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (extraParameters == null) {
      throw new NullPointerException("Null extraParameters");
    }
    this.extraParameters = extraParameters;
    this.windowT = windowT;
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
  public @Nullable TypeDescriptor<? extends BoundedWindow> windowT() {
    return windowT;
  }

  @Override
  public String toString() {
    return "BundleMethod{"
        + "targetMethod=" + targetMethod + ", "
        + "extraParameters=" + extraParameters + ", "
        + "windowT=" + windowT
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.BundleMethod) {
      DoFnSignature.BundleMethod that = (DoFnSignature.BundleMethod) o;
      return this.targetMethod.equals(that.targetMethod())
          && this.extraParameters.equals(that.extraParameters())
          && (this.windowT == null ? that.windowT() == null : this.windowT.equals(that.windowT()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= targetMethod.hashCode();
    h$ *= 1000003;
    h$ ^= extraParameters.hashCode();
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    return h$;
  }

}
