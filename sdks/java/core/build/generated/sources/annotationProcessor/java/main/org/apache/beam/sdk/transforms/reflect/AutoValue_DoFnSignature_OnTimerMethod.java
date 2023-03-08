package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_OnTimerMethod extends DoFnSignature.OnTimerMethod {

  private final String id;

  private final Method targetMethod;

  private final boolean requiresStableInput;

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  private final List<DoFnSignature.Parameter> extraParameters;

  AutoValue_DoFnSignature_OnTimerMethod(
      String id,
      Method targetMethod,
      boolean requiresStableInput,
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
      List<DoFnSignature.Parameter> extraParameters) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    this.requiresStableInput = requiresStableInput;
    this.windowT = windowT;
    if (extraParameters == null) {
      throw new NullPointerException("Null extraParameters");
    }
    this.extraParameters = extraParameters;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Method targetMethod() {
    return targetMethod;
  }

  @Override
  public boolean requiresStableInput() {
    return requiresStableInput;
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
    return "OnTimerMethod{"
        + "id=" + id + ", "
        + "targetMethod=" + targetMethod + ", "
        + "requiresStableInput=" + requiresStableInput + ", "
        + "windowT=" + windowT + ", "
        + "extraParameters=" + extraParameters
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.OnTimerMethod) {
      DoFnSignature.OnTimerMethod that = (DoFnSignature.OnTimerMethod) o;
      return this.id.equals(that.id())
          && this.targetMethod.equals(that.targetMethod())
          && this.requiresStableInput == that.requiresStableInput()
          && (this.windowT == null ? that.windowT() == null : this.windowT.equals(that.windowT()))
          && this.extraParameters.equals(that.extraParameters());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= id.hashCode();
    h$ *= 1000003;
    h$ ^= targetMethod.hashCode();
    h$ *= 1000003;
    h$ ^= requiresStableInput ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    h$ *= 1000003;
    h$ ^= extraParameters.hashCode();
    return h$;
  }

}
