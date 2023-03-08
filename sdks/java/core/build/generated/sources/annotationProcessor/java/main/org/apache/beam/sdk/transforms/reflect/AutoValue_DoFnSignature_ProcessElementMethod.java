package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_ProcessElementMethod extends DoFnSignature.ProcessElementMethod {

  private final Method targetMethod;

  private final List<DoFnSignature.Parameter> extraParameters;

  private final boolean requiresStableInput;

  private final boolean requiresTimeSortedInput;

  private final @Nullable TypeDescriptor<?> trackerT;

  private final @Nullable TypeDescriptor<?> watermarkEstimatorT;

  private final @Nullable TypeDescriptor<? extends BoundedWindow> windowT;

  private final boolean hasReturnValue;

  AutoValue_DoFnSignature_ProcessElementMethod(
      Method targetMethod,
      List<DoFnSignature.Parameter> extraParameters,
      boolean requiresStableInput,
      boolean requiresTimeSortedInput,
      @Nullable TypeDescriptor<?> trackerT,
      @Nullable TypeDescriptor<?> watermarkEstimatorT,
      @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
      boolean hasReturnValue) {
    if (targetMethod == null) {
      throw new NullPointerException("Null targetMethod");
    }
    this.targetMethod = targetMethod;
    if (extraParameters == null) {
      throw new NullPointerException("Null extraParameters");
    }
    this.extraParameters = extraParameters;
    this.requiresStableInput = requiresStableInput;
    this.requiresTimeSortedInput = requiresTimeSortedInput;
    this.trackerT = trackerT;
    this.watermarkEstimatorT = watermarkEstimatorT;
    this.windowT = windowT;
    this.hasReturnValue = hasReturnValue;
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
  public boolean requiresStableInput() {
    return requiresStableInput;
  }

  @Override
  public boolean requiresTimeSortedInput() {
    return requiresTimeSortedInput;
  }

  @Override
  public @Nullable TypeDescriptor<?> trackerT() {
    return trackerT;
  }

  @Override
  public @Nullable TypeDescriptor<?> watermarkEstimatorT() {
    return watermarkEstimatorT;
  }

  @Override
  public @Nullable TypeDescriptor<? extends BoundedWindow> windowT() {
    return windowT;
  }

  @Override
  public boolean hasReturnValue() {
    return hasReturnValue;
  }

  @Override
  public String toString() {
    return "ProcessElementMethod{"
        + "targetMethod=" + targetMethod + ", "
        + "extraParameters=" + extraParameters + ", "
        + "requiresStableInput=" + requiresStableInput + ", "
        + "requiresTimeSortedInput=" + requiresTimeSortedInput + ", "
        + "trackerT=" + trackerT + ", "
        + "watermarkEstimatorT=" + watermarkEstimatorT + ", "
        + "windowT=" + windowT + ", "
        + "hasReturnValue=" + hasReturnValue
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.ProcessElementMethod) {
      DoFnSignature.ProcessElementMethod that = (DoFnSignature.ProcessElementMethod) o;
      return this.targetMethod.equals(that.targetMethod())
          && this.extraParameters.equals(that.extraParameters())
          && this.requiresStableInput == that.requiresStableInput()
          && this.requiresTimeSortedInput == that.requiresTimeSortedInput()
          && (this.trackerT == null ? that.trackerT() == null : this.trackerT.equals(that.trackerT()))
          && (this.watermarkEstimatorT == null ? that.watermarkEstimatorT() == null : this.watermarkEstimatorT.equals(that.watermarkEstimatorT()))
          && (this.windowT == null ? that.windowT() == null : this.windowT.equals(that.windowT()))
          && this.hasReturnValue == that.hasReturnValue();
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
    h$ ^= requiresStableInput ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= requiresTimeSortedInput ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (trackerT == null) ? 0 : trackerT.hashCode();
    h$ *= 1000003;
    h$ ^= (watermarkEstimatorT == null) ? 0 : watermarkEstimatorT.hashCode();
    h$ *= 1000003;
    h$ ^= (windowT == null) ? 0 : windowT.hashCode();
    h$ *= 1000003;
    h$ ^= hasReturnValue ? 1231 : 1237;
    return h$;
  }

}
