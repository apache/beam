package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_WindowParameter extends DoFnSignature.Parameter.WindowParameter {

  private final TypeDescriptor<? extends BoundedWindow> windowT;

  AutoValue_DoFnSignature_Parameter_WindowParameter(
      TypeDescriptor<? extends BoundedWindow> windowT) {
    if (windowT == null) {
      throw new NullPointerException("Null windowT");
    }
    this.windowT = windowT;
  }

  @Override
  public TypeDescriptor<? extends BoundedWindow> windowT() {
    return windowT;
  }

  @Override
  public String toString() {
    return "WindowParameter{"
        + "windowT=" + windowT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.WindowParameter) {
      DoFnSignature.Parameter.WindowParameter that = (DoFnSignature.Parameter.WindowParameter) o;
      return this.windowT.equals(that.windowT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= windowT.hashCode();
    return h$;
  }

}
