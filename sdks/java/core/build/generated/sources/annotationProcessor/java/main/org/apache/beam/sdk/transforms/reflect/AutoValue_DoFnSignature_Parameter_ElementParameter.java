package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_ElementParameter extends DoFnSignature.Parameter.ElementParameter {

  private final TypeDescriptor<?> elementT;

  AutoValue_DoFnSignature_Parameter_ElementParameter(
      TypeDescriptor<?> elementT) {
    if (elementT == null) {
      throw new NullPointerException("Null elementT");
    }
    this.elementT = elementT;
  }

  @Override
  public TypeDescriptor<?> elementT() {
    return elementT;
  }

  @Override
  public String toString() {
    return "ElementParameter{"
        + "elementT=" + elementT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.ElementParameter) {
      DoFnSignature.Parameter.ElementParameter that = (DoFnSignature.Parameter.ElementParameter) o;
      return this.elementT.equals(that.elementT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= elementT.hashCode();
    return h$;
  }

}
