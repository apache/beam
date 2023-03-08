package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_TimerParameter extends DoFnSignature.Parameter.TimerParameter {

  private final DoFnSignature.TimerDeclaration referent;

  AutoValue_DoFnSignature_Parameter_TimerParameter(
      DoFnSignature.TimerDeclaration referent) {
    if (referent == null) {
      throw new NullPointerException("Null referent");
    }
    this.referent = referent;
  }

  @Override
  public DoFnSignature.TimerDeclaration referent() {
    return referent;
  }

  @Override
  public String toString() {
    return "TimerParameter{"
        + "referent=" + referent
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.TimerParameter) {
      DoFnSignature.Parameter.TimerParameter that = (DoFnSignature.Parameter.TimerParameter) o;
      return this.referent.equals(that.referent());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= referent.hashCode();
    return h$;
  }

}
