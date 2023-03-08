package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_TimerFamilyParameter extends DoFnSignature.Parameter.TimerFamilyParameter {

  private final DoFnSignature.TimerFamilyDeclaration referent;

  AutoValue_DoFnSignature_Parameter_TimerFamilyParameter(
      DoFnSignature.TimerFamilyDeclaration referent) {
    if (referent == null) {
      throw new NullPointerException("Null referent");
    }
    this.referent = referent;
  }

  @Override
  public DoFnSignature.TimerFamilyDeclaration referent() {
    return referent;
  }

  @Override
  public String toString() {
    return "TimerFamilyParameter{"
        + "referent=" + referent
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.TimerFamilyParameter) {
      DoFnSignature.Parameter.TimerFamilyParameter that = (DoFnSignature.Parameter.TimerFamilyParameter) o;
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
