package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_RestrictionParameter extends DoFnSignature.Parameter.RestrictionParameter {

  private final TypeDescriptor<?> restrictionT;

  AutoValue_DoFnSignature_Parameter_RestrictionParameter(
      TypeDescriptor<?> restrictionT) {
    if (restrictionT == null) {
      throw new NullPointerException("Null restrictionT");
    }
    this.restrictionT = restrictionT;
  }

  @Override
  public TypeDescriptor<?> restrictionT() {
    return restrictionT;
  }

  @Override
  public String toString() {
    return "RestrictionParameter{"
        + "restrictionT=" + restrictionT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.RestrictionParameter) {
      DoFnSignature.Parameter.RestrictionParameter that = (DoFnSignature.Parameter.RestrictionParameter) o;
      return this.restrictionT.equals(that.restrictionT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= restrictionT.hashCode();
    return h$;
  }

}
