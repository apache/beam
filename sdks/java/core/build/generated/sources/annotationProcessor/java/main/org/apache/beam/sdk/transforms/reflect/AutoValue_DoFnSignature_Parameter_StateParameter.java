package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_StateParameter extends DoFnSignature.Parameter.StateParameter {

  private final DoFnSignature.StateDeclaration referent;

  private final boolean alwaysFetched;

  AutoValue_DoFnSignature_Parameter_StateParameter(
      DoFnSignature.StateDeclaration referent,
      boolean alwaysFetched) {
    if (referent == null) {
      throw new NullPointerException("Null referent");
    }
    this.referent = referent;
    this.alwaysFetched = alwaysFetched;
  }

  @Override
  public DoFnSignature.StateDeclaration referent() {
    return referent;
  }

  @Override
  public boolean alwaysFetched() {
    return alwaysFetched;
  }

  @Override
  public String toString() {
    return "StateParameter{"
        + "referent=" + referent + ", "
        + "alwaysFetched=" + alwaysFetched
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.StateParameter) {
      DoFnSignature.Parameter.StateParameter that = (DoFnSignature.Parameter.StateParameter) o;
      return this.referent.equals(that.referent())
          && this.alwaysFetched == that.alwaysFetched();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= referent.hashCode();
    h$ *= 1000003;
    h$ ^= alwaysFetched ? 1231 : 1237;
    return h$;
  }

}
