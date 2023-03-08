package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_KeyParameter extends DoFnSignature.Parameter.KeyParameter {

  private final TypeDescriptor<?> keyT;

  AutoValue_DoFnSignature_Parameter_KeyParameter(
      TypeDescriptor<?> keyT) {
    if (keyT == null) {
      throw new NullPointerException("Null keyT");
    }
    this.keyT = keyT;
  }

  @Override
  public TypeDescriptor<?> keyT() {
    return keyT;
  }

  @Override
  public String toString() {
    return "KeyParameter{"
        + "keyT=" + keyT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.KeyParameter) {
      DoFnSignature.Parameter.KeyParameter that = (DoFnSignature.Parameter.KeyParameter) o;
      return this.keyT.equals(that.keyT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= keyT.hashCode();
    return h$;
  }

}
