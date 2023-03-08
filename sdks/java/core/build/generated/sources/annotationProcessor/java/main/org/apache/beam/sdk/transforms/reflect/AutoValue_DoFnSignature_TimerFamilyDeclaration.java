package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Field;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_TimerFamilyDeclaration extends DoFnSignature.TimerFamilyDeclaration {

  private final String id;

  private final Field field;

  AutoValue_DoFnSignature_TimerFamilyDeclaration(
      String id,
      Field field) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (field == null) {
      throw new NullPointerException("Null field");
    }
    this.field = field;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Field field() {
    return field;
  }

  @Override
  public String toString() {
    return "TimerFamilyDeclaration{"
        + "id=" + id + ", "
        + "field=" + field
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.TimerFamilyDeclaration) {
      DoFnSignature.TimerFamilyDeclaration that = (DoFnSignature.TimerFamilyDeclaration) o;
      return this.id.equals(that.id())
          && this.field.equals(that.field());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= id.hashCode();
    h$ *= 1000003;
    h$ ^= field.hashCode();
    return h$;
  }

}
