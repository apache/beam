package org.apache.beam.sdk.transforms.reflect;

import java.lang.reflect.Field;
import javax.annotation.Generated;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_StateDeclaration extends DoFnSignature.StateDeclaration {

  private final String id;

  private final Field field;

  private final TypeDescriptor<? extends State> stateType;

  AutoValue_DoFnSignature_StateDeclaration(
      String id,
      Field field,
      TypeDescriptor<? extends State> stateType) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (field == null) {
      throw new NullPointerException("Null field");
    }
    this.field = field;
    if (stateType == null) {
      throw new NullPointerException("Null stateType");
    }
    this.stateType = stateType;
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
  public TypeDescriptor<? extends State> stateType() {
    return stateType;
  }

  @Override
  public String toString() {
    return "StateDeclaration{"
        + "id=" + id + ", "
        + "field=" + field + ", "
        + "stateType=" + stateType
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.StateDeclaration) {
      DoFnSignature.StateDeclaration that = (DoFnSignature.StateDeclaration) o;
      return this.id.equals(that.id())
          && this.field.equals(that.field())
          && this.stateType.equals(that.stateType());
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
    h$ *= 1000003;
    h$ ^= stateType.hashCode();
    return h$;
  }

}
