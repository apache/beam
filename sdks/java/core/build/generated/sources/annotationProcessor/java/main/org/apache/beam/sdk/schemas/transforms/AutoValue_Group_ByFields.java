package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Group_ByFields<InputT> extends Group.ByFields<InputT> {

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private final String keyField;

  private final String valueField;

  private AutoValue_Group_ByFields(
      FieldAccessDescriptor fieldAccessDescriptor,
      String keyField,
      String valueField) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
    this.keyField = keyField;
    this.valueField = valueField;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  String getKeyField() {
    return keyField;
  }

  @Override
  String getValueField() {
    return valueField;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Group.ByFields) {
      Group.ByFields<?> that = (Group.ByFields<?>) o;
      return this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor())
          && this.keyField.equals(that.getKeyField())
          && this.valueField.equals(that.getValueField());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= keyField.hashCode();
    h$ *= 1000003;
    h$ ^= valueField.hashCode();
    return h$;
  }

  @Override
  Group.ByFields.Builder<InputT> toBuilder() {
    return new Builder<InputT>(this);
  }

  static final class Builder<InputT> extends Group.ByFields.Builder<InputT> {
    private FieldAccessDescriptor fieldAccessDescriptor;
    private String keyField;
    private String valueField;
    Builder() {
    }
    private Builder(Group.ByFields<InputT> source) {
      this.fieldAccessDescriptor = source.getFieldAccessDescriptor();
      this.keyField = source.getKeyField();
      this.valueField = source.getValueField();
    }
    @Override
    Group.ByFields.Builder<InputT> setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      if (fieldAccessDescriptor == null) {
        throw new NullPointerException("Null fieldAccessDescriptor");
      }
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      return this;
    }
    @Override
    Group.ByFields.Builder<InputT> setKeyField(String keyField) {
      if (keyField == null) {
        throw new NullPointerException("Null keyField");
      }
      this.keyField = keyField;
      return this;
    }
    @Override
    Group.ByFields.Builder<InputT> setValueField(String valueField) {
      if (valueField == null) {
        throw new NullPointerException("Null valueField");
      }
      this.valueField = valueField;
      return this;
    }
    @Override
    Group.ByFields<InputT> build() {
      if (this.fieldAccessDescriptor == null
          || this.keyField == null
          || this.valueField == null) {
        StringBuilder missing = new StringBuilder();
        if (this.fieldAccessDescriptor == null) {
          missing.append(" fieldAccessDescriptor");
        }
        if (this.keyField == null) {
          missing.append(" keyField");
        }
        if (this.valueField == null) {
          missing.append(" valueField");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Group_ByFields<InputT>(
          this.fieldAccessDescriptor,
          this.keyField,
          this.valueField);
    }
  }

}
