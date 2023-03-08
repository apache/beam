package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AddFields_Inner_NewField extends AddFields.Inner.NewField {

  private final String name;

  private final FieldAccessDescriptor descriptor;

  private final Schema.FieldType fieldType;

  private final @Nullable Object defaultValue;

  private AutoValue_AddFields_Inner_NewField(
      String name,
      FieldAccessDescriptor descriptor,
      Schema.FieldType fieldType,
      @Nullable Object defaultValue) {
    this.name = name;
    this.descriptor = descriptor;
    this.fieldType = fieldType;
    this.defaultValue = defaultValue;
  }

  @Override
  String getName() {
    return name;
  }

  @Override
  FieldAccessDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  Schema.FieldType getFieldType() {
    return fieldType;
  }

  @Override
  @Nullable Object getDefaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return "NewField{"
        + "name=" + name + ", "
        + "descriptor=" + descriptor + ", "
        + "fieldType=" + fieldType + ", "
        + "defaultValue=" + defaultValue
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AddFields.Inner.NewField) {
      AddFields.Inner.NewField that = (AddFields.Inner.NewField) o;
      return this.name.equals(that.getName())
          && this.descriptor.equals(that.getDescriptor())
          && this.fieldType.equals(that.getFieldType())
          && (this.defaultValue == null ? that.getDefaultValue() == null : this.defaultValue.equals(that.getDefaultValue()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= name.hashCode();
    h$ *= 1000003;
    h$ ^= descriptor.hashCode();
    h$ *= 1000003;
    h$ ^= fieldType.hashCode();
    h$ *= 1000003;
    h$ ^= (defaultValue == null) ? 0 : defaultValue.hashCode();
    return h$;
  }

  @Override
  AddFields.Inner.NewField.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends AddFields.Inner.NewField.Builder {
    private String name;
    private FieldAccessDescriptor descriptor;
    private Schema.FieldType fieldType;
    private @Nullable Object defaultValue;
    Builder() {
    }
    private Builder(AddFields.Inner.NewField source) {
      this.name = source.getName();
      this.descriptor = source.getDescriptor();
      this.fieldType = source.getFieldType();
      this.defaultValue = source.getDefaultValue();
    }
    @Override
    AddFields.Inner.NewField.Builder setName(String name) {
      if (name == null) {
        throw new NullPointerException("Null name");
      }
      this.name = name;
      return this;
    }
    @Override
    AddFields.Inner.NewField.Builder setDescriptor(FieldAccessDescriptor descriptor) {
      if (descriptor == null) {
        throw new NullPointerException("Null descriptor");
      }
      this.descriptor = descriptor;
      return this;
    }
    @Override
    AddFields.Inner.NewField.Builder setFieldType(Schema.FieldType fieldType) {
      if (fieldType == null) {
        throw new NullPointerException("Null fieldType");
      }
      this.fieldType = fieldType;
      return this;
    }
    @Override
    AddFields.Inner.NewField.Builder setDefaultValue(@Nullable Object defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }
    @Override
    AddFields.Inner.NewField build() {
      if (this.name == null
          || this.descriptor == null
          || this.fieldType == null) {
        StringBuilder missing = new StringBuilder();
        if (this.name == null) {
          missing.append(" name");
        }
        if (this.descriptor == null) {
          missing.append(" descriptor");
        }
        if (this.fieldType == null) {
          missing.append(" fieldType");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AddFields_Inner_NewField(
          this.name,
          this.descriptor,
          this.fieldType,
          this.defaultValue);
    }
  }

}
