package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AddFields_Inner_AddFieldsInformation extends AddFields.Inner.AddFieldsInformation {

  private final Schema.@Nullable FieldType outputFieldType;

  private final List<Object> defaultValues;

  private final List<AddFields.Inner.AddFieldsInformation> nestedNewValues;

  private AutoValue_AddFields_Inner_AddFieldsInformation(
      Schema.@Nullable FieldType outputFieldType,
      List<Object> defaultValues,
      List<AddFields.Inner.AddFieldsInformation> nestedNewValues) {
    this.outputFieldType = outputFieldType;
    this.defaultValues = defaultValues;
    this.nestedNewValues = nestedNewValues;
  }

  @Override
  Schema.@Nullable FieldType getOutputFieldType() {
    return outputFieldType;
  }

  @Override
  List<Object> getDefaultValues() {
    return defaultValues;
  }

  @Override
  List<AddFields.Inner.AddFieldsInformation> getNestedNewValues() {
    return nestedNewValues;
  }

  @Override
  public String toString() {
    return "AddFieldsInformation{"
        + "outputFieldType=" + outputFieldType + ", "
        + "defaultValues=" + defaultValues + ", "
        + "nestedNewValues=" + nestedNewValues
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AddFields.Inner.AddFieldsInformation) {
      AddFields.Inner.AddFieldsInformation that = (AddFields.Inner.AddFieldsInformation) o;
      return (this.outputFieldType == null ? that.getOutputFieldType() == null : this.outputFieldType.equals(that.getOutputFieldType()))
          && this.defaultValues.equals(that.getDefaultValues())
          && this.nestedNewValues.equals(that.getNestedNewValues());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (outputFieldType == null) ? 0 : outputFieldType.hashCode();
    h$ *= 1000003;
    h$ ^= defaultValues.hashCode();
    h$ *= 1000003;
    h$ ^= nestedNewValues.hashCode();
    return h$;
  }

  @Override
  AddFields.Inner.AddFieldsInformation.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends AddFields.Inner.AddFieldsInformation.Builder {
    private Schema.@Nullable FieldType outputFieldType;
    private List<Object> defaultValues;
    private List<AddFields.Inner.AddFieldsInformation> nestedNewValues;
    Builder() {
    }
    private Builder(AddFields.Inner.AddFieldsInformation source) {
      this.outputFieldType = source.getOutputFieldType();
      this.defaultValues = source.getDefaultValues();
      this.nestedNewValues = source.getNestedNewValues();
    }
    @Override
    AddFields.Inner.AddFieldsInformation.Builder setOutputFieldType(Schema.FieldType outputFieldType) {
      this.outputFieldType = outputFieldType;
      return this;
    }
    @Override
    AddFields.Inner.AddFieldsInformation.Builder setDefaultValues(List<Object> defaultValues) {
      if (defaultValues == null) {
        throw new NullPointerException("Null defaultValues");
      }
      this.defaultValues = defaultValues;
      return this;
    }
    @Override
    AddFields.Inner.AddFieldsInformation.Builder setNestedNewValues(List<AddFields.Inner.AddFieldsInformation> nestedNewValues) {
      if (nestedNewValues == null) {
        throw new NullPointerException("Null nestedNewValues");
      }
      this.nestedNewValues = nestedNewValues;
      return this;
    }
    @Override
    AddFields.Inner.AddFieldsInformation build() {
      if (this.defaultValues == null
          || this.nestedNewValues == null) {
        StringBuilder missing = new StringBuilder();
        if (this.defaultValues == null) {
          missing.append(" defaultValues");
        }
        if (this.nestedNewValues == null) {
          missing.append(" nestedNewValues");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AddFields_Inner_AddFieldsInformation(
          this.outputFieldType,
          this.defaultValues,
          this.nestedNewValues);
    }
  }

}
