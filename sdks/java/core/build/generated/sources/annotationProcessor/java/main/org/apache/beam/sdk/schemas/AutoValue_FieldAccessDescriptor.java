package org.apache.beam.sdk.schemas;

import java.util.List;
import java.util.Map;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FieldAccessDescriptor extends FieldAccessDescriptor {

  private final boolean allFields;

  private final List<FieldAccessDescriptor.FieldDescriptor> fieldsAccessed;

  private final Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> nestedFieldsAccessed;

  private AutoValue_FieldAccessDescriptor(
      boolean allFields,
      List<FieldAccessDescriptor.FieldDescriptor> fieldsAccessed,
      Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> nestedFieldsAccessed) {
    this.allFields = allFields;
    this.fieldsAccessed = fieldsAccessed;
    this.nestedFieldsAccessed = nestedFieldsAccessed;
  }

  @Override
  public boolean getAllFields() {
    return allFields;
  }

  @Override
  public List<FieldAccessDescriptor.FieldDescriptor> getFieldsAccessed() {
    return fieldsAccessed;
  }

  @Override
  public Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> getNestedFieldsAccessed() {
    return nestedFieldsAccessed;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FieldAccessDescriptor) {
      FieldAccessDescriptor that = (FieldAccessDescriptor) o;
      return this.allFields == that.getAllFields()
          && this.fieldsAccessed.equals(that.getFieldsAccessed())
          && this.nestedFieldsAccessed.equals(that.getNestedFieldsAccessed());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= allFields ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= fieldsAccessed.hashCode();
    h$ *= 1000003;
    h$ ^= nestedFieldsAccessed.hashCode();
    return h$;
  }

  @Override
  FieldAccessDescriptor.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FieldAccessDescriptor.Builder {
    private Boolean allFields;
    private List<FieldAccessDescriptor.FieldDescriptor> fieldsAccessed;
    private Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> nestedFieldsAccessed;
    Builder() {
    }
    private Builder(FieldAccessDescriptor source) {
      this.allFields = source.getAllFields();
      this.fieldsAccessed = source.getFieldsAccessed();
      this.nestedFieldsAccessed = source.getNestedFieldsAccessed();
    }
    @Override
    FieldAccessDescriptor.Builder setAllFields(boolean allFields) {
      this.allFields = allFields;
      return this;
    }
    @Override
    FieldAccessDescriptor.Builder setFieldsAccessed(List<FieldAccessDescriptor.FieldDescriptor> fieldsAccessed) {
      if (fieldsAccessed == null) {
        throw new NullPointerException("Null fieldsAccessed");
      }
      this.fieldsAccessed = fieldsAccessed;
      return this;
    }
    @Override
    FieldAccessDescriptor.Builder setNestedFieldsAccessed(Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> nestedFieldsAccessed) {
      if (nestedFieldsAccessed == null) {
        throw new NullPointerException("Null nestedFieldsAccessed");
      }
      this.nestedFieldsAccessed = nestedFieldsAccessed;
      return this;
    }
    @Override
    FieldAccessDescriptor build() {
      if (this.allFields == null
          || this.fieldsAccessed == null
          || this.nestedFieldsAccessed == null) {
        StringBuilder missing = new StringBuilder();
        if (this.allFields == null) {
          missing.append(" allFields");
        }
        if (this.fieldsAccessed == null) {
          missing.append(" fieldsAccessed");
        }
        if (this.nestedFieldsAccessed == null) {
          missing.append(" nestedFieldsAccessed");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FieldAccessDescriptor(
          this.allFields,
          this.fieldsAccessed,
          this.nestedFieldsAccessed);
    }
  }

}
