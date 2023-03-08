package org.apache.beam.sdk.schemas;

import java.util.List;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FieldAccessDescriptor_FieldDescriptor extends FieldAccessDescriptor.FieldDescriptor {

  private final @Nullable String fieldName;

  private final @Nullable Integer fieldId;

  private final @Nullable String fieldRename;

  private final List<FieldAccessDescriptor.FieldDescriptor.Qualifier> qualifiers;

  private AutoValue_FieldAccessDescriptor_FieldDescriptor(
      @Nullable String fieldName,
      @Nullable Integer fieldId,
      @Nullable String fieldRename,
      List<FieldAccessDescriptor.FieldDescriptor.Qualifier> qualifiers) {
    this.fieldName = fieldName;
    this.fieldId = fieldId;
    this.fieldRename = fieldRename;
    this.qualifiers = qualifiers;
  }

  @Override
  public @Nullable String getFieldName() {
    return fieldName;
  }

  @Override
  public @Nullable Integer getFieldId() {
    return fieldId;
  }

  @Override
  public @Nullable String getFieldRename() {
    return fieldRename;
  }

  @Override
  public List<FieldAccessDescriptor.FieldDescriptor.Qualifier> getQualifiers() {
    return qualifiers;
  }

  @Override
  public String toString() {
    return "FieldDescriptor{"
        + "fieldName=" + fieldName + ", "
        + "fieldId=" + fieldId + ", "
        + "fieldRename=" + fieldRename + ", "
        + "qualifiers=" + qualifiers
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FieldAccessDescriptor.FieldDescriptor) {
      FieldAccessDescriptor.FieldDescriptor that = (FieldAccessDescriptor.FieldDescriptor) o;
      return (this.fieldName == null ? that.getFieldName() == null : this.fieldName.equals(that.getFieldName()))
          && (this.fieldId == null ? that.getFieldId() == null : this.fieldId.equals(that.getFieldId()))
          && (this.fieldRename == null ? that.getFieldRename() == null : this.fieldRename.equals(that.getFieldRename()))
          && this.qualifiers.equals(that.getQualifiers());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (fieldName == null) ? 0 : fieldName.hashCode();
    h$ *= 1000003;
    h$ ^= (fieldId == null) ? 0 : fieldId.hashCode();
    h$ *= 1000003;
    h$ ^= (fieldRename == null) ? 0 : fieldRename.hashCode();
    h$ *= 1000003;
    h$ ^= qualifiers.hashCode();
    return h$;
  }

  @Override
  FieldAccessDescriptor.FieldDescriptor.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FieldAccessDescriptor.FieldDescriptor.Builder {
    private @Nullable String fieldName;
    private @Nullable Integer fieldId;
    private @Nullable String fieldRename;
    private List<FieldAccessDescriptor.FieldDescriptor.Qualifier> qualifiers;
    Builder() {
    }
    private Builder(FieldAccessDescriptor.FieldDescriptor source) {
      this.fieldName = source.getFieldName();
      this.fieldId = source.getFieldId();
      this.fieldRename = source.getFieldRename();
      this.qualifiers = source.getQualifiers();
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Builder setFieldName(@Nullable String fieldName) {
      this.fieldName = fieldName;
      return this;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Builder setFieldId(@Nullable Integer fieldId) {
      this.fieldId = fieldId;
      return this;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Builder setFieldRename(@Nullable String fieldRename) {
      this.fieldRename = fieldRename;
      return this;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Builder setQualifiers(List<FieldAccessDescriptor.FieldDescriptor.Qualifier> qualifiers) {
      if (qualifiers == null) {
        throw new NullPointerException("Null qualifiers");
      }
      this.qualifiers = qualifiers;
      return this;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor build() {
      if (this.qualifiers == null) {
        String missing = " qualifiers";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FieldAccessDescriptor_FieldDescriptor(
          this.fieldName,
          this.fieldId,
          this.fieldRename,
          this.qualifiers);
    }
  }

}
