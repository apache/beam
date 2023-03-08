package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Select_Fields<T> extends Select.Fields<T> {

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private final @Nullable Schema outputSchema;

  private AutoValue_Select_Fields(
      FieldAccessDescriptor fieldAccessDescriptor,
      @Nullable Schema outputSchema) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
    this.outputSchema = outputSchema;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  @Nullable Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Select.Fields) {
      Select.Fields<?> that = (Select.Fields<?>) o;
      return this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor())
          && (this.outputSchema == null ? that.getOutputSchema() == null : this.outputSchema.equals(that.getOutputSchema()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= (outputSchema == null) ? 0 : outputSchema.hashCode();
    return h$;
  }

  @Override
  Select.Fields.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends Select.Fields.Builder<T> {
    private FieldAccessDescriptor fieldAccessDescriptor;
    private @Nullable Schema outputSchema;
    Builder() {
    }
    private Builder(Select.Fields<T> source) {
      this.fieldAccessDescriptor = source.getFieldAccessDescriptor();
      this.outputSchema = source.getOutputSchema();
    }
    @Override
    Select.Fields.Builder<T> setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      if (fieldAccessDescriptor == null) {
        throw new NullPointerException("Null fieldAccessDescriptor");
      }
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      return this;
    }
    @Override
    Select.Fields.Builder<T> setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }
    @Override
    Select.Fields<T> build() {
      if (this.fieldAccessDescriptor == null) {
        String missing = " fieldAccessDescriptor";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Select_Fields<T>(
          this.fieldAccessDescriptor,
          this.outputSchema);
    }
  }

}
