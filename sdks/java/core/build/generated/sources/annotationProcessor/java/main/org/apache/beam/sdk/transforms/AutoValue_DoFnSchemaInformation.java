package org.apache.beam.sdk.transforms;

import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSchemaInformation extends DoFnSchemaInformation {

  private final List<SerializableFunction<?, ?>> elementConverters;

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private AutoValue_DoFnSchemaInformation(
      List<SerializableFunction<?, ?>> elementConverters,
      FieldAccessDescriptor fieldAccessDescriptor) {
    this.elementConverters = elementConverters;
    this.fieldAccessDescriptor = fieldAccessDescriptor;
  }

  @Override
  public List<SerializableFunction<?, ?>> getElementConverters() {
    return elementConverters;
  }

  @Override
  public FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  public String toString() {
    return "DoFnSchemaInformation{"
        + "elementConverters=" + elementConverters + ", "
        + "fieldAccessDescriptor=" + fieldAccessDescriptor
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSchemaInformation) {
      DoFnSchemaInformation that = (DoFnSchemaInformation) o;
      return this.elementConverters.equals(that.getElementConverters())
          && this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= elementConverters.hashCode();
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    return h$;
  }

  @Override
  public DoFnSchemaInformation.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends DoFnSchemaInformation.Builder {
    private List<SerializableFunction<?, ?>> elementConverters;
    private FieldAccessDescriptor fieldAccessDescriptor;
    Builder() {
    }
    private Builder(DoFnSchemaInformation source) {
      this.elementConverters = source.getElementConverters();
      this.fieldAccessDescriptor = source.getFieldAccessDescriptor();
    }
    @Override
    DoFnSchemaInformation.Builder setElementConverters(List<SerializableFunction<?, ?>> elementConverters) {
      if (elementConverters == null) {
        throw new NullPointerException("Null elementConverters");
      }
      this.elementConverters = elementConverters;
      return this;
    }
    @Override
    DoFnSchemaInformation.Builder setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      if (fieldAccessDescriptor == null) {
        throw new NullPointerException("Null fieldAccessDescriptor");
      }
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      return this;
    }
    @Override
    DoFnSchemaInformation build() {
      if (this.elementConverters == null
          || this.fieldAccessDescriptor == null) {
        StringBuilder missing = new StringBuilder();
        if (this.elementConverters == null) {
          missing.append(" elementConverters");
        }
        if (this.fieldAccessDescriptor == null) {
          missing.append(" fieldAccessDescriptor");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_DoFnSchemaInformation(
          this.elementConverters,
          this.fieldAccessDescriptor);
    }
  }

}
