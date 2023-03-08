package org.apache.beam.sdk.schemas.utils;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SelectByteBuddyHelpers_SchemaAndDescriptor extends SelectByteBuddyHelpers.SchemaAndDescriptor {

  private final Schema schema;

  private final FieldAccessDescriptor fieldAccessDecriptor;

  AutoValue_SelectByteBuddyHelpers_SchemaAndDescriptor(
      Schema schema,
      FieldAccessDescriptor fieldAccessDecriptor) {
    if (schema == null) {
      throw new NullPointerException("Null schema");
    }
    this.schema = schema;
    if (fieldAccessDecriptor == null) {
      throw new NullPointerException("Null fieldAccessDecriptor");
    }
    this.fieldAccessDecriptor = fieldAccessDecriptor;
  }

  @Override
  Schema getSchema() {
    return schema;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDecriptor() {
    return fieldAccessDecriptor;
  }

  @Override
  public String toString() {
    return "SchemaAndDescriptor{"
        + "schema=" + schema + ", "
        + "fieldAccessDecriptor=" + fieldAccessDecriptor
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SelectByteBuddyHelpers.SchemaAndDescriptor) {
      SelectByteBuddyHelpers.SchemaAndDescriptor that = (SelectByteBuddyHelpers.SchemaAndDescriptor) o;
      return this.schema.equals(that.getSchema())
          && this.fieldAccessDecriptor.equals(that.getFieldAccessDecriptor());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= schema.hashCode();
    h$ *= 1000003;
    h$ ^= fieldAccessDecriptor.hashCode();
    return h$;
  }

}
