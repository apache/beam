package org.apache.beam.sdk.schemas.utils;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ReflectUtils_ClassWithSchema extends ReflectUtils.ClassWithSchema {

  private final Class<?> clazz;

  private final Schema schema;

  AutoValue_ReflectUtils_ClassWithSchema(
      Class<?> clazz,
      Schema schema) {
    if (clazz == null) {
      throw new NullPointerException("Null clazz");
    }
    this.clazz = clazz;
    if (schema == null) {
      throw new NullPointerException("Null schema");
    }
    this.schema = schema;
  }

  @Override
  public Class<?> getClazz() {
    return clazz;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "ClassWithSchema{"
        + "clazz=" + clazz + ", "
        + "schema=" + schema
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ReflectUtils.ClassWithSchema) {
      ReflectUtils.ClassWithSchema that = (ReflectUtils.ClassWithSchema) o;
      return this.clazz.equals(that.getClazz())
          && this.schema.equals(that.getSchema());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= clazz.hashCode();
    h$ *= 1000003;
    h$ ^= schema.hashCode();
    return h$;
  }

}
