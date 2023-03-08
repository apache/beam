package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Cast<T> extends Cast<T> {

  private final Schema outputSchema;

  private final Cast.Validator validator;

  AutoValue_Cast(
      Schema outputSchema,
      Cast.Validator validator) {
    if (outputSchema == null) {
      throw new NullPointerException("Null outputSchema");
    }
    this.outputSchema = outputSchema;
    if (validator == null) {
      throw new NullPointerException("Null validator");
    }
    this.validator = validator;
  }

  @Override
  public Schema outputSchema() {
    return outputSchema;
  }

  @Override
  public Cast.Validator validator() {
    return validator;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Cast) {
      Cast<?> that = (Cast<?>) o;
      return this.outputSchema.equals(that.outputSchema())
          && this.validator.equals(that.validator());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= outputSchema.hashCode();
    h$ *= 1000003;
    h$ ^= validator.hashCode();
    return h$;
  }

}
