package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Select_Flattened<T> extends Select.Flattened<T> {

  private final SerializableFunction<List<String>, String> nameFn;

  private final Map<String, String> nameOverrides;

  private final @Nullable Schema outputSchema;

  private AutoValue_Select_Flattened(
      SerializableFunction<List<String>, String> nameFn,
      Map<String, String> nameOverrides,
      @Nullable Schema outputSchema) {
    this.nameFn = nameFn;
    this.nameOverrides = nameOverrides;
    this.outputSchema = outputSchema;
  }

  @Override
  SerializableFunction<List<String>, String> getNameFn() {
    return nameFn;
  }

  @Override
  Map<String, String> getNameOverrides() {
    return nameOverrides;
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
    if (o instanceof Select.Flattened) {
      Select.Flattened<?> that = (Select.Flattened<?>) o;
      return this.nameFn.equals(that.getNameFn())
          && this.nameOverrides.equals(that.getNameOverrides())
          && (this.outputSchema == null ? that.getOutputSchema() == null : this.outputSchema.equals(that.getOutputSchema()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= nameFn.hashCode();
    h$ *= 1000003;
    h$ ^= nameOverrides.hashCode();
    h$ *= 1000003;
    h$ ^= (outputSchema == null) ? 0 : outputSchema.hashCode();
    return h$;
  }

  @Override
  Select.Flattened.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends Select.Flattened.Builder<T> {
    private SerializableFunction<List<String>, String> nameFn;
    private Map<String, String> nameOverrides;
    private @Nullable Schema outputSchema;
    Builder() {
    }
    private Builder(Select.Flattened<T> source) {
      this.nameFn = source.getNameFn();
      this.nameOverrides = source.getNameOverrides();
      this.outputSchema = source.getOutputSchema();
    }
    @Override
    Select.Flattened.Builder<T> setNameFn(SerializableFunction<List<String>, String> nameFn) {
      if (nameFn == null) {
        throw new NullPointerException("Null nameFn");
      }
      this.nameFn = nameFn;
      return this;
    }
    @Override
    Select.Flattened.Builder<T> setNameOverrides(Map<String, String> nameOverrides) {
      if (nameOverrides == null) {
        throw new NullPointerException("Null nameOverrides");
      }
      this.nameOverrides = nameOverrides;
      return this;
    }
    @Override
    Select.Flattened.Builder<T> setOutputSchema(@Nullable Schema outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }
    @Override
    Select.Flattened<T> build() {
      if (this.nameFn == null
          || this.nameOverrides == null) {
        StringBuilder missing = new StringBuilder();
        if (this.nameFn == null) {
          missing.append(" nameFn");
        }
        if (this.nameOverrides == null) {
          missing.append(" nameOverrides");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Select_Flattened<T>(
          this.nameFn,
          this.nameOverrides,
          this.outputSchema);
    }
  }

}
