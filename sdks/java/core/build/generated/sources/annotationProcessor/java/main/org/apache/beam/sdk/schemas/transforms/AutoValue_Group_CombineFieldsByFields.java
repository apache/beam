package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Group_CombineFieldsByFields<InputT> extends Group.CombineFieldsByFields<InputT> {

  private final Group.ByFields<InputT> byFields;

  private final SchemaAggregateFn.Inner schemaAggregateFn;

  private final String keyField;

  private final String valueField;

  private AutoValue_Group_CombineFieldsByFields(
      Group.ByFields<InputT> byFields,
      SchemaAggregateFn.Inner schemaAggregateFn,
      String keyField,
      String valueField) {
    this.byFields = byFields;
    this.schemaAggregateFn = schemaAggregateFn;
    this.keyField = keyField;
    this.valueField = valueField;
  }

  @Override
  Group.ByFields<InputT> getByFields() {
    return byFields;
  }

  @Override
  SchemaAggregateFn.Inner getSchemaAggregateFn() {
    return schemaAggregateFn;
  }

  @Override
  String getKeyField() {
    return keyField;
  }

  @Override
  String getValueField() {
    return valueField;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Group.CombineFieldsByFields) {
      Group.CombineFieldsByFields<?> that = (Group.CombineFieldsByFields<?>) o;
      return this.byFields.equals(that.getByFields())
          && this.schemaAggregateFn.equals(that.getSchemaAggregateFn())
          && this.keyField.equals(that.getKeyField())
          && this.valueField.equals(that.getValueField());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= byFields.hashCode();
    h$ *= 1000003;
    h$ ^= schemaAggregateFn.hashCode();
    h$ *= 1000003;
    h$ ^= keyField.hashCode();
    h$ *= 1000003;
    h$ ^= valueField.hashCode();
    return h$;
  }

  @Override
  Group.CombineFieldsByFields.Builder<InputT> toBuilder() {
    return new Builder<InputT>(this);
  }

  static final class Builder<InputT> extends Group.CombineFieldsByFields.Builder<InputT> {
    private Group.ByFields<InputT> byFields;
    private SchemaAggregateFn.Inner schemaAggregateFn;
    private String keyField;
    private String valueField;
    Builder() {
    }
    private Builder(Group.CombineFieldsByFields<InputT> source) {
      this.byFields = source.getByFields();
      this.schemaAggregateFn = source.getSchemaAggregateFn();
      this.keyField = source.getKeyField();
      this.valueField = source.getValueField();
    }
    @Override
    Group.CombineFieldsByFields.Builder<InputT> setByFields(Group.ByFields<InputT> byFields) {
      if (byFields == null) {
        throw new NullPointerException("Null byFields");
      }
      this.byFields = byFields;
      return this;
    }
    @Override
    Group.CombineFieldsByFields.Builder<InputT> setSchemaAggregateFn(SchemaAggregateFn.Inner schemaAggregateFn) {
      if (schemaAggregateFn == null) {
        throw new NullPointerException("Null schemaAggregateFn");
      }
      this.schemaAggregateFn = schemaAggregateFn;
      return this;
    }
    @Override
    Group.CombineFieldsByFields.Builder<InputT> setKeyField(String keyField) {
      if (keyField == null) {
        throw new NullPointerException("Null keyField");
      }
      this.keyField = keyField;
      return this;
    }
    @Override
    Group.CombineFieldsByFields.Builder<InputT> setValueField(String valueField) {
      if (valueField == null) {
        throw new NullPointerException("Null valueField");
      }
      this.valueField = valueField;
      return this;
    }
    @Override
    Group.CombineFieldsByFields<InputT> build() {
      if (this.byFields == null
          || this.schemaAggregateFn == null
          || this.keyField == null
          || this.valueField == null) {
        StringBuilder missing = new StringBuilder();
        if (this.byFields == null) {
          missing.append(" byFields");
        }
        if (this.schemaAggregateFn == null) {
          missing.append(" schemaAggregateFn");
        }
        if (this.keyField == null) {
          missing.append(" keyField");
        }
        if (this.valueField == null) {
          missing.append(" valueField");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Group_CombineFieldsByFields<InputT>(
          this.byFields,
          this.schemaAggregateFn,
          this.keyField,
          this.valueField);
    }
  }

}
