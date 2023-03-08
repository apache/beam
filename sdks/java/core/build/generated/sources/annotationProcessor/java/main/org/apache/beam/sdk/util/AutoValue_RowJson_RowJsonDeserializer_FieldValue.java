package org.apache.beam.sdk.util;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RowJson_RowJsonDeserializer_FieldValue extends RowJson.RowJsonDeserializer.FieldValue {

  private final String name;

  private final Schema.FieldType type;

  private final @Nullable JsonNode jsonValue;

  AutoValue_RowJson_RowJsonDeserializer_FieldValue(
      String name,
      Schema.FieldType type,
      @Nullable JsonNode jsonValue) {
    if (name == null) {
      throw new NullPointerException("Null name");
    }
    this.name = name;
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    this.jsonValue = jsonValue;
  }

  @Override
  String name() {
    return name;
  }

  @Override
  Schema.FieldType type() {
    return type;
  }

  @Override
  @Nullable JsonNode jsonValue() {
    return jsonValue;
  }

  @Override
  public String toString() {
    return "FieldValue{"
        + "name=" + name + ", "
        + "type=" + type + ", "
        + "jsonValue=" + jsonValue
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RowJson.RowJsonDeserializer.FieldValue) {
      RowJson.RowJsonDeserializer.FieldValue that = (RowJson.RowJsonDeserializer.FieldValue) o;
      return this.name.equals(that.name())
          && this.type.equals(that.type())
          && (this.jsonValue == null ? that.jsonValue() == null : this.jsonValue.equals(that.jsonValue()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= name.hashCode();
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= (jsonValue == null) ? 0 : jsonValue.hashCode();
    return h$;
  }

}
