package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_JsonToRow_JsonToRowWithErrFn_ParseWithError extends JsonToRow.JsonToRowWithErrFn.ParseWithError {

  private final JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn;

  private AutoValue_JsonToRow_JsonToRowWithErrFn_ParseWithError(
      JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn) {
    this.jsonToRowWithErrFn = jsonToRowWithErrFn;
  }

  @Override
  public JsonToRow.JsonToRowWithErrFn getJsonToRowWithErrFn() {
    return jsonToRowWithErrFn;
  }

  @Override
  public String toString() {
    return "ParseWithError{"
        + "jsonToRowWithErrFn=" + jsonToRowWithErrFn
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JsonToRow.JsonToRowWithErrFn.ParseWithError) {
      JsonToRow.JsonToRowWithErrFn.ParseWithError that = (JsonToRow.JsonToRowWithErrFn.ParseWithError) o;
      return this.jsonToRowWithErrFn.equals(that.getJsonToRowWithErrFn());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= jsonToRowWithErrFn.hashCode();
    return h$;
  }

  @Override
  public JsonToRow.JsonToRowWithErrFn.ParseWithError.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends JsonToRow.JsonToRowWithErrFn.ParseWithError.Builder {
    private JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn;
    Builder() {
    }
    private Builder(JsonToRow.JsonToRowWithErrFn.ParseWithError source) {
      this.jsonToRowWithErrFn = source.getJsonToRowWithErrFn();
    }
    @Override
    public JsonToRow.JsonToRowWithErrFn.ParseWithError.Builder setJsonToRowWithErrFn(JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn) {
      if (jsonToRowWithErrFn == null) {
        throw new NullPointerException("Null jsonToRowWithErrFn");
      }
      this.jsonToRowWithErrFn = jsonToRowWithErrFn;
      return this;
    }
    @Override
    public JsonToRow.JsonToRowWithErrFn.ParseWithError build() {
      if (this.jsonToRowWithErrFn == null) {
        String missing = " jsonToRowWithErrFn";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JsonToRow_JsonToRowWithErrFn_ParseWithError(
          this.jsonToRowWithErrFn);
    }
  }

}
