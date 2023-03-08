package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowJson;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_JsonToRow_JsonToRowWithErrFn extends JsonToRow.JsonToRowWithErrFn {

  private final Schema schema;

  private final String lineFieldName;

  private final String errorFieldName;

  private final boolean extendedErrorInfo;

  private final RowJson.RowJsonDeserializer.NullBehavior nullBehavior;

  private AutoValue_JsonToRow_JsonToRowWithErrFn(
      Schema schema,
      String lineFieldName,
      String errorFieldName,
      boolean extendedErrorInfo,
      RowJson.RowJsonDeserializer.NullBehavior nullBehavior) {
    this.schema = schema;
    this.lineFieldName = lineFieldName;
    this.errorFieldName = errorFieldName;
    this.extendedErrorInfo = extendedErrorInfo;
    this.nullBehavior = nullBehavior;
  }

  @Override
  Schema getSchema() {
    return schema;
  }

  @Override
  String getLineFieldName() {
    return lineFieldName;
  }

  @Override
  String getErrorFieldName() {
    return errorFieldName;
  }

  @Override
  boolean getExtendedErrorInfo() {
    return extendedErrorInfo;
  }

  @Override
  RowJson.RowJsonDeserializer.NullBehavior getNullBehavior() {
    return nullBehavior;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JsonToRow.JsonToRowWithErrFn) {
      JsonToRow.JsonToRowWithErrFn that = (JsonToRow.JsonToRowWithErrFn) o;
      return this.schema.equals(that.getSchema())
          && this.lineFieldName.equals(that.getLineFieldName())
          && this.errorFieldName.equals(that.getErrorFieldName())
          && this.extendedErrorInfo == that.getExtendedErrorInfo()
          && this.nullBehavior.equals(that.getNullBehavior());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= schema.hashCode();
    h$ *= 1000003;
    h$ ^= lineFieldName.hashCode();
    h$ *= 1000003;
    h$ ^= errorFieldName.hashCode();
    h$ *= 1000003;
    h$ ^= extendedErrorInfo ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= nullBehavior.hashCode();
    return h$;
  }

  @Override
  JsonToRow.JsonToRowWithErrFn.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends JsonToRow.JsonToRowWithErrFn.Builder {
    private Schema schema;
    private String lineFieldName;
    private String errorFieldName;
    private Boolean extendedErrorInfo;
    private RowJson.RowJsonDeserializer.NullBehavior nullBehavior;
    Builder() {
    }
    private Builder(JsonToRow.JsonToRowWithErrFn source) {
      this.schema = source.getSchema();
      this.lineFieldName = source.getLineFieldName();
      this.errorFieldName = source.getErrorFieldName();
      this.extendedErrorInfo = source.getExtendedErrorInfo();
      this.nullBehavior = source.getNullBehavior();
    }
    @Override
    JsonToRow.JsonToRowWithErrFn.Builder setSchema(Schema schema) {
      if (schema == null) {
        throw new NullPointerException("Null schema");
      }
      this.schema = schema;
      return this;
    }
    @Override
    JsonToRow.JsonToRowWithErrFn.Builder setLineFieldName(String lineFieldName) {
      if (lineFieldName == null) {
        throw new NullPointerException("Null lineFieldName");
      }
      this.lineFieldName = lineFieldName;
      return this;
    }
    @Override
    JsonToRow.JsonToRowWithErrFn.Builder setErrorFieldName(String errorFieldName) {
      if (errorFieldName == null) {
        throw new NullPointerException("Null errorFieldName");
      }
      this.errorFieldName = errorFieldName;
      return this;
    }
    @Override
    JsonToRow.JsonToRowWithErrFn.Builder setExtendedErrorInfo(boolean extendedErrorInfo) {
      this.extendedErrorInfo = extendedErrorInfo;
      return this;
    }
    @Override
    JsonToRow.JsonToRowWithErrFn.Builder setNullBehavior(RowJson.RowJsonDeserializer.NullBehavior nullBehavior) {
      if (nullBehavior == null) {
        throw new NullPointerException("Null nullBehavior");
      }
      this.nullBehavior = nullBehavior;
      return this;
    }
    @Override
    JsonToRow.JsonToRowWithErrFn build() {
      if (this.schema == null
          || this.lineFieldName == null
          || this.errorFieldName == null
          || this.extendedErrorInfo == null
          || this.nullBehavior == null) {
        StringBuilder missing = new StringBuilder();
        if (this.schema == null) {
          missing.append(" schema");
        }
        if (this.lineFieldName == null) {
          missing.append(" lineFieldName");
        }
        if (this.errorFieldName == null) {
          missing.append(" errorFieldName");
        }
        if (this.extendedErrorInfo == null) {
          missing.append(" extendedErrorInfo");
        }
        if (this.nullBehavior == null) {
          missing.append(" nullBehavior");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JsonToRow_JsonToRowWithErrFn(
          this.schema,
          this.lineFieldName,
          this.errorFieldName,
          this.extendedErrorInfo,
          this.nullBehavior);
    }
  }

}
