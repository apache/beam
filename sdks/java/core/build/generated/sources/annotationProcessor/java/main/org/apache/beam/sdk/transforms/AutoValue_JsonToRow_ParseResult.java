package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_JsonToRow_ParseResult extends JsonToRow.ParseResult {

  private final JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn;

  private final PCollection<Row> parsedLine;

  private final PCollection<Row> failedParse;

  private final Pipeline callingPipeline;

  private AutoValue_JsonToRow_ParseResult(
      JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn,
      PCollection<Row> parsedLine,
      PCollection<Row> failedParse,
      Pipeline callingPipeline) {
    this.jsonToRowWithErrFn = jsonToRowWithErrFn;
    this.parsedLine = parsedLine;
    this.failedParse = failedParse;
    this.callingPipeline = callingPipeline;
  }

  @Override
  JsonToRow.JsonToRowWithErrFn getJsonToRowWithErrFn() {
    return jsonToRowWithErrFn;
  }

  @Override
  PCollection<Row> getParsedLine() {
    return parsedLine;
  }

  @Override
  PCollection<Row> getFailedParse() {
    return failedParse;
  }

  @Override
  Pipeline getCallingPipeline() {
    return callingPipeline;
  }

  @Override
  public String toString() {
    return "ParseResult{"
        + "jsonToRowWithErrFn=" + jsonToRowWithErrFn + ", "
        + "parsedLine=" + parsedLine + ", "
        + "failedParse=" + failedParse + ", "
        + "callingPipeline=" + callingPipeline
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JsonToRow.ParseResult) {
      JsonToRow.ParseResult that = (JsonToRow.ParseResult) o;
      return this.jsonToRowWithErrFn.equals(that.getJsonToRowWithErrFn())
          && this.parsedLine.equals(that.getParsedLine())
          && this.failedParse.equals(that.getFailedParse())
          && this.callingPipeline.equals(that.getCallingPipeline());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= jsonToRowWithErrFn.hashCode();
    h$ *= 1000003;
    h$ ^= parsedLine.hashCode();
    h$ *= 1000003;
    h$ ^= failedParse.hashCode();
    h$ *= 1000003;
    h$ ^= callingPipeline.hashCode();
    return h$;
  }

  @Override
  JsonToRow.ParseResult.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends JsonToRow.ParseResult.Builder {
    private JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn;
    private PCollection<Row> parsedLine;
    private PCollection<Row> failedParse;
    private Pipeline callingPipeline;
    Builder() {
    }
    private Builder(JsonToRow.ParseResult source) {
      this.jsonToRowWithErrFn = source.getJsonToRowWithErrFn();
      this.parsedLine = source.getParsedLine();
      this.failedParse = source.getFailedParse();
      this.callingPipeline = source.getCallingPipeline();
    }
    @Override
    JsonToRow.ParseResult.Builder setJsonToRowWithErrFn(JsonToRow.JsonToRowWithErrFn jsonToRowWithErrFn) {
      if (jsonToRowWithErrFn == null) {
        throw new NullPointerException("Null jsonToRowWithErrFn");
      }
      this.jsonToRowWithErrFn = jsonToRowWithErrFn;
      return this;
    }
    @Override
    JsonToRow.ParseResult.Builder setParsedLine(PCollection<Row> parsedLine) {
      if (parsedLine == null) {
        throw new NullPointerException("Null parsedLine");
      }
      this.parsedLine = parsedLine;
      return this;
    }
    @Override
    JsonToRow.ParseResult.Builder setFailedParse(PCollection<Row> failedParse) {
      if (failedParse == null) {
        throw new NullPointerException("Null failedParse");
      }
      this.failedParse = failedParse;
      return this;
    }
    @Override
    JsonToRow.ParseResult.Builder setCallingPipeline(Pipeline callingPipeline) {
      if (callingPipeline == null) {
        throw new NullPointerException("Null callingPipeline");
      }
      this.callingPipeline = callingPipeline;
      return this;
    }
    @Override
    JsonToRow.ParseResult build() {
      if (this.jsonToRowWithErrFn == null
          || this.parsedLine == null
          || this.failedParse == null
          || this.callingPipeline == null) {
        StringBuilder missing = new StringBuilder();
        if (this.jsonToRowWithErrFn == null) {
          missing.append(" jsonToRowWithErrFn");
        }
        if (this.parsedLine == null) {
          missing.append(" parsedLine");
        }
        if (this.failedParse == null) {
          missing.append(" failedParse");
        }
        if (this.callingPipeline == null) {
          missing.append(" callingPipeline");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JsonToRow_ParseResult(
          this.jsonToRowWithErrFn,
          this.parsedLine,
          this.failedParse,
          this.callingPipeline);
    }
  }

}
