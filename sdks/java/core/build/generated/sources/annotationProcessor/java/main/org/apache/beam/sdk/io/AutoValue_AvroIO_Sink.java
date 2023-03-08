package org.apache.beam.sdk.io;

import java.util.Map;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_Sink<ElementT> extends AvroIO.Sink<ElementT> {

  private final AvroIO.@Nullable RecordFormatter<ElementT> recordFormatter;

  private final @Nullable String jsonSchema;

  private final Map<String, Object> metadata;

  private final SerializableAvroCodecFactory codec;

  private AutoValue_AvroIO_Sink(
      AvroIO.@Nullable RecordFormatter<ElementT> recordFormatter,
      @Nullable String jsonSchema,
      Map<String, Object> metadata,
      SerializableAvroCodecFactory codec) {
    this.recordFormatter = recordFormatter;
    this.jsonSchema = jsonSchema;
    this.metadata = metadata;
    this.codec = codec;
  }

  @Deprecated
  @Override
  AvroIO.@Nullable RecordFormatter<ElementT> getRecordFormatter() {
    return recordFormatter;
  }

  @Override
  @Nullable String getJsonSchema() {
    return jsonSchema;
  }

  @Override
  Map<String, Object> getMetadata() {
    return metadata;
  }

  @Override
  SerializableAvroCodecFactory getCodec() {
    return codec;
  }

  @Override
  public String toString() {
    return "Sink{"
        + "recordFormatter=" + recordFormatter + ", "
        + "jsonSchema=" + jsonSchema + ", "
        + "metadata=" + metadata + ", "
        + "codec=" + codec
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.Sink) {
      AvroIO.Sink<?> that = (AvroIO.Sink<?>) o;
      return (this.recordFormatter == null ? that.getRecordFormatter() == null : this.recordFormatter.equals(that.getRecordFormatter()))
          && (this.jsonSchema == null ? that.getJsonSchema() == null : this.jsonSchema.equals(that.getJsonSchema()))
          && this.metadata.equals(that.getMetadata())
          && this.codec.equals(that.getCodec());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (recordFormatter == null) ? 0 : recordFormatter.hashCode();
    h$ *= 1000003;
    h$ ^= (jsonSchema == null) ? 0 : jsonSchema.hashCode();
    h$ *= 1000003;
    h$ ^= metadata.hashCode();
    h$ *= 1000003;
    h$ ^= codec.hashCode();
    return h$;
  }

  @Override
  AvroIO.Sink.Builder<ElementT> toBuilder() {
    return new Builder<ElementT>(this);
  }

  static final class Builder<ElementT> extends AvroIO.Sink.Builder<ElementT> {
    private AvroIO.@Nullable RecordFormatter<ElementT> recordFormatter;
    private @Nullable String jsonSchema;
    private Map<String, Object> metadata;
    private SerializableAvroCodecFactory codec;
    Builder() {
    }
    private Builder(AvroIO.Sink<ElementT> source) {
      this.recordFormatter = source.getRecordFormatter();
      this.jsonSchema = source.getJsonSchema();
      this.metadata = source.getMetadata();
      this.codec = source.getCodec();
    }
    @Override
    AvroIO.Sink.Builder<ElementT> setRecordFormatter(AvroIO.RecordFormatter<ElementT> recordFormatter) {
      this.recordFormatter = recordFormatter;
      return this;
    }
    @Override
    AvroIO.Sink.Builder<ElementT> setJsonSchema(String jsonSchema) {
      this.jsonSchema = jsonSchema;
      return this;
    }
    @Override
    AvroIO.Sink.Builder<ElementT> setMetadata(Map<String, Object> metadata) {
      if (metadata == null) {
        throw new NullPointerException("Null metadata");
      }
      this.metadata = metadata;
      return this;
    }
    @Override
    AvroIO.Sink.Builder<ElementT> setCodec(SerializableAvroCodecFactory codec) {
      if (codec == null) {
        throw new NullPointerException("Null codec");
      }
      this.codec = codec;
      return this;
    }
    @Override
    AvroIO.Sink<ElementT> build() {
      if (this.metadata == null
          || this.codec == null) {
        StringBuilder missing = new StringBuilder();
        if (this.metadata == null) {
          missing.append(" metadata");
        }
        if (this.codec == null) {
          missing.append(" codec");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_Sink<ElementT>(
          this.recordFormatter,
          this.jsonSchema,
          this.metadata,
          this.codec);
    }
  }

}
