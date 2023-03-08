package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_ReadAll<T> extends AvroIO.ReadAll<T> {

  private final FileIO.MatchConfiguration matchConfiguration;

  private final @Nullable Class<T> recordClass;

  private final @Nullable Schema schema;

  private final long desiredBundleSizeBytes;

  private final boolean inferBeamSchema;

  private AutoValue_AvroIO_ReadAll(
      FileIO.MatchConfiguration matchConfiguration,
      @Nullable Class<T> recordClass,
      @Nullable Schema schema,
      long desiredBundleSizeBytes,
      boolean inferBeamSchema) {
    this.matchConfiguration = matchConfiguration;
    this.recordClass = recordClass;
    this.schema = schema;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.inferBeamSchema = inferBeamSchema;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
  }

  @Override
  @Nullable Class<T> getRecordClass() {
    return recordClass;
  }

  @Override
  @Nullable Schema getSchema() {
    return schema;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @Override
  boolean getInferBeamSchema() {
    return inferBeamSchema;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.ReadAll) {
      AvroIO.ReadAll<?> that = (AvroIO.ReadAll<?>) o;
      return this.matchConfiguration.equals(that.getMatchConfiguration())
          && (this.recordClass == null ? that.getRecordClass() == null : this.recordClass.equals(that.getRecordClass()))
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes()
          && this.inferBeamSchema == that.getInferBeamSchema();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (recordClass == null) ? 0 : recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    h$ *= 1000003;
    h$ ^= inferBeamSchema ? 1231 : 1237;
    return h$;
  }

  @Override
  AvroIO.ReadAll.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.ReadAll.Builder<T> {
    private FileIO.MatchConfiguration matchConfiguration;
    private @Nullable Class<T> recordClass;
    private @Nullable Schema schema;
    private Long desiredBundleSizeBytes;
    private Boolean inferBeamSchema;
    Builder() {
    }
    private Builder(AvroIO.ReadAll<T> source) {
      this.matchConfiguration = source.getMatchConfiguration();
      this.recordClass = source.getRecordClass();
      this.schema = source.getSchema();
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      this.inferBeamSchema = source.getInferBeamSchema();
    }
    @Override
    AvroIO.ReadAll.Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    AvroIO.ReadAll.Builder<T> setRecordClass(Class<T> recordClass) {
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.ReadAll.Builder<T> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.ReadAll.Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      return this;
    }
    @Override
    AvroIO.ReadAll.Builder<T> setInferBeamSchema(boolean inferBeamSchema) {
      this.inferBeamSchema = inferBeamSchema;
      return this;
    }
    @Override
    AvroIO.ReadAll<T> build() {
      if (this.matchConfiguration == null
          || this.desiredBundleSizeBytes == null
          || this.inferBeamSchema == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.desiredBundleSizeBytes == null) {
          missing.append(" desiredBundleSizeBytes");
        }
        if (this.inferBeamSchema == null) {
          missing.append(" inferBeamSchema");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_ReadAll<T>(
          this.matchConfiguration,
          this.recordClass,
          this.schema,
          this.desiredBundleSizeBytes,
          this.inferBeamSchema);
    }
  }

}
