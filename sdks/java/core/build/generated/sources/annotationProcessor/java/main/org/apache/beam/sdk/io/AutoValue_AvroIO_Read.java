package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_Read<T> extends AvroIO.Read<T> {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration matchConfiguration;

  private final @Nullable Class<T> recordClass;

  private final @Nullable Schema schema;

  private final boolean inferBeamSchema;

  private final boolean hintMatchesManyFiles;

  private AutoValue_AvroIO_Read(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration matchConfiguration,
      @Nullable Class<T> recordClass,
      @Nullable Schema schema,
      boolean inferBeamSchema,
      boolean hintMatchesManyFiles) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.recordClass = recordClass;
    this.schema = schema;
    this.inferBeamSchema = inferBeamSchema;
    this.hintMatchesManyFiles = hintMatchesManyFiles;
  }

  @Override
  @Nullable ValueProvider<String> getFilepattern() {
    return filepattern;
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
  boolean getInferBeamSchema() {
    return inferBeamSchema;
  }

  @Override
  boolean getHintMatchesManyFiles() {
    return hintMatchesManyFiles;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.Read) {
      AvroIO.Read<?> that = (AvroIO.Read<?>) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.matchConfiguration.equals(that.getMatchConfiguration())
          && (this.recordClass == null ? that.getRecordClass() == null : this.recordClass.equals(that.getRecordClass()))
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.inferBeamSchema == that.getInferBeamSchema()
          && this.hintMatchesManyFiles == that.getHintMatchesManyFiles();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filepattern == null) ? 0 : filepattern.hashCode();
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (recordClass == null) ? 0 : recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= inferBeamSchema ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= hintMatchesManyFiles ? 1231 : 1237;
    return h$;
  }

  @Override
  AvroIO.Read.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.Read.Builder<T> {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.MatchConfiguration matchConfiguration;
    private @Nullable Class<T> recordClass;
    private @Nullable Schema schema;
    private Boolean inferBeamSchema;
    private Boolean hintMatchesManyFiles;
    Builder() {
    }
    private Builder(AvroIO.Read<T> source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.recordClass = source.getRecordClass();
      this.schema = source.getSchema();
      this.inferBeamSchema = source.getInferBeamSchema();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
    }
    @Override
    AvroIO.Read.Builder<T> setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setRecordClass(Class<T> recordClass) {
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setInferBeamSchema(boolean inferBeamSchema) {
      this.inferBeamSchema = inferBeamSchema;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      return this;
    }
    @Override
    AvroIO.Read<T> build() {
      if (this.matchConfiguration == null
          || this.inferBeamSchema == null
          || this.hintMatchesManyFiles == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.inferBeamSchema == null) {
          missing.append(" inferBeamSchema");
        }
        if (this.hintMatchesManyFiles == null) {
          missing.append(" hintMatchesManyFiles");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_Read<T>(
          this.filepattern,
          this.matchConfiguration,
          this.recordClass,
          this.schema,
          this.inferBeamSchema,
          this.hintMatchesManyFiles);
    }
  }

}
