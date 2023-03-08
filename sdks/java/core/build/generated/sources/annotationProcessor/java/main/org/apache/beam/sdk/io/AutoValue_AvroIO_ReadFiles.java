package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_ReadFiles<T> extends AvroIO.ReadFiles<T> {

  private final @Nullable Class<T> recordClass;

  private final @Nullable Schema schema;

  private final boolean usesReshuffle;

  private final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler;

  private final long desiredBundleSizeBytes;

  private final boolean inferBeamSchema;

  private final AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;

  private AutoValue_AvroIO_ReadFiles(
      @Nullable Class<T> recordClass,
      @Nullable Schema schema,
      boolean usesReshuffle,
      ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler,
      long desiredBundleSizeBytes,
      boolean inferBeamSchema,
      AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory) {
    this.recordClass = recordClass;
    this.schema = schema;
    this.usesReshuffle = usesReshuffle;
    this.fileExceptionHandler = fileExceptionHandler;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.inferBeamSchema = inferBeamSchema;
    this.datumReaderFactory = datumReaderFactory;
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
  boolean getUsesReshuffle() {
    return usesReshuffle;
  }

  @Override
  ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler getFileExceptionHandler() {
    return fileExceptionHandler;
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
  AvroSource.@Nullable DatumReaderFactory<T> getDatumReaderFactory() {
    return datumReaderFactory;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.ReadFiles) {
      AvroIO.ReadFiles<?> that = (AvroIO.ReadFiles<?>) o;
      return (this.recordClass == null ? that.getRecordClass() == null : this.recordClass.equals(that.getRecordClass()))
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.usesReshuffle == that.getUsesReshuffle()
          && this.fileExceptionHandler.equals(that.getFileExceptionHandler())
          && this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes()
          && this.inferBeamSchema == that.getInferBeamSchema()
          && (this.datumReaderFactory == null ? that.getDatumReaderFactory() == null : this.datumReaderFactory.equals(that.getDatumReaderFactory()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (recordClass == null) ? 0 : recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= usesReshuffle ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= fileExceptionHandler.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    h$ *= 1000003;
    h$ ^= inferBeamSchema ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (datumReaderFactory == null) ? 0 : datumReaderFactory.hashCode();
    return h$;
  }

  @Override
  AvroIO.ReadFiles.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.ReadFiles.Builder<T> {
    private @Nullable Class<T> recordClass;
    private @Nullable Schema schema;
    private Boolean usesReshuffle;
    private ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler;
    private Long desiredBundleSizeBytes;
    private Boolean inferBeamSchema;
    private AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;
    Builder() {
    }
    private Builder(AvroIO.ReadFiles<T> source) {
      this.recordClass = source.getRecordClass();
      this.schema = source.getSchema();
      this.usesReshuffle = source.getUsesReshuffle();
      this.fileExceptionHandler = source.getFileExceptionHandler();
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      this.inferBeamSchema = source.getInferBeamSchema();
      this.datumReaderFactory = source.getDatumReaderFactory();
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setRecordClass(Class<T> recordClass) {
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setUsesReshuffle(boolean usesReshuffle) {
      this.usesReshuffle = usesReshuffle;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setFileExceptionHandler(ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler) {
      if (fileExceptionHandler == null) {
        throw new NullPointerException("Null fileExceptionHandler");
      }
      this.fileExceptionHandler = fileExceptionHandler;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setInferBeamSchema(boolean inferBeamSchema) {
      this.inferBeamSchema = inferBeamSchema;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setDatumReaderFactory(AvroSource.DatumReaderFactory<T> datumReaderFactory) {
      this.datumReaderFactory = datumReaderFactory;
      return this;
    }
    @Override
    AvroIO.ReadFiles<T> build() {
      if (this.usesReshuffle == null
          || this.fileExceptionHandler == null
          || this.desiredBundleSizeBytes == null
          || this.inferBeamSchema == null) {
        StringBuilder missing = new StringBuilder();
        if (this.usesReshuffle == null) {
          missing.append(" usesReshuffle");
        }
        if (this.fileExceptionHandler == null) {
          missing.append(" fileExceptionHandler");
        }
        if (this.desiredBundleSizeBytes == null) {
          missing.append(" desiredBundleSizeBytes");
        }
        if (this.inferBeamSchema == null) {
          missing.append(" inferBeamSchema");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_ReadFiles<T>(
          this.recordClass,
          this.schema,
          this.usesReshuffle,
          this.fileExceptionHandler,
          this.desiredBundleSizeBytes,
          this.inferBeamSchema,
          this.datumReaderFactory);
    }
  }

}
