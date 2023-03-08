package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_ParseAll<T> extends AvroIO.ParseAll<T> {

  private final FileIO.MatchConfiguration matchConfiguration;

  private final SerializableFunction<GenericRecord, T> parseFn;

  private final @Nullable Coder<T> coder;

  private final long desiredBundleSizeBytes;

  private AutoValue_AvroIO_ParseAll(
      FileIO.MatchConfiguration matchConfiguration,
      SerializableFunction<GenericRecord, T> parseFn,
      @Nullable Coder<T> coder,
      long desiredBundleSizeBytes) {
    this.matchConfiguration = matchConfiguration;
    this.parseFn = parseFn;
    this.coder = coder;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
  }

  @Override
  SerializableFunction<GenericRecord, T> getParseFn() {
    return parseFn;
  }

  @Override
  @Nullable Coder<T> getCoder() {
    return coder;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.ParseAll) {
      AvroIO.ParseAll<?> that = (AvroIO.ParseAll<?>) o;
      return this.matchConfiguration.equals(that.getMatchConfiguration())
          && this.parseFn.equals(that.getParseFn())
          && (this.coder == null ? that.getCoder() == null : this.coder.equals(that.getCoder()))
          && this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= parseFn.hashCode();
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    return h$;
  }

  @Override
  AvroIO.ParseAll.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.ParseAll.Builder<T> {
    private FileIO.MatchConfiguration matchConfiguration;
    private SerializableFunction<GenericRecord, T> parseFn;
    private @Nullable Coder<T> coder;
    private Long desiredBundleSizeBytes;
    Builder() {
    }
    private Builder(AvroIO.ParseAll<T> source) {
      this.matchConfiguration = source.getMatchConfiguration();
      this.parseFn = source.getParseFn();
      this.coder = source.getCoder();
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
    }
    @Override
    AvroIO.ParseAll.Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    AvroIO.ParseAll.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn) {
      if (parseFn == null) {
        throw new NullPointerException("Null parseFn");
      }
      this.parseFn = parseFn;
      return this;
    }
    @Override
    AvroIO.ParseAll.Builder<T> setCoder(Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    AvroIO.ParseAll.Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      return this;
    }
    @Override
    AvroIO.ParseAll<T> build() {
      if (this.matchConfiguration == null
          || this.parseFn == null
          || this.desiredBundleSizeBytes == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.parseFn == null) {
          missing.append(" parseFn");
        }
        if (this.desiredBundleSizeBytes == null) {
          missing.append(" desiredBundleSizeBytes");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_ParseAll<T>(
          this.matchConfiguration,
          this.parseFn,
          this.coder,
          this.desiredBundleSizeBytes);
    }
  }

}
