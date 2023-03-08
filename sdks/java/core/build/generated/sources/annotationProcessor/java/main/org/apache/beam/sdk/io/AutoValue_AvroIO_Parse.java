package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_Parse<T> extends AvroIO.Parse<T> {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration matchConfiguration;

  private final SerializableFunction<GenericRecord, T> parseFn;

  private final @Nullable Coder<T> coder;

  private final boolean hintMatchesManyFiles;

  private AutoValue_AvroIO_Parse(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration matchConfiguration,
      SerializableFunction<GenericRecord, T> parseFn,
      @Nullable Coder<T> coder,
      boolean hintMatchesManyFiles) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.parseFn = parseFn;
    this.coder = coder;
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
  SerializableFunction<GenericRecord, T> getParseFn() {
    return parseFn;
  }

  @Override
  @Nullable Coder<T> getCoder() {
    return coder;
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
    if (o instanceof AvroIO.Parse) {
      AvroIO.Parse<?> that = (AvroIO.Parse<?>) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.matchConfiguration.equals(that.getMatchConfiguration())
          && this.parseFn.equals(that.getParseFn())
          && (this.coder == null ? that.getCoder() == null : this.coder.equals(that.getCoder()))
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
    h$ ^= parseFn.hashCode();
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= hintMatchesManyFiles ? 1231 : 1237;
    return h$;
  }

  @Override
  AvroIO.Parse.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.Parse.Builder<T> {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.MatchConfiguration matchConfiguration;
    private SerializableFunction<GenericRecord, T> parseFn;
    private @Nullable Coder<T> coder;
    private Boolean hintMatchesManyFiles;
    Builder() {
    }
    private Builder(AvroIO.Parse<T> source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.parseFn = source.getParseFn();
      this.coder = source.getCoder();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
    }
    @Override
    AvroIO.Parse.Builder<T> setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    AvroIO.Parse.Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    AvroIO.Parse.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn) {
      if (parseFn == null) {
        throw new NullPointerException("Null parseFn");
      }
      this.parseFn = parseFn;
      return this;
    }
    @Override
    AvroIO.Parse.Builder<T> setCoder(Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    AvroIO.Parse.Builder<T> setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      return this;
    }
    @Override
    AvroIO.Parse<T> build() {
      if (this.matchConfiguration == null
          || this.parseFn == null
          || this.hintMatchesManyFiles == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.parseFn == null) {
          missing.append(" parseFn");
        }
        if (this.hintMatchesManyFiles == null) {
          missing.append(" hintMatchesManyFiles");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_Parse<T>(
          this.filepattern,
          this.matchConfiguration,
          this.parseFn,
          this.coder,
          this.hintMatchesManyFiles);
    }
  }

}
