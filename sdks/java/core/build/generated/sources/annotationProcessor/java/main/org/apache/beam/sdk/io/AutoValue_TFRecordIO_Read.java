package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TFRecordIO_Read extends TFRecordIO.Read {

  private final @Nullable ValueProvider<String> filepattern;

  private final boolean validate;

  private final Compression compression;

  private AutoValue_TFRecordIO_Read(
      @Nullable ValueProvider<String> filepattern,
      boolean validate,
      Compression compression) {
    this.filepattern = filepattern;
    this.validate = validate;
    this.compression = compression;
  }

  @Override
  @Nullable ValueProvider<String> getFilepattern() {
    return filepattern;
  }

  @Override
  boolean getValidate() {
    return validate;
  }

  @Override
  Compression getCompression() {
    return compression;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TFRecordIO.Read) {
      TFRecordIO.Read that = (TFRecordIO.Read) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.validate == that.getValidate()
          && this.compression.equals(that.getCompression());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filepattern == null) ? 0 : filepattern.hashCode();
    h$ *= 1000003;
    h$ ^= validate ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    return h$;
  }

  @Override
  TFRecordIO.Read.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TFRecordIO.Read.Builder {
    private @Nullable ValueProvider<String> filepattern;
    private Boolean validate;
    private Compression compression;
    Builder() {
    }
    private Builder(TFRecordIO.Read source) {
      this.filepattern = source.getFilepattern();
      this.validate = source.getValidate();
      this.compression = source.getCompression();
    }
    @Override
    TFRecordIO.Read.Builder setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    TFRecordIO.Read.Builder setValidate(boolean validate) {
      this.validate = validate;
      return this;
    }
    @Override
    TFRecordIO.Read.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    TFRecordIO.Read build() {
      if (this.validate == null
          || this.compression == null) {
        StringBuilder missing = new StringBuilder();
        if (this.validate == null) {
          missing.append(" validate");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TFRecordIO_Read(
          this.filepattern,
          this.validate,
          this.compression);
    }
  }

}
