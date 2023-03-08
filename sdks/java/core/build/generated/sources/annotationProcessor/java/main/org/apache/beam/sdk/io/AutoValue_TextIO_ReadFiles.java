package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_ReadFiles extends TextIO.ReadFiles {

  private final long desiredBundleSizeBytes;

  private final byte @Nullable [] delimiter;

  private AutoValue_TextIO_ReadFiles(
      long desiredBundleSizeBytes,
      byte @Nullable [] delimiter) {
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.delimiter = delimiter;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @SuppressWarnings("mutable")
  @Override
  byte @Nullable [] getDelimiter() {
    return delimiter;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TextIO.ReadFiles) {
      TextIO.ReadFiles that = (TextIO.ReadFiles) o;
      return this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes()
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_ReadFiles) ? ((AutoValue_TextIO_ReadFiles) that).delimiter : that.getDelimiter());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    return h$;
  }

  @Override
  TextIO.ReadFiles.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TextIO.ReadFiles.Builder {
    private Long desiredBundleSizeBytes;
    private byte @Nullable [] delimiter;
    Builder() {
    }
    private Builder(TextIO.ReadFiles source) {
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      this.delimiter = source.getDelimiter();
    }
    @Override
    TextIO.ReadFiles.Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      return this;
    }
    @Override
    TextIO.ReadFiles.Builder setDelimiter(byte @Nullable [] delimiter) {
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.ReadFiles build() {
      if (this.desiredBundleSizeBytes == null) {
        String missing = " desiredBundleSizeBytes";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_ReadFiles(
          this.desiredBundleSizeBytes,
          this.delimiter);
    }
  }

}
