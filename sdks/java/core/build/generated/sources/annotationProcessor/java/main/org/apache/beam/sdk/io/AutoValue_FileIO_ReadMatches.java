package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FileIO_ReadMatches extends FileIO.ReadMatches {

  private final Compression compression;

  private final FileIO.ReadMatches.DirectoryTreatment directoryTreatment;

  private AutoValue_FileIO_ReadMatches(
      Compression compression,
      FileIO.ReadMatches.DirectoryTreatment directoryTreatment) {
    this.compression = compression;
    this.directoryTreatment = directoryTreatment;
  }

  @Override
  Compression getCompression() {
    return compression;
  }

  @Override
  FileIO.ReadMatches.DirectoryTreatment getDirectoryTreatment() {
    return directoryTreatment;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FileIO.ReadMatches) {
      FileIO.ReadMatches that = (FileIO.ReadMatches) o;
      return this.compression.equals(that.getCompression())
          && this.directoryTreatment.equals(that.getDirectoryTreatment());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= directoryTreatment.hashCode();
    return h$;
  }

  @Override
  FileIO.ReadMatches.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FileIO.ReadMatches.Builder {
    private Compression compression;
    private FileIO.ReadMatches.DirectoryTreatment directoryTreatment;
    Builder() {
    }
    private Builder(FileIO.ReadMatches source) {
      this.compression = source.getCompression();
      this.directoryTreatment = source.getDirectoryTreatment();
    }
    @Override
    FileIO.ReadMatches.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    FileIO.ReadMatches.Builder setDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment directoryTreatment) {
      if (directoryTreatment == null) {
        throw new NullPointerException("Null directoryTreatment");
      }
      this.directoryTreatment = directoryTreatment;
      return this;
    }
    @Override
    FileIO.ReadMatches build() {
      if (this.compression == null
          || this.directoryTreatment == null) {
        StringBuilder missing = new StringBuilder();
        if (this.compression == null) {
          missing.append(" compression");
        }
        if (this.directoryTreatment == null) {
          missing.append(" directoryTreatment");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FileIO_ReadMatches(
          this.compression,
          this.directoryTreatment);
    }
  }

}
