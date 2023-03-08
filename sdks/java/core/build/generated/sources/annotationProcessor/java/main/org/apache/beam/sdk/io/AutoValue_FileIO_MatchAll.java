package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FileIO_MatchAll extends FileIO.MatchAll {

  private final FileIO.MatchConfiguration configuration;

  private AutoValue_FileIO_MatchAll(
      FileIO.MatchConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  FileIO.MatchConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FileIO.MatchAll) {
      FileIO.MatchAll that = (FileIO.MatchAll) o;
      return this.configuration.equals(that.getConfiguration());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= configuration.hashCode();
    return h$;
  }

  @Override
  FileIO.MatchAll.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FileIO.MatchAll.Builder {
    private FileIO.MatchConfiguration configuration;
    Builder() {
    }
    private Builder(FileIO.MatchAll source) {
      this.configuration = source.getConfiguration();
    }
    @Override
    FileIO.MatchAll.Builder setConfiguration(FileIO.MatchConfiguration configuration) {
      if (configuration == null) {
        throw new NullPointerException("Null configuration");
      }
      this.configuration = configuration;
      return this;
    }
    @Override
    FileIO.MatchAll build() {
      if (this.configuration == null) {
        String missing = " configuration";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FileIO_MatchAll(
          this.configuration);
    }
  }

}
