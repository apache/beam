package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FileIO_Match extends FileIO.Match {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration configuration;

  private AutoValue_FileIO_Match(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration configuration) {
    this.filepattern = filepattern;
    this.configuration = configuration;
  }

  @Override
  @Nullable ValueProvider<String> getFilepattern() {
    return filepattern;
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
    if (o instanceof FileIO.Match) {
      FileIO.Match that = (FileIO.Match) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.configuration.equals(that.getConfiguration());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filepattern == null) ? 0 : filepattern.hashCode();
    h$ *= 1000003;
    h$ ^= configuration.hashCode();
    return h$;
  }

  @Override
  FileIO.Match.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FileIO.Match.Builder {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.MatchConfiguration configuration;
    Builder() {
    }
    private Builder(FileIO.Match source) {
      this.filepattern = source.getFilepattern();
      this.configuration = source.getConfiguration();
    }
    @Override
    FileIO.Match.Builder setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    FileIO.Match.Builder setConfiguration(FileIO.MatchConfiguration configuration) {
      if (configuration == null) {
        throw new NullPointerException("Null configuration");
      }
      this.configuration = configuration;
      return this;
    }
    @Override
    FileIO.Match build() {
      if (this.configuration == null) {
        String missing = " configuration";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FileIO_Match(
          this.filepattern,
          this.configuration);
    }
  }

}
