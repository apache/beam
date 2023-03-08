package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_Read extends TextIO.Read {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration matchConfiguration;

  private final boolean hintMatchesManyFiles;

  private final Compression compression;

  private final byte @Nullable [] delimiter;

  private AutoValue_TextIO_Read(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration matchConfiguration,
      boolean hintMatchesManyFiles,
      Compression compression,
      byte @Nullable [] delimiter) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.hintMatchesManyFiles = hintMatchesManyFiles;
    this.compression = compression;
    this.delimiter = delimiter;
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
  boolean getHintMatchesManyFiles() {
    return hintMatchesManyFiles;
  }

  @Override
  Compression getCompression() {
    return compression;
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
    if (o instanceof TextIO.Read) {
      TextIO.Read that = (TextIO.Read) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.matchConfiguration.equals(that.getMatchConfiguration())
          && this.hintMatchesManyFiles == that.getHintMatchesManyFiles()
          && this.compression.equals(that.getCompression())
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_Read) ? ((AutoValue_TextIO_Read) that).delimiter : that.getDelimiter());
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
    h$ ^= hintMatchesManyFiles ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    return h$;
  }

  @Override
  TextIO.Read.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TextIO.Read.Builder {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.MatchConfiguration matchConfiguration;
    private Boolean hintMatchesManyFiles;
    private Compression compression;
    private byte @Nullable [] delimiter;
    Builder() {
    }
    private Builder(TextIO.Read source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
      this.compression = source.getCompression();
      this.delimiter = source.getDelimiter();
    }
    @Override
    TextIO.Read.Builder setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    TextIO.Read.Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    TextIO.Read.Builder setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      return this;
    }
    @Override
    TextIO.Read.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    TextIO.Read.Builder setDelimiter(byte @Nullable [] delimiter) {
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.Read build() {
      if (this.matchConfiguration == null
          || this.hintMatchesManyFiles == null
          || this.compression == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.hintMatchesManyFiles == null) {
          missing.append(" hintMatchesManyFiles");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_Read(
          this.filepattern,
          this.matchConfiguration,
          this.hintMatchesManyFiles,
          this.compression,
          this.delimiter);
    }
  }

}
