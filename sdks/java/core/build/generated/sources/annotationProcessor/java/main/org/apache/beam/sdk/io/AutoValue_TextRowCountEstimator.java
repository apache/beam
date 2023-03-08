package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextRowCountEstimator extends TextRowCountEstimator {

  private final long numSampledBytesPerFile;

  private final byte @Nullable [] delimiters;

  private final String filePattern;

  private final Compression compression;

  private final TextRowCountEstimator.SamplingStrategy samplingStrategy;

  private final EmptyMatchTreatment emptyMatchTreatment;

  private final FileIO.ReadMatches.DirectoryTreatment directoryTreatment;

  private AutoValue_TextRowCountEstimator(
      long numSampledBytesPerFile,
      byte @Nullable [] delimiters,
      String filePattern,
      Compression compression,
      TextRowCountEstimator.SamplingStrategy samplingStrategy,
      EmptyMatchTreatment emptyMatchTreatment,
      FileIO.ReadMatches.DirectoryTreatment directoryTreatment) {
    this.numSampledBytesPerFile = numSampledBytesPerFile;
    this.delimiters = delimiters;
    this.filePattern = filePattern;
    this.compression = compression;
    this.samplingStrategy = samplingStrategy;
    this.emptyMatchTreatment = emptyMatchTreatment;
    this.directoryTreatment = directoryTreatment;
  }

  @Override
  public long getNumSampledBytesPerFile() {
    return numSampledBytesPerFile;
  }

  @SuppressWarnings("mutable")
  @Override
  public byte @Nullable [] getDelimiters() {
    return delimiters;
  }

  @Override
  public String getFilePattern() {
    return filePattern;
  }

  @Override
  public Compression getCompression() {
    return compression;
  }

  @Override
  public TextRowCountEstimator.SamplingStrategy getSamplingStrategy() {
    return samplingStrategy;
  }

  @Override
  public EmptyMatchTreatment getEmptyMatchTreatment() {
    return emptyMatchTreatment;
  }

  @Override
  public FileIO.ReadMatches.DirectoryTreatment getDirectoryTreatment() {
    return directoryTreatment;
  }

  @Override
  public String toString() {
    return "TextRowCountEstimator{"
        + "numSampledBytesPerFile=" + numSampledBytesPerFile + ", "
        + "delimiters=" + Arrays.toString(delimiters) + ", "
        + "filePattern=" + filePattern + ", "
        + "compression=" + compression + ", "
        + "samplingStrategy=" + samplingStrategy + ", "
        + "emptyMatchTreatment=" + emptyMatchTreatment + ", "
        + "directoryTreatment=" + directoryTreatment
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TextRowCountEstimator) {
      TextRowCountEstimator that = (TextRowCountEstimator) o;
      return this.numSampledBytesPerFile == that.getNumSampledBytesPerFile()
          && Arrays.equals(this.delimiters, (that instanceof AutoValue_TextRowCountEstimator) ? ((AutoValue_TextRowCountEstimator) that).delimiters : that.getDelimiters())
          && this.filePattern.equals(that.getFilePattern())
          && this.compression.equals(that.getCompression())
          && this.samplingStrategy.equals(that.getSamplingStrategy())
          && this.emptyMatchTreatment.equals(that.getEmptyMatchTreatment())
          && this.directoryTreatment.equals(that.getDirectoryTreatment());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((numSampledBytesPerFile >>> 32) ^ numSampledBytesPerFile);
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiters);
    h$ *= 1000003;
    h$ ^= filePattern.hashCode();
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= samplingStrategy.hashCode();
    h$ *= 1000003;
    h$ ^= emptyMatchTreatment.hashCode();
    h$ *= 1000003;
    h$ ^= directoryTreatment.hashCode();
    return h$;
  }

  static final class Builder extends TextRowCountEstimator.Builder {
    private Long numSampledBytesPerFile;
    private byte @Nullable [] delimiters;
    private String filePattern;
    private Compression compression;
    private TextRowCountEstimator.SamplingStrategy samplingStrategy;
    private EmptyMatchTreatment emptyMatchTreatment;
    private FileIO.ReadMatches.DirectoryTreatment directoryTreatment;
    Builder() {
    }
    @Override
    public TextRowCountEstimator.Builder setNumSampledBytesPerFile(long numSampledBytesPerFile) {
      this.numSampledBytesPerFile = numSampledBytesPerFile;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setDelimiters(byte @Nullable [] delimiters) {
      this.delimiters = delimiters;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setFilePattern(String filePattern) {
      if (filePattern == null) {
        throw new NullPointerException("Null filePattern");
      }
      this.filePattern = filePattern;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setSamplingStrategy(TextRowCountEstimator.SamplingStrategy samplingStrategy) {
      if (samplingStrategy == null) {
        throw new NullPointerException("Null samplingStrategy");
      }
      this.samplingStrategy = samplingStrategy;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setEmptyMatchTreatment(EmptyMatchTreatment emptyMatchTreatment) {
      if (emptyMatchTreatment == null) {
        throw new NullPointerException("Null emptyMatchTreatment");
      }
      this.emptyMatchTreatment = emptyMatchTreatment;
      return this;
    }
    @Override
    public TextRowCountEstimator.Builder setDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment directoryTreatment) {
      if (directoryTreatment == null) {
        throw new NullPointerException("Null directoryTreatment");
      }
      this.directoryTreatment = directoryTreatment;
      return this;
    }
    @Override
    public TextRowCountEstimator build() {
      if (this.numSampledBytesPerFile == null
          || this.filePattern == null
          || this.compression == null
          || this.samplingStrategy == null
          || this.emptyMatchTreatment == null
          || this.directoryTreatment == null) {
        StringBuilder missing = new StringBuilder();
        if (this.numSampledBytesPerFile == null) {
          missing.append(" numSampledBytesPerFile");
        }
        if (this.filePattern == null) {
          missing.append(" filePattern");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        if (this.samplingStrategy == null) {
          missing.append(" samplingStrategy");
        }
        if (this.emptyMatchTreatment == null) {
          missing.append(" emptyMatchTreatment");
        }
        if (this.directoryTreatment == null) {
          missing.append(" directoryTreatment");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextRowCountEstimator(
          this.numSampledBytesPerFile,
          this.delimiters,
          this.filePattern,
          this.compression,
          this.samplingStrategy,
          this.emptyMatchTreatment,
          this.directoryTreatment);
    }
  }

}
