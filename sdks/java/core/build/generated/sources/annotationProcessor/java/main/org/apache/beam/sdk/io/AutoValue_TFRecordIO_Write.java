package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TFRecordIO_Write extends TFRecordIO.Write {

  private final @Nullable ValueProvider<ResourceId> outputPrefix;

  private final @Nullable String filenameSuffix;

  private final int numShards;

  private final @Nullable String shardTemplate;

  private final Compression compression;

  private final boolean noSpilling;

  private AutoValue_TFRecordIO_Write(
      @Nullable ValueProvider<ResourceId> outputPrefix,
      @Nullable String filenameSuffix,
      int numShards,
      @Nullable String shardTemplate,
      Compression compression,
      boolean noSpilling) {
    this.outputPrefix = outputPrefix;
    this.filenameSuffix = filenameSuffix;
    this.numShards = numShards;
    this.shardTemplate = shardTemplate;
    this.compression = compression;
    this.noSpilling = noSpilling;
  }

  @Override
  @Nullable ValueProvider<ResourceId> getOutputPrefix() {
    return outputPrefix;
  }

  @Override
  @Nullable String getFilenameSuffix() {
    return filenameSuffix;
  }

  @Override
  int getNumShards() {
    return numShards;
  }

  @Override
  @Nullable String getShardTemplate() {
    return shardTemplate;
  }

  @Override
  Compression getCompression() {
    return compression;
  }

  @Override
  boolean getNoSpilling() {
    return noSpilling;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TFRecordIO.Write) {
      TFRecordIO.Write that = (TFRecordIO.Write) o;
      return (this.outputPrefix == null ? that.getOutputPrefix() == null : this.outputPrefix.equals(that.getOutputPrefix()))
          && (this.filenameSuffix == null ? that.getFilenameSuffix() == null : this.filenameSuffix.equals(that.getFilenameSuffix()))
          && this.numShards == that.getNumShards()
          && (this.shardTemplate == null ? that.getShardTemplate() == null : this.shardTemplate.equals(that.getShardTemplate()))
          && this.compression.equals(that.getCompression())
          && this.noSpilling == that.getNoSpilling();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (outputPrefix == null) ? 0 : outputPrefix.hashCode();
    h$ *= 1000003;
    h$ ^= (filenameSuffix == null) ? 0 : filenameSuffix.hashCode();
    h$ *= 1000003;
    h$ ^= numShards;
    h$ *= 1000003;
    h$ ^= (shardTemplate == null) ? 0 : shardTemplate.hashCode();
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= noSpilling ? 1231 : 1237;
    return h$;
  }

  @Override
  TFRecordIO.Write.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TFRecordIO.Write.Builder {
    private @Nullable ValueProvider<ResourceId> outputPrefix;
    private @Nullable String filenameSuffix;
    private Integer numShards;
    private @Nullable String shardTemplate;
    private Compression compression;
    private Boolean noSpilling;
    Builder() {
    }
    private Builder(TFRecordIO.Write source) {
      this.outputPrefix = source.getOutputPrefix();
      this.filenameSuffix = source.getFilenameSuffix();
      this.numShards = source.getNumShards();
      this.shardTemplate = source.getShardTemplate();
      this.compression = source.getCompression();
      this.noSpilling = source.getNoSpilling();
    }
    @Override
    TFRecordIO.Write.Builder setOutputPrefix(ValueProvider<ResourceId> outputPrefix) {
      this.outputPrefix = outputPrefix;
      return this;
    }
    @Override
    TFRecordIO.Write.Builder setFilenameSuffix(@Nullable String filenameSuffix) {
      this.filenameSuffix = filenameSuffix;
      return this;
    }
    @Override
    TFRecordIO.Write.Builder setNumShards(int numShards) {
      this.numShards = numShards;
      return this;
    }
    @Override
    TFRecordIO.Write.Builder setShardTemplate(@Nullable String shardTemplate) {
      this.shardTemplate = shardTemplate;
      return this;
    }
    @Override
    TFRecordIO.Write.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    TFRecordIO.Write.Builder setNoSpilling(boolean noSpilling) {
      this.noSpilling = noSpilling;
      return this;
    }
    @Override
    TFRecordIO.Write build() {
      if (this.numShards == null
          || this.compression == null
          || this.noSpilling == null) {
        StringBuilder missing = new StringBuilder();
        if (this.numShards == null) {
          missing.append(" numShards");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        if (this.noSpilling == null) {
          missing.append(" noSpilling");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TFRecordIO_Write(
          this.outputPrefix,
          this.filenameSuffix,
          this.numShards,
          this.shardTemplate,
          this.compression,
          this.noSpilling);
    }
  }

}
