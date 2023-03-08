package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FileIO_Write<DestinationT, UserT> extends FileIO.Write<DestinationT, UserT> {

  private final boolean dynamic;

  private final @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Sink<?>>> sinkFn;

  private final @Nullable Contextful<Contextful.Fn<UserT, ?>> outputFn;

  private final @Nullable Contextful<Contextful.Fn<UserT, DestinationT>> destinationFn;

  private final @Nullable ValueProvider<String> outputDirectory;

  private final @Nullable ValueProvider<String> filenamePrefix;

  private final @Nullable ValueProvider<String> filenameSuffix;

  private final FileIO.Write.@Nullable FileNaming constantFileNaming;

  private final @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Write.FileNaming>> fileNamingFn;

  private final DestinationT emptyWindowDestination;

  private final @Nullable Coder<DestinationT> destinationCoder;

  private final @Nullable ValueProvider<String> tempDirectory;

  private final Compression compression;

  private final @Nullable ValueProvider<Integer> numShards;

  private final @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding;

  private final boolean ignoreWindowing;

  private final boolean noSpilling;

  private AutoValue_FileIO_Write(
      boolean dynamic,
      @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Sink<?>>> sinkFn,
      @Nullable Contextful<Contextful.Fn<UserT, ?>> outputFn,
      @Nullable Contextful<Contextful.Fn<UserT, DestinationT>> destinationFn,
      @Nullable ValueProvider<String> outputDirectory,
      @Nullable ValueProvider<String> filenamePrefix,
      @Nullable ValueProvider<String> filenameSuffix,
      FileIO.Write.@Nullable FileNaming constantFileNaming,
      @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Write.FileNaming>> fileNamingFn,
      DestinationT emptyWindowDestination,
      @Nullable Coder<DestinationT> destinationCoder,
      @Nullable ValueProvider<String> tempDirectory,
      Compression compression,
      @Nullable ValueProvider<Integer> numShards,
      @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding,
      boolean ignoreWindowing,
      boolean noSpilling) {
    this.dynamic = dynamic;
    this.sinkFn = sinkFn;
    this.outputFn = outputFn;
    this.destinationFn = destinationFn;
    this.outputDirectory = outputDirectory;
    this.filenamePrefix = filenamePrefix;
    this.filenameSuffix = filenameSuffix;
    this.constantFileNaming = constantFileNaming;
    this.fileNamingFn = fileNamingFn;
    this.emptyWindowDestination = emptyWindowDestination;
    this.destinationCoder = destinationCoder;
    this.tempDirectory = tempDirectory;
    this.compression = compression;
    this.numShards = numShards;
    this.sharding = sharding;
    this.ignoreWindowing = ignoreWindowing;
    this.noSpilling = noSpilling;
  }

  @Override
  boolean getDynamic() {
    return dynamic;
  }

  @Override
  @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Sink<?>>> getSinkFn() {
    return sinkFn;
  }

  @Override
  @Nullable Contextful<Contextful.Fn<UserT, ?>> getOutputFn() {
    return outputFn;
  }

  @Override
  @Nullable Contextful<Contextful.Fn<UserT, DestinationT>> getDestinationFn() {
    return destinationFn;
  }

  @Override
  @Nullable ValueProvider<String> getOutputDirectory() {
    return outputDirectory;
  }

  @Override
  @Nullable ValueProvider<String> getFilenamePrefix() {
    return filenamePrefix;
  }

  @Override
  @Nullable ValueProvider<String> getFilenameSuffix() {
    return filenameSuffix;
  }

  @Override
  FileIO.Write.@Nullable FileNaming getConstantFileNaming() {
    return constantFileNaming;
  }

  @Override
  @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Write.FileNaming>> getFileNamingFn() {
    return fileNamingFn;
  }

  @Override
  DestinationT getEmptyWindowDestination() {
    return emptyWindowDestination;
  }

  @Override
  @Nullable Coder<DestinationT> getDestinationCoder() {
    return destinationCoder;
  }

  @Override
  @Nullable ValueProvider<String> getTempDirectory() {
    return tempDirectory;
  }

  @Override
  Compression getCompression() {
    return compression;
  }

  @Override
  @Nullable ValueProvider<Integer> getNumShards() {
    return numShards;
  }

  @Override
  @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding() {
    return sharding;
  }

  @Override
  boolean getIgnoreWindowing() {
    return ignoreWindowing;
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
    if (o instanceof FileIO.Write) {
      FileIO.Write<?, ?> that = (FileIO.Write<?, ?>) o;
      return this.dynamic == that.getDynamic()
          && (this.sinkFn == null ? that.getSinkFn() == null : this.sinkFn.equals(that.getSinkFn()))
          && (this.outputFn == null ? that.getOutputFn() == null : this.outputFn.equals(that.getOutputFn()))
          && (this.destinationFn == null ? that.getDestinationFn() == null : this.destinationFn.equals(that.getDestinationFn()))
          && (this.outputDirectory == null ? that.getOutputDirectory() == null : this.outputDirectory.equals(that.getOutputDirectory()))
          && (this.filenamePrefix == null ? that.getFilenamePrefix() == null : this.filenamePrefix.equals(that.getFilenamePrefix()))
          && (this.filenameSuffix == null ? that.getFilenameSuffix() == null : this.filenameSuffix.equals(that.getFilenameSuffix()))
          && (this.constantFileNaming == null ? that.getConstantFileNaming() == null : this.constantFileNaming.equals(that.getConstantFileNaming()))
          && (this.fileNamingFn == null ? that.getFileNamingFn() == null : this.fileNamingFn.equals(that.getFileNamingFn()))
          && (this.emptyWindowDestination == null ? that.getEmptyWindowDestination() == null : this.emptyWindowDestination.equals(that.getEmptyWindowDestination()))
          && (this.destinationCoder == null ? that.getDestinationCoder() == null : this.destinationCoder.equals(that.getDestinationCoder()))
          && (this.tempDirectory == null ? that.getTempDirectory() == null : this.tempDirectory.equals(that.getTempDirectory()))
          && this.compression.equals(that.getCompression())
          && (this.numShards == null ? that.getNumShards() == null : this.numShards.equals(that.getNumShards()))
          && (this.sharding == null ? that.getSharding() == null : this.sharding.equals(that.getSharding()))
          && this.ignoreWindowing == that.getIgnoreWindowing()
          && this.noSpilling == that.getNoSpilling();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= dynamic ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (sinkFn == null) ? 0 : sinkFn.hashCode();
    h$ *= 1000003;
    h$ ^= (outputFn == null) ? 0 : outputFn.hashCode();
    h$ *= 1000003;
    h$ ^= (destinationFn == null) ? 0 : destinationFn.hashCode();
    h$ *= 1000003;
    h$ ^= (outputDirectory == null) ? 0 : outputDirectory.hashCode();
    h$ *= 1000003;
    h$ ^= (filenamePrefix == null) ? 0 : filenamePrefix.hashCode();
    h$ *= 1000003;
    h$ ^= (filenameSuffix == null) ? 0 : filenameSuffix.hashCode();
    h$ *= 1000003;
    h$ ^= (constantFileNaming == null) ? 0 : constantFileNaming.hashCode();
    h$ *= 1000003;
    h$ ^= (fileNamingFn == null) ? 0 : fileNamingFn.hashCode();
    h$ *= 1000003;
    h$ ^= (emptyWindowDestination == null) ? 0 : emptyWindowDestination.hashCode();
    h$ *= 1000003;
    h$ ^= (destinationCoder == null) ? 0 : destinationCoder.hashCode();
    h$ *= 1000003;
    h$ ^= (tempDirectory == null) ? 0 : tempDirectory.hashCode();
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= (numShards == null) ? 0 : numShards.hashCode();
    h$ *= 1000003;
    h$ ^= (sharding == null) ? 0 : sharding.hashCode();
    h$ *= 1000003;
    h$ ^= ignoreWindowing ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= noSpilling ? 1231 : 1237;
    return h$;
  }

  @Override
  FileIO.Write.Builder<DestinationT, UserT> toBuilder() {
    return new Builder<DestinationT, UserT>(this);
  }

  static final class Builder<DestinationT, UserT> extends FileIO.Write.Builder<DestinationT, UserT> {
    private Boolean dynamic;
    private @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Sink<?>>> sinkFn;
    private @Nullable Contextful<Contextful.Fn<UserT, ?>> outputFn;
    private @Nullable Contextful<Contextful.Fn<UserT, DestinationT>> destinationFn;
    private @Nullable ValueProvider<String> outputDirectory;
    private @Nullable ValueProvider<String> filenamePrefix;
    private @Nullable ValueProvider<String> filenameSuffix;
    private FileIO.Write.@Nullable FileNaming constantFileNaming;
    private @Nullable Contextful<Contextful.Fn<DestinationT, FileIO.Write.FileNaming>> fileNamingFn;
    private DestinationT emptyWindowDestination;
    private @Nullable Coder<DestinationT> destinationCoder;
    private @Nullable ValueProvider<String> tempDirectory;
    private Compression compression;
    private @Nullable ValueProvider<Integer> numShards;
    private @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding;
    private Boolean ignoreWindowing;
    private Boolean noSpilling;
    Builder() {
    }
    private Builder(FileIO.Write<DestinationT, UserT> source) {
      this.dynamic = source.getDynamic();
      this.sinkFn = source.getSinkFn();
      this.outputFn = source.getOutputFn();
      this.destinationFn = source.getDestinationFn();
      this.outputDirectory = source.getOutputDirectory();
      this.filenamePrefix = source.getFilenamePrefix();
      this.filenameSuffix = source.getFilenameSuffix();
      this.constantFileNaming = source.getConstantFileNaming();
      this.fileNamingFn = source.getFileNamingFn();
      this.emptyWindowDestination = source.getEmptyWindowDestination();
      this.destinationCoder = source.getDestinationCoder();
      this.tempDirectory = source.getTempDirectory();
      this.compression = source.getCompression();
      this.numShards = source.getNumShards();
      this.sharding = source.getSharding();
      this.ignoreWindowing = source.getIgnoreWindowing();
      this.noSpilling = source.getNoSpilling();
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setDynamic(boolean dynamic) {
      this.dynamic = dynamic;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setSinkFn(Contextful<Contextful.Fn<DestinationT, FileIO.Sink<?>>> sinkFn) {
      this.sinkFn = sinkFn;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setOutputFn(Contextful<Contextful.Fn<UserT, ?>> outputFn) {
      this.outputFn = outputFn;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setDestinationFn(Contextful<Contextful.Fn<UserT, DestinationT>> destinationFn) {
      this.destinationFn = destinationFn;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setOutputDirectory(ValueProvider<String> outputDirectory) {
      this.outputDirectory = outputDirectory;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setFilenamePrefix(ValueProvider<String> filenamePrefix) {
      this.filenamePrefix = filenamePrefix;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setFilenameSuffix(ValueProvider<String> filenameSuffix) {
      this.filenameSuffix = filenameSuffix;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setConstantFileNaming(FileIO.Write.FileNaming constantFileNaming) {
      this.constantFileNaming = constantFileNaming;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setFileNamingFn(Contextful<Contextful.Fn<DestinationT, FileIO.Write.FileNaming>> fileNamingFn) {
      this.fileNamingFn = fileNamingFn;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setEmptyWindowDestination(DestinationT emptyWindowDestination) {
      this.emptyWindowDestination = emptyWindowDestination;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setDestinationCoder(Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setTempDirectory(ValueProvider<String> tempDirectory) {
      this.tempDirectory = tempDirectory;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setNumShards(@Nullable ValueProvider<Integer> numShards) {
      this.numShards = numShards;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setSharding(PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding) {
      this.sharding = sharding;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setIgnoreWindowing(boolean ignoreWindowing) {
      this.ignoreWindowing = ignoreWindowing;
      return this;
    }
    @Override
    FileIO.Write.Builder<DestinationT, UserT> setNoSpilling(boolean noSpilling) {
      this.noSpilling = noSpilling;
      return this;
    }
    @Override
    FileIO.Write<DestinationT, UserT> build() {
      if (this.dynamic == null
          || this.compression == null
          || this.ignoreWindowing == null
          || this.noSpilling == null) {
        StringBuilder missing = new StringBuilder();
        if (this.dynamic == null) {
          missing.append(" dynamic");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        if (this.ignoreWindowing == null) {
          missing.append(" ignoreWindowing");
        }
        if (this.noSpilling == null) {
          missing.append(" noSpilling");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FileIO_Write<DestinationT, UserT>(
          this.dynamic,
          this.sinkFn,
          this.outputFn,
          this.destinationFn,
          this.outputDirectory,
          this.filenamePrefix,
          this.filenameSuffix,
          this.constantFileNaming,
          this.fileNamingFn,
          this.emptyWindowDestination,
          this.destinationCoder,
          this.tempDirectory,
          this.compression,
          this.numShards,
          this.sharding,
          this.ignoreWindowing,
          this.noSpilling);
    }
  }

}
