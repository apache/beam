package org.apache.beam.sdk.io;

import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_WriteFiles<UserT, DestinationT, OutputT> extends WriteFiles<UserT, DestinationT, OutputT> {

  private final FileBasedSink<UserT, DestinationT, OutputT> sink;

  private final @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards;

  private final @Nullable ValueProvider<Integer> numShardsProvider;

  private final boolean windowedWrites;

  private final int maxNumWritersPerBundle;

  private final boolean skipIfEmpty;

  private final List<PCollectionView<?>> sideInputs;

  private final @Nullable ShardingFunction<UserT, DestinationT> shardingFunction;

  private AutoValue_WriteFiles(
      FileBasedSink<UserT, DestinationT, OutputT> sink,
      @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards,
      @Nullable ValueProvider<Integer> numShardsProvider,
      boolean windowedWrites,
      int maxNumWritersPerBundle,
      boolean skipIfEmpty,
      List<PCollectionView<?>> sideInputs,
      @Nullable ShardingFunction<UserT, DestinationT> shardingFunction) {
    this.sink = sink;
    this.computeNumShards = computeNumShards;
    this.numShardsProvider = numShardsProvider;
    this.windowedWrites = windowedWrites;
    this.maxNumWritersPerBundle = maxNumWritersPerBundle;
    this.skipIfEmpty = skipIfEmpty;
    this.sideInputs = sideInputs;
    this.shardingFunction = shardingFunction;
  }

  @Override
  public FileBasedSink<UserT, DestinationT, OutputT> getSink() {
    return sink;
  }

  @Override
  public @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> getComputeNumShards() {
    return computeNumShards;
  }

  @Override
  public @Nullable ValueProvider<Integer> getNumShardsProvider() {
    return numShardsProvider;
  }

  @Override
  public boolean getWindowedWrites() {
    return windowedWrites;
  }

  @Override
  int getMaxNumWritersPerBundle() {
    return maxNumWritersPerBundle;
  }

  @Override
  boolean getSkipIfEmpty() {
    return skipIfEmpty;
  }

  @Override
  List<PCollectionView<?>> getSideInputs() {
    return sideInputs;
  }

  @Override
  public @Nullable ShardingFunction<UserT, DestinationT> getShardingFunction() {
    return shardingFunction;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof WriteFiles) {
      WriteFiles<?, ?, ?> that = (WriteFiles<?, ?, ?>) o;
      return this.sink.equals(that.getSink())
          && (this.computeNumShards == null ? that.getComputeNumShards() == null : this.computeNumShards.equals(that.getComputeNumShards()))
          && (this.numShardsProvider == null ? that.getNumShardsProvider() == null : this.numShardsProvider.equals(that.getNumShardsProvider()))
          && this.windowedWrites == that.getWindowedWrites()
          && this.maxNumWritersPerBundle == that.getMaxNumWritersPerBundle()
          && this.skipIfEmpty == that.getSkipIfEmpty()
          && this.sideInputs.equals(that.getSideInputs())
          && (this.shardingFunction == null ? that.getShardingFunction() == null : this.shardingFunction.equals(that.getShardingFunction()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= sink.hashCode();
    h$ *= 1000003;
    h$ ^= (computeNumShards == null) ? 0 : computeNumShards.hashCode();
    h$ *= 1000003;
    h$ ^= (numShardsProvider == null) ? 0 : numShardsProvider.hashCode();
    h$ *= 1000003;
    h$ ^= windowedWrites ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= maxNumWritersPerBundle;
    h$ *= 1000003;
    h$ ^= skipIfEmpty ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= sideInputs.hashCode();
    h$ *= 1000003;
    h$ ^= (shardingFunction == null) ? 0 : shardingFunction.hashCode();
    return h$;
  }

  @Override
  WriteFiles.Builder<UserT, DestinationT, OutputT> toBuilder() {
    return new Builder<UserT, DestinationT, OutputT>(this);
  }

  static final class Builder<UserT, DestinationT, OutputT> extends WriteFiles.Builder<UserT, DestinationT, OutputT> {
    private FileBasedSink<UserT, DestinationT, OutputT> sink;
    private @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards;
    private @Nullable ValueProvider<Integer> numShardsProvider;
    private Boolean windowedWrites;
    private Integer maxNumWritersPerBundle;
    private Boolean skipIfEmpty;
    private List<PCollectionView<?>> sideInputs;
    private @Nullable ShardingFunction<UserT, DestinationT> shardingFunction;
    Builder() {
    }
    private Builder(WriteFiles<UserT, DestinationT, OutputT> source) {
      this.sink = source.getSink();
      this.computeNumShards = source.getComputeNumShards();
      this.numShardsProvider = source.getNumShardsProvider();
      this.windowedWrites = source.getWindowedWrites();
      this.maxNumWritersPerBundle = source.getMaxNumWritersPerBundle();
      this.skipIfEmpty = source.getSkipIfEmpty();
      this.sideInputs = source.getSideInputs();
      this.shardingFunction = source.getShardingFunction();
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setSink(FileBasedSink<UserT, DestinationT, OutputT> sink) {
      if (sink == null) {
        throw new NullPointerException("Null sink");
      }
      this.sink = sink;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setComputeNumShards(@Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards) {
      this.computeNumShards = computeNumShards;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setNumShardsProvider(@Nullable ValueProvider<Integer> numShardsProvider) {
      this.numShardsProvider = numShardsProvider;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setMaxNumWritersPerBundle(int maxNumWritersPerBundle) {
      this.maxNumWritersPerBundle = maxNumWritersPerBundle;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setSkipIfEmpty(boolean skipIfEmpty) {
      this.skipIfEmpty = skipIfEmpty;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setSideInputs(List<PCollectionView<?>> sideInputs) {
      if (sideInputs == null) {
        throw new NullPointerException("Null sideInputs");
      }
      this.sideInputs = sideInputs;
      return this;
    }
    @Override
    WriteFiles.Builder<UserT, DestinationT, OutputT> setShardingFunction(@Nullable ShardingFunction<UserT, DestinationT> shardingFunction) {
      this.shardingFunction = shardingFunction;
      return this;
    }
    @Override
    WriteFiles<UserT, DestinationT, OutputT> build() {
      if (this.sink == null
          || this.windowedWrites == null
          || this.maxNumWritersPerBundle == null
          || this.skipIfEmpty == null
          || this.sideInputs == null) {
        StringBuilder missing = new StringBuilder();
        if (this.sink == null) {
          missing.append(" sink");
        }
        if (this.windowedWrites == null) {
          missing.append(" windowedWrites");
        }
        if (this.maxNumWritersPerBundle == null) {
          missing.append(" maxNumWritersPerBundle");
        }
        if (this.skipIfEmpty == null) {
          missing.append(" skipIfEmpty");
        }
        if (this.sideInputs == null) {
          missing.append(" sideInputs");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_WriteFiles<UserT, DestinationT, OutputT>(
          this.sink,
          this.computeNumShards,
          this.numShardsProvider,
          this.windowedWrites,
          this.maxNumWritersPerBundle,
          this.skipIfEmpty,
          this.sideInputs,
          this.shardingFunction);
    }
  }

}
