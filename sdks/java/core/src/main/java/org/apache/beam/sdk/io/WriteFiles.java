/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FileResultCoder;
import org.apache.beam.sdk.io.FileBasedSink.WriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.Writer;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.DefaultErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that writes to a {@link FileBasedSink}. A write begins with a sequential
 * global initialization of a sink, followed by a parallel write, and ends with a sequential
 * finalization of the write. The output of a write is {@link PDone}.
 *
 * <p>By default, every bundle in the input {@link PCollection} will be processed by a {@link
 * WriteOperation}, so the number of output will vary based on runner behavior, though at least 1
 * output will always be produced. The exact parallelism of the write stage can be controlled using
 * {@link WriteFiles#withNumShards}, typically used to control how many files are produced or to
 * globally limit the number of workers connecting to an external service. However, this option can
 * often hurt performance: it adds an additional {@link GroupByKey} to the pipeline.
 *
 * <p>Example usage with runner-determined sharding:
 *
 * <pre>{@code p.apply(WriteFiles.to(new MySink(...)));}</pre>
 *
 * <p>Example usage with a fixed number of shards:
 *
 * <pre>{@code p.apply(WriteFiles.to(new MySink(...)).withNumShards(3));}</pre>
 */
@AutoValue
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class WriteFiles<UserT, DestinationT, OutputT>
    extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFiles.class);

  /** For internal use by runners. */
  @Internal
  public static final Class<? extends WriteFiles> CONCRETE_CLASS = AutoValue_WriteFiles.class;

  // The maximum number of file writers to keep open in a single bundle at a time, since file
  // writers default to 64mb buffers. This comes into play when writing per-window files.
  // The first 20 files from a single WriteFiles transform will write files inline in the
  // transform. Anything beyond that might be shuffled.
  // Keep in mind that specific runners may decide to run multiple bundles in parallel, based on
  // their own policy.
  private static final int DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE = 20;

  // When we spill records, shard the output keys to prevent hotspots.
  // We could consider making this a parameter.
  private static final int SPILLED_RECORD_SHARDING_FACTOR = 10;

  // The record count and buffering duration to trigger flushing records to a tmp file. Mainly used
  // for writing unbounded data to avoid generating too many small files.
  public static final int FILE_TRIGGERING_RECORD_COUNT = 100000;
  public static final int FILE_TRIGGERING_BYTE_COUNT = 64 * 1024 * 1024; // 64MiB as of now
  public static final Duration FILE_TRIGGERING_RECORD_BUFFERING_DURATION =
      Duration.standardSeconds(5);

  static final int UNKNOWN_SHARDNUM = -1;
  static final int DUMMY_SHARDNUM = 0;
  private @Nullable WriteOperation<DestinationT, OutputT> writeOperation;

  /**
   * Creates a {@link WriteFiles} transform that writes to the given {@link FileBasedSink}, letting
   * the runner control how many different shards are produced.
   */
  public static <UserT, DestinationT, OutputT> WriteFiles<UserT, DestinationT, OutputT> to(
      FileBasedSink<UserT, DestinationT, OutputT> sink) {
    checkArgument(sink != null, "sink can not be null");
    return new AutoValue_WriteFiles.Builder<UserT, DestinationT, OutputT>()
        .setSink(sink)
        .setComputeNumShards(null)
        .setNumShardsProvider(null)
        .setWindowedWrites(false)
        .setWithAutoSharding(false)
        .setMaxNumWritersPerBundle(DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE)
        .setSideInputs(sink.getDynamicDestinations().getSideInputs())
        .setSkipIfEmpty(false)
        .setBadRecordErrorHandler(new DefaultErrorHandler<>())
        .setBadRecordRouter(BadRecordRouter.THROWING_ROUTER)
        .build();
  }

  public abstract FileBasedSink<UserT, DestinationT, OutputT> getSink();

  public abstract @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>>
      getComputeNumShards();

  // We don't use a side input for static sharding, as we want this value to be updatable
  // when a pipeline is updated.

  public abstract @Nullable ValueProvider<Integer> getNumShardsProvider();

  public abstract boolean getWindowedWrites();

  public abstract boolean getWithAutoSharding();

  abstract int getMaxNumWritersPerBundle();

  abstract boolean getSkipIfEmpty();

  abstract @Nullable Integer getBatchSize();

  abstract @Nullable Integer getBatchSizeBytes();

  abstract @Nullable Duration getBatchMaxBufferingDuration();

  abstract List<PCollectionView<?>> getSideInputs();

  public abstract @Nullable ShardingFunction<UserT, DestinationT> getShardingFunction();

  public abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

  public abstract BadRecordRouter getBadRecordRouter();

  abstract Builder<UserT, DestinationT, OutputT> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<UserT, DestinationT, OutputT> {
    abstract Builder<UserT, DestinationT, OutputT> setSink(
        FileBasedSink<UserT, DestinationT, OutputT> sink);

    abstract Builder<UserT, DestinationT, OutputT> setComputeNumShards(
        @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards);

    abstract Builder<UserT, DestinationT, OutputT> setNumShardsProvider(
        @Nullable ValueProvider<Integer> numShardsProvider);

    abstract Builder<UserT, DestinationT, OutputT> setWindowedWrites(boolean windowedWrites);

    abstract Builder<UserT, DestinationT, OutputT> setWithAutoSharding(boolean withAutoSharding);

    abstract Builder<UserT, DestinationT, OutputT> setMaxNumWritersPerBundle(
        int maxNumWritersPerBundle);

    abstract Builder<UserT, DestinationT, OutputT> setSkipIfEmpty(boolean skipIfEmpty);

    abstract Builder<UserT, DestinationT, OutputT> setBatchSize(@Nullable Integer batchSize);

    abstract Builder<UserT, DestinationT, OutputT> setBatchSizeBytes(
        @Nullable Integer batchSizeBytes);

    abstract Builder<UserT, DestinationT, OutputT> setBatchMaxBufferingDuration(
        @Nullable Duration batchMaxBufferingDuration);

    abstract Builder<UserT, DestinationT, OutputT> setSideInputs(
        List<PCollectionView<?>> sideInputs);

    abstract Builder<UserT, DestinationT, OutputT> setShardingFunction(
        @Nullable ShardingFunction<UserT, DestinationT> shardingFunction);

    abstract Builder<UserT, DestinationT, OutputT> setBadRecordErrorHandler(
        ErrorHandler<BadRecord, ?> badRecordErrorHandler);

    abstract Builder<UserT, DestinationT, OutputT> setBadRecordRouter(
        BadRecordRouter badRecordRouter);

    abstract WriteFiles<UserT, DestinationT, OutputT> build();
  }

  @Override
  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return PCollectionViews.toAdditionalInputs(getSideInputs());
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} using the
   * specified number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link WriteFiles} for
   * more information.
   *
   * <p>A value less than or equal to 0 will be equivalent to the default behavior of
   * runner-determined sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withNumShards(int numShards) {
    if (numShards > 0) {
      return withNumShards(StaticValueProvider.of(numShards));
    }
    return withRunnerDeterminedSharding();
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} using the
   * {@link ValueProvider} specified number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link WriteFiles} for
   * more information.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withNumShards(
      ValueProvider<Integer> numShardsProvider) {
    return toBuilder().setNumShardsProvider(numShardsProvider).build();
  }

  /** Set the maximum number of writers created in a bundle before spilling to shuffle. */
  public WriteFiles<UserT, DestinationT, OutputT> withMaxNumWritersPerBundle(
      int maxNumWritersPerBundle) {
    return toBuilder().setMaxNumWritersPerBundle(maxNumWritersPerBundle).build();
  }

  /** Set this sink to skip writing any files if the PCollection is empty. */
  public WriteFiles<UserT, DestinationT, OutputT> withSkipIfEmpty(boolean skipIfEmpty) {
    return toBuilder().setSkipIfEmpty(skipIfEmpty).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will batch the input records using specified batch size.
   * The default value is {@link #FILE_TRIGGERING_RECORD_COUNT}.
   *
   * <p>This option is used only for writing unbounded data with auto-sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withBatchSize(@Nullable Integer batchSize) {
    return toBuilder().setBatchSize(batchSize).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will batch the input records using specified batch size
   * in bytes. The default value is {@link #FILE_TRIGGERING_BYTE_COUNT}.
   *
   * <p>This option is used only for writing unbounded data with auto-sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withBatchSizeBytes(
      @Nullable Integer batchSizeBytes) {
    return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will batch the input records using specified max
   * buffering duration. The default value is {@link #FILE_TRIGGERING_RECORD_BUFFERING_DURATION}.
   *
   * <p>This option is used only for writing unbounded data with auto-sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withBatchMaxBufferingDuration(
      @Nullable Duration batchMaxBufferingDuration) {
    return toBuilder().setBatchMaxBufferingDuration(batchMaxBufferingDuration).build();
  }

  public WriteFiles<UserT, DestinationT, OutputT> withSideInputs(
      List<PCollectionView<?>> sideInputs) {
    return toBuilder().setSideInputs(sideInputs).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} using the
   * specified {@link PTransform} to compute the number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link WriteFiles} for
   * more information.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withSharding(
      PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding) {
    checkArgument(
        sharding != null, "sharding can not be null. Use withRunnerDeterminedSharding() instead.");
    return toBuilder().setComputeNumShards(sharding).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} with
   * runner-determined sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withRunnerDeterminedSharding() {
    return toBuilder().setComputeNumShards(null).setNumShardsProvider(null).build();
  }

  public WriteFiles<UserT, DestinationT, OutputT> withAutoSharding() {
    checkArgument(
        getComputeNumShards() == null && getNumShardsProvider() == null,
        " sharding should be null if autosharding is specified.");
    return toBuilder().setWithAutoSharding(true).build();
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} using the
   * specified sharding function to assign shard for inputs.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withShardingFunction(
      ShardingFunction<UserT, DestinationT> shardingFunction) {
    return toBuilder().setShardingFunction(shardingFunction).build();
  }

  /**
   * Returns a new {@link WriteFiles} that writes preserves windowing on it's input.
   *
   * <p>If this option is not specified, windowing and triggering are replaced by {@link
   * GlobalWindows} and {@link DefaultTrigger}.
   *
   * <p>If there is no data for a window, no output shards will be generated for that window. If a
   * window triggers multiple times, then more than a single output shard might be generated
   * multiple times; it's up to the sink implementation to keep these output shards unique.
   *
   * <p>This option can only be used if {@link #withNumShards(int)} is also set to a positive value.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withWindowedWrites() {
    return toBuilder().setWindowedWrites(true).build();
  }

  /**
   * Returns a new {@link WriteFiles} that writes all data without spilling, simplifying the
   * pipeline. This option should not be used with {@link #withMaxNumWritersPerBundle(int)} and it
   * will eliminate this limit possibly causing many writers to be opened. Use with caution.
   *
   * <p>This option only applies to writes {@link #withRunnerDeterminedSharding()}.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withNoSpilling() {
    return toBuilder().setMaxNumWritersPerBundle(-1).build();
  }

  public WriteFiles<UserT, DestinationT, OutputT> withSkipIfEmpty() {
    return toBuilder().setSkipIfEmpty(true).build();
  }

  /** See {@link FileIO.Write#withBadRecordErrorHandler(ErrorHandler)} for details on usage. */
  public WriteFiles<UserT, DestinationT, OutputT> withBadRecordErrorHandler(
      ErrorHandler<BadRecord, ?> errorHandler) {
    return toBuilder()
        .setBadRecordErrorHandler(errorHandler)
        .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
        .build();
  }

  @Override
  public void validate(PipelineOptions options) {
    getSink().validate(options);
  }

  @Override
  public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
    if (input.isBounded() == IsBounded.UNBOUNDED) {
      checkArgument(
          getWindowedWrites(),
          "Must use windowed writes when applying %s to an unbounded PCollection",
          WriteFiles.class.getSimpleName());
      // Sharding used to be required due to https://issues.apache.org/jira/browse/BEAM-1438 and
      // similar behavior in other runners. Some runners may support runner determined sharding now.
      // Check merging window here due to https://github.com/apache/beam/issues/20928.
      if (input.getWindowingStrategy().needsMerge()) {
        checkArgument(
            getComputeNumShards() != null || getNumShardsProvider() != null,
            "When applying %s to an unbounded PCollection with merging windows,"
                + " must specify number of output shards explicitly",
            WriteFiles.class.getSimpleName());
      }
    }
    this.writeOperation = getSink().createWriteOperation();
    if (getWindowedWrites()) {
      this.writeOperation.setWindowedWrites();
    } else {
      // Re-window the data into the global window and remove any existing triggers.
      input =
          input.apply(
              "RewindowIntoGlobal",
              Window.<UserT>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
    }

    Coder<DestinationT> destinationCoder;
    try {
      destinationCoder =
          getDynamicDestinations()
              .getDestinationCoderWithDefault(input.getPipeline().getCoderRegistry());
      destinationCoder.verifyDeterministic();
    } catch (CannotProvideCoderException | NonDeterministicException e) {
      throw new RuntimeException(e);
    }
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) input.getWindowingStrategy().getWindowFn().windowCoder();
    FileResultCoder<DestinationT> fileResultCoder =
        FileResultCoder.of(windowCoder, destinationCoder);

    PCollectionView<Integer> numShardsView =
        (getComputeNumShards() == null) ? null : input.apply(getComputeNumShards());

    boolean fixedSharding = getComputeNumShards() != null || getNumShardsProvider() != null;
    PCollection<List<FileResult<DestinationT>>> tempFileResults;
    if (fixedSharding) {
      tempFileResults =
          input
              .apply(
                  "WriteShardedBundlesToTempFiles",
                  new WriteShardedBundlesToTempFiles(
                      destinationCoder, fileResultCoder, numShardsView))
              .apply("GatherTempFileResults", new GatherResults<>(fileResultCoder));
    } else {
      if (input.isBounded() == IsBounded.BOUNDED) {
        tempFileResults =
            input
                .apply(
                    "WriteUnshardedBundlesToTempFiles",
                    new WriteUnshardedBundlesToTempFiles(destinationCoder, fileResultCoder))
                .apply("GatherTempFileResults", new GatherResults<>(fileResultCoder));
      } else {
        tempFileResults =
            input.apply(
                "WriteAutoShardedBundlesToTempFiles",
                new WriteAutoShardedBundlesToTempFiles(
                    destinationCoder,
                    fileResultCoder,
                    getBatchSize(),
                    getBatchSizeBytes(),
                    getBatchMaxBufferingDuration()));
      }
    }

    return tempFileResults.apply(
        "FinalizeTempFileBundles", new FinalizeTempFileBundles(numShardsView, destinationCoder));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("sink", getSink().getClass()).withLabel("WriteFiles Sink"))
        .include("sink", getSink());
    if (getComputeNumShards() != null) {
      builder.include("sharding", getComputeNumShards());
    } else {
      builder.addIfNotNull(
          DisplayData.item("numShards", getNumShardsProvider())
              .withLabel("Fixed Number of Shards"));
    }
  }

  private DynamicDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicDestinations<UserT, DestinationT, OutputT>)
        writeOperation.getSink().getDynamicDestinations();
  }

  private class GatherResults<ResultT>
      extends PTransform<PCollection<ResultT>, PCollection<List<ResultT>>> {
    private final Coder<ResultT> resultCoder;

    private GatherResults(Coder<ResultT> resultCoder) {
      this.resultCoder = resultCoder;
    }

    @Override
    public PCollection<List<ResultT>> expand(PCollection<ResultT> input) {
      if (getWindowedWrites()) {
        // Reshuffle the results to make them stable against retries.
        // Use a single void key to maximize size of bundles for finalization.
        return input
            .apply("Add void key", WithKeys.of((Void) null))
            .apply("Reshuffle", Reshuffle.of())
            .apply("Drop key", Values.create())
            .apply("Gather bundles", ParDo.of(new GatherBundlesPerWindowFn<>()))
            .setCoder(ListCoder.of(resultCoder))
            // Reshuffle one more time to stabilize the contents of the bundle lists to finalize.
            .apply(Reshuffle.viaRandomKey());
      } else {
        // Pass results via a side input rather than reshuffle, because we need to get an empty
        // iterable to finalize if there are no results.
        return input
            .getPipeline()
            .apply(
                "AsPossiblyEmptyList",
                Reify.viewInGlobalWindow(
                    // Insert a reshuffle before taking the view to consolidate the (typically)
                    // one-output-per-bundle writes.
                    // This avoids producing a huge number of tiny files in the case that side
                    // inputs are materialized to disk bundle-by-bundle.
                    input.apply("Consolidate", Reshuffle.viaRandomKey()).apply(View.asIterable()),
                    IterableCoder.of(resultCoder)))
            // View.asIterable() can be (significantly) cheaper than View.asList(), as it does not
            // create a backing indexable view, but we must return a list to maintain update
            // compatibility for consumers that are shared between this path and the streaming one.
            .apply(
                "IterableToList",
                MapElements.via(
                    new SimpleFunction<Iterable<ResultT>, List<ResultT>>(
                        x -> ImmutableList.copyOf(x)) {}))
            .setCoder(ListCoder.of(resultCoder));
      }
    }
  }

  private class WriteUnshardedBundlesToTempFiles
      extends PTransform<PCollection<UserT>, PCollection<FileResult<DestinationT>>> {
    private final Coder<DestinationT> destinationCoder;
    private final Coder<FileResult<DestinationT>> fileResultCoder;

    private WriteUnshardedBundlesToTempFiles(
        Coder<DestinationT> destinationCoder, Coder<FileResult<DestinationT>> fileResultCoder) {
      this.destinationCoder = destinationCoder;
      this.fileResultCoder = fileResultCoder;
    }

    @Override
    public PCollection<FileResult<DestinationT>> expand(PCollection<UserT> input) {
      TupleTag<FileResult<DestinationT>> writtenRecordsTag = new TupleTag<>("writtenRecords");
      TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittenRecordsTag =
          new TupleTag<>("unwrittenRecords");
      Coder<UserT> inputCoder = input.getCoder();
      if (getMaxNumWritersPerBundle() < 0) {
        PCollectionTuple writeTuple =
            input.apply(
                "WritedUnshardedBundles",
                ParDo.of(new WriteUnshardedTempFilesFn(null, destinationCoder, inputCoder))
                    .withSideInputs(getSideInputs())
                    .withOutputTags(
                        writtenRecordsTag, TupleTagList.of(ImmutableList.of(BAD_RECORD_TAG))));
        addErrorCollection(writeTuple);
        return writeTuple.get(writtenRecordsTag).setCoder(fileResultCoder);
      }

      PCollectionTuple writeTuple =
          input.apply(
              "WriteUnshardedBundles",
              ParDo.of(
                      new WriteUnshardedTempFilesFn(
                          unwrittenRecordsTag, destinationCoder, inputCoder))
                  .withSideInputs(getSideInputs())
                  .withOutputTags(
                      writtenRecordsTag,
                      TupleTagList.of(ImmutableList.of(unwrittenRecordsTag, BAD_RECORD_TAG))));
      addErrorCollection(writeTuple);

      PCollection<FileResult<DestinationT>> writtenBundleFiles =
          writeTuple.get(writtenRecordsTag).setCoder(fileResultCoder);
      // Any "spilled" elements are written using WriteShardedBundles. Assign shard numbers in
      // finalize to stay consistent with what WriteWindowedBundles does.
      PCollectionTuple spilledWriteTuple =
          writeTuple
              .get(unwrittenRecordsTag)
              .setCoder(KvCoder.of(ShardedKeyCoder.of(VarIntCoder.of()), input.getCoder()))
              // Here we group by a synthetic shard number in the range [0, spill factor),
              // just for the sake of getting some parallelism within each destination when
              // writing the spilled records, whereas the non-spilled records don't have a shard
              // number assigned at all. Drop the shard number on the spilled records so that
              // shard numbers are assigned together to both the spilled and non-spilled files in
              // finalize.
              .apply("GroupUnwritten", GroupByKey.create())
              .apply(
                  "WriteUnwritten",
                  ParDo.of(new WriteShardsIntoTempFilesFn(input.getCoder()))
                      .withSideInputs(getSideInputs())
                      .withOutputTags(writtenRecordsTag, TupleTagList.of(BAD_RECORD_TAG)));

      addErrorCollection(spilledWriteTuple);

      PCollection<FileResult<DestinationT>> writtenSpilledFiles =
          spilledWriteTuple
              .get(writtenRecordsTag)
              .setCoder(fileResultCoder)
              .apply(
                  "DropShardNum",
                  ParDo.of(
                      new DoFn<FileResult<DestinationT>, FileResult<DestinationT>>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(c.element().withShard(UNKNOWN_SHARDNUM));
                        }
                      }))
              .setCoder(fileResultCoder);
      return PCollectionList.of(writtenBundleFiles)
          .and(writtenSpilledFiles)
          .apply(Flatten.pCollections())
          .setCoder(fileResultCoder);
    }
  }

  /**
   * Writes all the elements in a bundle using a {@link Writer} produced by the {@link
   * WriteOperation} associated with the {@link FileBasedSink}.
   */
  private class WriteUnshardedTempFilesFn extends DoFn<UserT, FileResult<DestinationT>> {
    private final @Nullable TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittenRecordsTag;
    private final Coder<DestinationT> destinationCoder;

    private final Coder<UserT> inputCoder;

    // Initialized in startBundle()
    private @Nullable Map<WriterKey<DestinationT>, Writer<DestinationT, OutputT>> writers;

    private int spilledShardNum = UNKNOWN_SHARDNUM;

    WriteUnshardedTempFilesFn(
        @Nullable TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittenRecordsTag,
        Coder<DestinationT> destinationCoder,
        Coder<UserT> inputCoder) {
      this.unwrittenRecordsTag = unwrittenRecordsTag;
      this.destinationCoder = destinationCoder;
      this.inputCoder = inputCoder;
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      // Reset state in case of reuse. We need to make sure that each bundle gets unique writers.
      writers = Maps.newHashMap();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c, BoundedWindow window, MultiOutputReceiver outputReceiver)
        throws Exception {
      getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
      PaneInfo paneInfo = c.pane();
      // If we are doing windowed writes, we need to ensure that we have separate files for
      // data in different windows/panes. Similar for dynamic writes, make sure that different
      // destinations go to different writers.
      // In the case of unwindowed writes, the window and the pane will always be the same, and
      // the map will only have a single element.
      MaybeDestination<DestinationT> maybeDestination =
          getDestinationWithErrorHandling(c.element(), outputReceiver, inputCoder);
      if (!maybeDestination.isValid) {
        return;
      }
      DestinationT destination = maybeDestination.destination;
      WriterKey<DestinationT> key = new WriterKey<>(window, c.pane(), destination);
      Writer<DestinationT, OutputT> writer = writers.get(key);
      if (writer == null) {
        if (getMaxNumWritersPerBundle() < 0 || writers.size() <= getMaxNumWritersPerBundle()) {
          String uuid = UUID.randomUUID().toString();
          LOG.info(
              "Opening writer {} for window {} pane {} destination {}",
              uuid,
              window,
              paneInfo,
              destination);
          writer = writeOperation.createWriter();
          writer.setDestination(destination);
          writer.open(uuid);
          writers.put(key, writer);
          LOG.debug("Done opening writer");
        } else {
          if (spilledShardNum == UNKNOWN_SHARDNUM) {
            // Cache the random value so we only call ThreadLocalRandom once per DoFn instance.
            spilledShardNum = ThreadLocalRandom.current().nextInt(SPILLED_RECORD_SHARDING_FACTOR);
          } else {
            spilledShardNum = (spilledShardNum + 1) % SPILLED_RECORD_SHARDING_FACTOR;
          }
          outputReceiver
              .get(unwrittenRecordsTag)
              .output(
                  KV.of(
                      ShardedKey.of(
                          hashDestination(destination, destinationCoder), spilledShardNum),
                      c.element()));
          return;
        }
      }
      OutputT formattedRecord =
          formatRecordWithErrorHandling(c.element(), outputReceiver, inputCoder);
      if (formattedRecord == null) {
        return;
      }
      writeOrClose(writer, formattedRecord);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      for (Map.Entry<WriterKey<DestinationT>, Writer<DestinationT, OutputT>> entry :
          writers.entrySet()) {
        WriterKey<DestinationT> key = entry.getKey();
        Writer<DestinationT, OutputT> writer = entry.getValue();
        try {
          writer.close();
        } catch (Exception e) {
          // If anything goes wrong, make sure to delete the temporary file.
          writer.cleanup();
          throw e;
        }
        BoundedWindow window = key.window;
        c.output(
            new FileResult<>(
                writer.getOutputFile(), UNKNOWN_SHARDNUM, window, key.paneInfo, key.destination),
            window.maxTimestamp(),
            window);
      }
    }
  }

  private static <DestinationT, OutputT> void writeOrClose(
      Writer<DestinationT, OutputT> writer, OutputT t) throws Exception {
    try {
      writer.write(t);
    } catch (Exception e) {
      try {
        writer.close();
        // If anything goes wrong, make sure to delete the temporary file.
        writer.cleanup();
      } catch (Exception closeException) {
        if (closeException instanceof InterruptedException) {
          // Do not silently ignore interrupted state.
          Thread.currentThread().interrupt();
        }
        // Do not mask the exception that caused the write to fail.
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  private static class WriterKey<DestinationT> {
    private final BoundedWindow window;
    private final PaneInfo paneInfo;
    private final DestinationT destination;

    WriterKey(BoundedWindow window, PaneInfo paneInfo, DestinationT destination) {
      this.window = window;
      this.paneInfo = paneInfo;
      this.destination = destination;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof WriterKey)) {
        return false;
      }
      WriterKey other = (WriterKey) o;
      return Objects.equal(window, other.window)
          && Objects.equal(paneInfo, other.paneInfo)
          && Objects.equal(destination, other.destination);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(window, paneInfo, destination);
    }
  }

  // Hash the destination in a manner that we can then use as a key in a GBK. Since Java's
  // hashCode isn't guaranteed to be stable across machines, we instead serialize the destination
  // and use murmur3_32 to hash it. We enforce that destinationCoder must be deterministic, so
  // this can be used as a key.
  private static <DestinationT> int hashDestination(
      DestinationT destination, Coder<DestinationT> destinationCoder) throws IOException {
    return Hashing.murmur3_32()
        .hashBytes(CoderUtils.encodeToByteArray(destinationCoder, destination))
        .asInt();
  }

  private static class MaybeDestination<DestinationT> {
    final DestinationT destination;
    final boolean isValid;

    MaybeDestination(DestinationT destination, boolean isValid) {
      this.destination = destination;
      this.isValid = isValid;
    }
  }
  // Utility method to get the dynamic destination based on a record. Returns a MaybeDestination
  // because some implementations of dynamic destinations return null, despite this being prohibited
  // by the interface
  private MaybeDestination<DestinationT> getDestinationWithErrorHandling(
      UserT input, MultiOutputReceiver outputReceiver, Coder<UserT> inputCoder) throws Exception {
    try {
      return new MaybeDestination<>(getDynamicDestinations().getDestination(input), true);
    } catch (Exception e) {
      getBadRecordRouter()
          .route(
              outputReceiver, input, inputCoder, e, "Unable to get dynamic destination for record");
      return new MaybeDestination<>(null, false);
    }
  }

  // Utility method to format a record based on the dynamic destination. If the operation fails, and
  // is output to the bad record router, this returns null
  private @Nullable OutputT formatRecordWithErrorHandling(
      UserT input, MultiOutputReceiver outputReceiver, Coder<UserT> inputCoder) throws Exception {
    try {
      return getDynamicDestinations().formatRecord(input);
    } catch (Exception e) {
      getBadRecordRouter()
          .route(
              outputReceiver,
              input,
              inputCoder,
              e,
              "Unable to format record for Dynamic Destination");
      return null;
    }
  }

  private void addErrorCollection(PCollectionTuple sourceTuple) {
    getBadRecordErrorHandler()
        .addErrorCollection(
            sourceTuple
                .get(BAD_RECORD_TAG)
                .setCoder(BadRecord.getCoder(sourceTuple.getPipeline())));
  }

  private class WriteShardedBundlesToTempFiles
      extends PTransform<PCollection<UserT>, PCollection<FileResult<DestinationT>>> {
    private final Coder<DestinationT> destinationCoder;
    private final Coder<FileResult<DestinationT>> fileResultCoder;
    private final @Nullable PCollectionView<Integer> numShardsView;

    private WriteShardedBundlesToTempFiles(
        Coder<DestinationT> destinationCoder,
        Coder<FileResult<DestinationT>> fileResultCoder,
        @Nullable PCollectionView<Integer> numShardsView) {
      this.destinationCoder = destinationCoder;
      this.fileResultCoder = fileResultCoder;
      this.numShardsView = numShardsView;
    }

    @Override
    public PCollection<FileResult<DestinationT>> expand(PCollection<UserT> input) {
      List<PCollectionView<?>> shardingSideInputs = Lists.newArrayList(getSideInputs());
      if (numShardsView != null) {
        shardingSideInputs.add(numShardsView);
      }

      ShardingFunction<UserT, DestinationT> shardingFunction =
          getShardingFunction() == null
              ? new RandomShardingFunction(destinationCoder)
              : getShardingFunction();

      TupleTag<KV<ShardedKey<Integer>, UserT>> shardedRecords = new TupleTag<>("shardedRecords");
      TupleTag<FileResult<DestinationT>> writtenRecordsTag = new TupleTag<>("writtenRecords");

      PCollectionTuple shardedFiles =
          input.apply(
              "ApplyShardingKey",
              ParDo.of(
                      new ApplyShardingFunctionFn(
                          shardingFunction, numShardsView, input.getCoder()))
                  .withSideInputs(shardingSideInputs)
                  .withOutputTags(shardedRecords, TupleTagList.of(BAD_RECORD_TAG)));
      addErrorCollection(shardedFiles);

      PCollectionTuple writtenFiles =
          shardedFiles
              .get(shardedRecords)
              .setCoder(KvCoder.of(ShardedKeyCoder.of(VarIntCoder.of()), input.getCoder()))
              .apply("GroupIntoShards", GroupByKey.create())
              .apply(
                  "WriteShardsIntoTempFiles",
                  ParDo.of(new WriteShardsIntoTempFilesFn(input.getCoder()))
                      .withSideInputs(getSideInputs())
                      .withOutputTags(writtenRecordsTag, TupleTagList.of(BAD_RECORD_TAG)));
      addErrorCollection(writtenFiles);

      return writtenFiles.get(writtenRecordsTag).setCoder(fileResultCoder);
    }
  }

  private class WriteAutoShardedBundlesToTempFiles
      extends PTransform<PCollection<UserT>, PCollection<List<FileResult<DestinationT>>>> {
    private final Coder<DestinationT> destinationCoder;
    private final Coder<FileResult<DestinationT>> fileResultCoder;
    private final int batchSize;
    private final int batchSizeBytes;
    private final Duration maxBufferingDuration;

    private WriteAutoShardedBundlesToTempFiles(
        Coder<DestinationT> destinationCoder,
        Coder<FileResult<DestinationT>> fileResultCoder,
        Integer batchSize,
        Integer batchSizeBytes,
        Duration maxBufferingDuration) {
      this.destinationCoder = destinationCoder;
      this.fileResultCoder = fileResultCoder;
      this.batchSize = batchSize != null ? batchSize : FILE_TRIGGERING_RECORD_COUNT;
      this.batchSizeBytes = batchSizeBytes != null ? batchSizeBytes : FILE_TRIGGERING_BYTE_COUNT;
      this.maxBufferingDuration =
          maxBufferingDuration != null
              ? maxBufferingDuration
              : FILE_TRIGGERING_RECORD_BUFFERING_DURATION;
    }

    @Override
    public PCollection<List<FileResult<DestinationT>>> expand(PCollection<UserT> input) {
      // Auto-sharding is achieved via GroupIntoBatches.WithShardedKey which shards, groups and at
      // the same time batches the input records. The sharding behavior depends on runners. The
      // batching is per window and we also emit the batches if there are a certain number of
      // records buffered or they have been buffered for a certain time, controlled by
      // FILE_TRIGGERING_RECORD_COUNT and BUFFERING_DURATION respectively.
      //
      // TODO(https://github.com/apache/beam/issues/20928): The implementation doesn't currently
      // work with merging windows.
      TupleTag<KV<Integer, UserT>> shardTag = new TupleTag<>("shardTag");

      PCollectionTuple shardedElements =
          input.apply(
              "KeyedByDestinationHash",
              ParDo.of(new KeyByDestinationHash(input.getCoder(), destinationCoder))
                  .withOutputTags(shardTag, TupleTagList.of(BAD_RECORD_TAG)));
      addErrorCollection(shardedElements);

      PCollection<KV<org.apache.beam.sdk.util.ShardedKey<Integer>, Iterable<UserT>>> shardedInput =
          shardedElements
              .get(shardTag)
              .setCoder(KvCoder.of(VarIntCoder.of(), input.getCoder()))
              .apply(
                  "ShardAndBatch",
                  GroupIntoBatches.<Integer, UserT>ofSize(batchSize)
                      .withByteSize(batchSizeBytes)
                      .withMaxBufferingDuration(maxBufferingDuration)
                      .withShardedKey())
              .setCoder(
                  KvCoder.of(
                      org.apache.beam.sdk.util.ShardedKey.Coder.of(VarIntCoder.of()),
                      IterableCoder.of(input.getCoder())));

      TupleTag<FileResult<DestinationT>> writtenRecordsTag = new TupleTag<>("writtenRecords");
      // Write grouped elements to temp files.
      PCollectionTuple writtenFiles =
          shardedInput
              .apply(
                  "AddDummyShard",
                  MapElements.via(
                      new SimpleFunction<
                          KV<org.apache.beam.sdk.util.ShardedKey<Integer>, Iterable<UserT>>,
                          KV<ShardedKey<Integer>, Iterable<UserT>>>() {
                        @Override
                        public KV<ShardedKey<Integer>, Iterable<UserT>> apply(
                            KV<org.apache.beam.sdk.util.ShardedKey<Integer>, Iterable<UserT>>
                                input) {
                          // Add dummy shard since it is required by WriteShardsIntoTempFilesFn. It
                          // will be dropped after we generate the temp files.
                          return KV.of(
                              ShardedKey.of(input.getKey().getKey(), DUMMY_SHARDNUM),
                              input.getValue());
                        }
                      }))
              .setCoder(
                  KvCoder.of(
                      ShardedKeyCoder.of(VarIntCoder.of()), IterableCoder.of(input.getCoder())))
              .apply(
                  "WriteShardsIntoTempFiles",
                  ParDo.of(new WriteShardsIntoTempFilesFn(input.getCoder()))
                      .withSideInputs(getSideInputs())
                      .withOutputTags(writtenRecordsTag, TupleTagList.of(BAD_RECORD_TAG)));

      addErrorCollection(writtenFiles);

      PCollection<FileResult<DestinationT>> tempFiles =
          writtenFiles
              .get(writtenRecordsTag)
              .setCoder(fileResultCoder)
              .apply(
                  "DropShardNum",
                  ParDo.of(
                      new DoFn<FileResult<DestinationT>, FileResult<DestinationT>>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(c.element().withShard(UNKNOWN_SHARDNUM));
                        }
                      }))
              .setCoder(fileResultCoder);

      // Group temp file results by destinations again to gather all the results in the same window.
      // This is needed since we don't have shard idx associated with each temp file so have to rely
      // on the indexing within a bundle.
      return tempFiles
          .apply(
              "KeyedByDestination",
              WithKeys.of(
                  new SimpleFunction<FileResult<DestinationT>, DestinationT>() {
                    @Override
                    public DestinationT apply(FileResult<DestinationT> input) {
                      return input.getDestination();
                    }
                  }))
          .setCoder(KvCoder.of(destinationCoder, fileResultCoder))
          .apply(GroupByKey.create())
          .apply(
              "ExtractValuesToList",
              ParDo.of(
                  new DoFn<
                      KV<DestinationT, Iterable<FileResult<DestinationT>>>,
                      List<FileResult<DestinationT>>>() {
                    @ProcessElement
                    public void processElement(
                        @Element KV<DestinationT, Iterable<FileResult<DestinationT>>> element,
                        ProcessContext c) {
                      List<FileResult<DestinationT>> result = new ArrayList<>();
                      for (FileResult<DestinationT> e : element.getValue()) {
                        result.add(e);
                      }
                      c.output(result);
                    }
                  }))
          .setCoder(ListCoder.of(fileResultCoder));
    }
  }

  private class KeyByDestinationHash extends DoFn<UserT, KV<Integer, UserT>> {

    private final Coder<UserT> inputCoder;

    private final Coder<DestinationT> destinationCoder;

    public KeyByDestinationHash(Coder<UserT> inputCoder, Coder<DestinationT> destinationCoder) {
      this.inputCoder = inputCoder;
      this.destinationCoder = destinationCoder;
    }

    @ProcessElement
    public void processElement(
        @Element UserT element, ProcessContext context, MultiOutputReceiver outputReceiver)
        throws Exception {
      getDynamicDestinations().setSideInputAccessorFromProcessContext(context);
      MaybeDestination<DestinationT> maybeDestination =
          getDestinationWithErrorHandling(context.element(), outputReceiver, inputCoder);
      if (!maybeDestination.isValid) {
        return;
      }
      DestinationT destination = maybeDestination.destination;
      context.output(KV.of(hashDestination(destination, destinationCoder), element));
    }
  }

  private class RandomShardingFunction implements ShardingFunction<UserT, DestinationT> {
    private final Coder<DestinationT> destinationCoder;

    private int shardNumber;

    RandomShardingFunction(Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
      this.shardNumber = UNKNOWN_SHARDNUM;
    }

    @Override
    public ShardedKey<Integer> assignShardKey(
        DestinationT destination, UserT element, int shardCount) throws Exception {

      if (shardNumber == UNKNOWN_SHARDNUM) {
        // We want to desynchronize the first record sharding key for each instance of
        // ApplyShardingKey, so records in a small PCollection will be statistically balanced.
        shardNumber = ThreadLocalRandom.current().nextInt(shardCount);
      } else {
        shardNumber = (shardNumber + 1) % shardCount;
      }
      // We avoid using destination itself as a sharding key, because destination is often large.
      // e.g. when using {@link DefaultFilenamePolicy}, the destination contains the entire path
      // to the file. Often most of the path is constant across all destinations, just the path
      // suffix is appended by the destination function. Instead we key by a 32-bit hash (carefully
      // chosen to be guaranteed stable), and call getDestination again in the next ParDo to resolve
      // the destinations. This does mean that multiple destinations might end up on the same shard,
      // however the number of collisions should be small, so there's no need to worry about memory
      // issues.
      return ShardedKey.of(hashDestination(destination, destinationCoder), shardNumber);
    }
  }

  private class ApplyShardingFunctionFn extends DoFn<UserT, KV<ShardedKey<Integer>, UserT>> {

    private final ShardingFunction<UserT, DestinationT> shardingFn;
    private final @Nullable PCollectionView<Integer> numShardsView;

    private final Coder<UserT> inputCoder;

    ApplyShardingFunctionFn(
        ShardingFunction<UserT, DestinationT> shardingFn,
        @Nullable PCollectionView<Integer> numShardsView,
        Coder<UserT> inputCoder) {
      this.numShardsView = numShardsView;
      this.shardingFn = shardingFn;
      this.inputCoder = inputCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver outputReceiver)
        throws Exception {
      getDynamicDestinations().setSideInputAccessorFromProcessContext(context);
      final int shardCount;
      if (numShardsView != null) {
        shardCount = context.sideInput(numShardsView);
      } else {
        checkNotNull(getNumShardsProvider());
        shardCount =
            checkNotNull(getNumShardsProvider().get(), "Must have non-null number of shards.");
      }
      checkArgument(
          shardCount > 0,
          "Must have a positive number of shards specified for non-runner-determined sharding."
              + " Got %s",
          shardCount);

      MaybeDestination<DestinationT> maybeDestination =
          getDestinationWithErrorHandling(context.element(), outputReceiver, inputCoder);
      if (!maybeDestination.isValid) {
        return;
      }
      DestinationT destination = maybeDestination.destination;
      ShardedKey<Integer> shardKey =
          shardingFn.assignShardKey(destination, context.element(), shardCount);
      context.output(KV.of(shardKey, context.element()));
    }
  }

  private class WriteShardsIntoTempFilesFn
      extends DoFn<KV<ShardedKey<Integer>, Iterable<UserT>>, FileResult<DestinationT>> {

    private final Coder<UserT> inputCoder;

    public WriteShardsIntoTempFilesFn(Coder<UserT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    private transient List<CompletionStage<Void>> closeFutures = new ArrayList<>();
    private transient List<KV<Instant, FileResult<DestinationT>>> deferredOutput =
        new ArrayList<>();

    // Ensure that transient fields are initialized.
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      closeFutures = new ArrayList<>();
      deferredOutput = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c, BoundedWindow window, MultiOutputReceiver outputReceiver)
        throws Exception {
      getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
      // Since we key by a 32-bit hash of the destination, there might be multiple destinations
      // in this iterable. The number of destinations is generally very small (1000s or less), so
      // there will rarely be hash collisions.
      Map<DestinationT, Writer<DestinationT, OutputT>> writers = Maps.newHashMap();
      for (UserT input : c.element().getValue()) {
        MaybeDestination<DestinationT> maybeDestination =
            getDestinationWithErrorHandling(input, outputReceiver, inputCoder);
        if (!maybeDestination.isValid) {
          continue;
        }
        DestinationT destination = maybeDestination.destination;
        Writer<DestinationT, OutputT> writer = writers.get(destination);
        if (writer == null) {
          String uuid = UUID.randomUUID().toString();
          LOG.info(
              "Opening writer {} for window {} pane {} destination {}",
              uuid,
              window,
              c.pane(),
              destination);
          writer = writeOperation.createWriter();
          writer.setDestination(destination);
          writer.open(uuid);
          writers.put(destination, writer);
        }

        OutputT formattedRecord = formatRecordWithErrorHandling(input, outputReceiver, inputCoder);
        if (formattedRecord == null) {
          continue;
        }
        writeOrClose(writer, formattedRecord);
      }

      // Ensure that we clean-up any prior writers that were being closed as part of this bundle
      // before we return from this processElement call. This allows us to perform the writes/closes
      // in parallel with the prior elements close calls and bounds the amount of data buffered to
      // limit the number of OOMs.
      CompletionStage<List<Void>> pastCloseFutures = MoreFutures.allAsList(closeFutures);
      closeFutures.clear();

      // Close all writers in the background
      for (Map.Entry<DestinationT, Writer<DestinationT, OutputT>> entry : writers.entrySet()) {
        int shard = c.element().getKey().getShardNumber();
        checkArgument(
            shard != UNKNOWN_SHARDNUM,
            "Shard should have been set, but is unset for element %s",
            c.element());
        Writer<DestinationT, OutputT> writer = entry.getValue();
        deferredOutput.add(
            KV.of(
                c.timestamp(),
                new FileResult<>(writer.getOutputFile(), shard, window, c.pane(), entry.getKey())));
        closeWriterInBackground(writer);
      }

      // Block on completing the past closes before returning. We do so after starting the current
      // closes in the background so that they can happen in parallel.
      MoreFutures.get(pastCloseFutures);
    }

    private void closeWriterInBackground(Writer<DestinationT, OutputT> writer) {
      // Close in parallel so flushing of buffered writes to files for many windows happens in
      // parallel.
      closeFutures.add(
          MoreFutures.runAsync(
              () -> {
                try {
                  // Close the writer; if this throws let the error propagate.
                  writer.close();
                } catch (Exception e) {
                  // If anything goes wrong, make sure to delete the temporary file.
                  writer.cleanup();
                  throw e;
                }
              }));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      try {
        MoreFutures.get(MoreFutures.allAsList(closeFutures));
        // If all writers were closed without exception, output the results to the next stage.
        for (KV<Instant, FileResult<DestinationT>> result : deferredOutput) {
          c.output(result.getValue(), result.getKey(), result.getValue().getWindow());
        }
      } finally {
        deferredOutput.clear();
        closeFutures.clear();
      }
    }
  }

  private class FinalizeTempFileBundles
      extends PTransform<
          PCollection<List<FileResult<DestinationT>>>, WriteFilesResult<DestinationT>> {
    private final @Nullable PCollectionView<Integer> numShardsView;
    private final Coder<DestinationT> destinationCoder;

    private FinalizeTempFileBundles(
        @Nullable PCollectionView<Integer> numShardsView, Coder<DestinationT> destinationCoder) {
      this.numShardsView = numShardsView;
      this.destinationCoder = destinationCoder;
    }

    @Override
    public WriteFilesResult<DestinationT> expand(
        PCollection<List<FileResult<DestinationT>>> input) {

      List<PCollectionView<?>> finalizeSideInputs = Lists.newArrayList(getSideInputs());
      if (numShardsView != null) {
        finalizeSideInputs.add(numShardsView);
      }
      PCollection<KV<DestinationT, String>> outputFilenames =
          input
              .apply("Finalize", ParDo.of(new FinalizeFn()).withSideInputs(finalizeSideInputs))
              .setCoder(KvCoder.of(destinationCoder, StringUtf8Coder.of()))
              // Reshuffle the filenames to make sure they are observable downstream
              // only after each one is done finalizing.
              .apply(Reshuffle.viaRandomKey());

      TupleTag<KV<DestinationT, String>> perDestinationOutputFilenamesTag =
          new TupleTag<>("perDestinationOutputFilenames");
      return WriteFilesResult.in(
          input.getPipeline(), perDestinationOutputFilenamesTag, outputFilenames);
    }

    private class FinalizeFn
        extends DoFn<List<FileResult<DestinationT>>, KV<DestinationT, String>> {
      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
        @Nullable Integer fixedNumShards;
        if (numShardsView != null) {
          fixedNumShards = c.sideInput(numShardsView);
        } else if (getNumShardsProvider() != null) {
          fixedNumShards = getNumShardsProvider().get();
        } else {
          fixedNumShards = null;
        }
        List<FileResult<DestinationT>> fileResults = Lists.newArrayList(c.element());
        LOG.info("Finalizing {} file results", fileResults.size());
        if (fileResults.isEmpty() && getSkipIfEmpty()) {
          return;
        }
        DestinationT defaultDest = getDynamicDestinations().getDefaultDestination();
        List<KV<FileResult<DestinationT>, ResourceId>> resultsToFinalFilenames =
            fileResults.isEmpty()
                ? writeOperation.finalizeDestination(
                    defaultDest, GlobalWindow.INSTANCE, fixedNumShards, fileResults)
                : finalizeAllDestinations(fileResults, fixedNumShards);
        writeOperation.moveToOutputFiles(resultsToFinalFilenames);
        for (KV<FileResult<DestinationT>, ResourceId> entry : resultsToFinalFilenames) {
          FileResult<DestinationT> res = entry.getKey();
          c.output(KV.of(res.getDestination(), entry.getValue().toString()));
        }
      }
    }
  }

  private List<KV<FileResult<DestinationT>, ResourceId>> finalizeAllDestinations(
      List<FileResult<DestinationT>> fileResults, @Nullable Integer fixedNumShards)
      throws Exception {
    Multimap<KV<DestinationT, BoundedWindow>, FileResult<DestinationT>> res =
        ArrayListMultimap.create();
    for (FileResult<DestinationT> result : fileResults) {
      res.put(KV.of(result.getDestination(), result.getWindow()), result);
    }
    List<KV<FileResult<DestinationT>, ResourceId>> resultsToFinalFilenames = Lists.newArrayList();
    for (Map.Entry<KV<DestinationT, BoundedWindow>, Collection<FileResult<DestinationT>>>
        destEntry : res.asMap().entrySet()) {
      KV<DestinationT, BoundedWindow> destWindow = destEntry.getKey();
      resultsToFinalFilenames.addAll(
          writeOperation.finalizeDestination(
              destWindow.getKey(), destWindow.getValue(), fixedNumShards, destEntry.getValue()));
    }
    return resultsToFinalFilenames;
  }

  private static class GatherBundlesPerWindowFn<T> extends DoFn<T, List<T>> {
    private transient @Nullable Multimap<BoundedWindow, T> bundles = null;

    @StartBundle
    public void startBundle() {
      bundles = ArrayListMultimap.create();
    }

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow w) {
      bundles.put(w, c.element());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      for (BoundedWindow w : bundles.keySet()) {
        c.output(Lists.newArrayList(bundles.get(w)), w.maxTimestamp(), w);
      }
    }
  }
}
