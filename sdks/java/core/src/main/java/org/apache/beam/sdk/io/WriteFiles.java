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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FileResultCoder;
import org.apache.beam.sdk.io.FileBasedSink.WriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.Writer;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
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
@Experimental(Experimental.Kind.SOURCE_SINK)
public class WriteFiles<UserT, DestinationT, OutputT>
    extends PTransform<PCollection<UserT>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFiles.class);

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

  static final int UNKNOWN_SHARDNUM = -1;
  private FileBasedSink<UserT, DestinationT, OutputT> sink;
  private WriteOperation<DestinationT, OutputT> writeOperation;
  // This allows the number of shards to be dynamically computed based on the input
  // PCollection.
  @Nullable private final PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards;
  // We don't use a side input for static sharding, as we want this value to be updatable
  // when a pipeline is updated.
  @Nullable
  private final ValueProvider<Integer> numShardsProvider;
  private final boolean windowedWrites;
  private int maxNumWritersPerBundle;
  // This is the set of side inputs used by this transform. This is usually populated by the users's
  // DynamicDestinations object.
  private final List<PCollectionView<?>> sideInputs;

  /**
   * Creates a {@link WriteFiles} transform that writes to the given {@link FileBasedSink}, letting
   * the runner control how many different shards are produced.
   */
  public static <UserT, DestinationT, OutputT> WriteFiles<UserT, DestinationT, OutputT> to(
      FileBasedSink<UserT, DestinationT, OutputT> sink) {
    checkNotNull(sink, "sink");
    return new WriteFiles<>(
        sink,
        null /* runner-determined sharding */,
        null,
        false,
        DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE,
        sink.getDynamicDestinations().getSideInputs());
  }

  private WriteFiles(
      FileBasedSink<UserT, DestinationT, OutputT> sink,
      @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> computeNumShards,
      @Nullable ValueProvider<Integer> numShardsProvider,
      boolean windowedWrites,
      int maxNumWritersPerBundle,
      List<PCollectionView<?>> sideInputs) {
    this.sink = sink;
    this.computeNumShards = computeNumShards;
    this.numShardsProvider = numShardsProvider;
    this.windowedWrites = windowedWrites;
    this.maxNumWritersPerBundle = maxNumWritersPerBundle;
    this.sideInputs = sideInputs;
  }

  @Override
  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return PCollectionViews.toAdditionalInputs(sideInputs);
  }

  @Override
  public PDone expand(PCollection<UserT> input) {
    if (input.isBounded() == IsBounded.UNBOUNDED) {
      checkArgument(windowedWrites,
          "Must use windowed writes when applying %s to an unbounded PCollection",
          WriteFiles.class.getSimpleName());
      // The reason for this is https://issues.apache.org/jira/browse/BEAM-1438
      // and similar behavior in other runners.
      checkArgument(
          computeNumShards != null || numShardsProvider != null,
          "When applying %s to an unbounded PCollection, "
              + "must specify number of output shards explicitly",
          WriteFiles.class.getSimpleName());
    }
    this.writeOperation = sink.createWriteOperation();
    this.writeOperation.setWindowedWrites(windowedWrites);
    return createWrite(input);
  }

  @Override
  public void validate(PipelineOptions options) {
    sink.validate(options);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("sink", sink.getClass()).withLabel("WriteFiles Sink"))
        .include("sink", sink);
    if (getSharding() != null) {
      builder.include("sharding", getSharding());
    } else if (getNumShards() != null) {
      String numShards = getNumShards().isAccessible()
          ? getNumShards().get().toString() : getNumShards().toString();
      builder.add(DisplayData.item("numShards", numShards)
          .withLabel("Fixed Number of Shards"));
    }
  }

  /** Returns the {@link FileBasedSink} associated with this PTransform. */
  public FileBasedSink<UserT, DestinationT, OutputT> getSink() {
    return sink;
  }

  /**
   * Returns whether or not to perform windowed writes.
   */
  public boolean isWindowedWrites() {
    return windowedWrites;
  }

  /**
   * Gets the {@link PTransform} that will be used to determine sharding. This can be either a
   * static number of shards (as following a call to {@link #withNumShards(int)}), dynamic (by
   * {@link #withSharding(PTransform)}), or runner-determined (by {@link
   * #withRunnerDeterminedSharding()}.
   */
  @Nullable
  public PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding() {
    return computeNumShards;
  }

  public ValueProvider<Integer> getNumShards() {
    return numShardsProvider;
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
    return new WriteFiles<>(
        sink,
        computeNumShards,
        numShardsProvider,
        windowedWrites,
        maxNumWritersPerBundle,
        sideInputs);
  }

  /** Set the maximum number of writers created in a bundle before spilling to shuffle. */
  public WriteFiles<UserT, DestinationT, OutputT> withMaxNumWritersPerBundle(
      int maxNumWritersPerBundle) {
    return new WriteFiles<>(
        sink,
        computeNumShards,
        numShardsProvider,
        windowedWrites,
        maxNumWritersPerBundle,
        sideInputs);
  }

  public WriteFiles<UserT, DestinationT, OutputT> withSideInputs(
      List<PCollectionView<?>> sideInputs) {
    return new WriteFiles<>(
        sink,
        computeNumShards,
        numShardsProvider,
        windowedWrites,
        maxNumWritersPerBundle,
        sideInputs);
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
    checkNotNull(
        sharding, "Cannot provide null sharding. Use withRunnerDeterminedSharding() instead");
    return new WriteFiles<>(
        sink, sharding, null, windowedWrites, maxNumWritersPerBundle, sideInputs);
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} with
   * runner-determined sharding.
   */
  public WriteFiles<UserT, DestinationT, OutputT> withRunnerDeterminedSharding() {
    return new WriteFiles<>(
        sink, null, null, windowedWrites, maxNumWritersPerBundle, sideInputs);
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
    return new WriteFiles<>(
        sink,
        computeNumShards,
        numShardsProvider,
        true,
        maxNumWritersPerBundle,
        sideInputs);
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
    public boolean equals(Object o) {
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

  /**
   * Writes all the elements in a bundle using a {@link Writer} produced by the {@link
   * WriteOperation} associated with the {@link FileBasedSink}.
   */
  private class WriteBundles extends DoFn<UserT, FileResult<DestinationT>> {
    private final TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittenRecordsTag;
    private final Coder<DestinationT> destinationCoder;
    private final boolean windowedWrites;

    private Map<WriterKey<DestinationT>, Writer<DestinationT, OutputT>> writers;
    private int spilledShardNum = UNKNOWN_SHARDNUM;

    WriteBundles(
        boolean windowedWrites,
        TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittenRecordsTag,
        Coder<DestinationT> destinationCoder) {
      this.windowedWrites = windowedWrites;
      this.unwrittenRecordsTag = unwrittenRecordsTag;
      this.destinationCoder = destinationCoder;
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      // Reset state in case of reuse. We need to make sure that each bundle gets unique writers.
      writers = Maps.newHashMap();
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      sink.getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
      PaneInfo paneInfo = c.pane();
      // If we are doing windowed writes, we need to ensure that we have separate files for
      // data in different windows/panes. Similar for dynamic writes, make sure that different
      // destinations go to different writers.
      // In the case of unwindowed writes, the window and the pane will always be the same, and
      // the map will only have a single element.
      DestinationT destination = sink.getDynamicDestinations().getDestination(c.element());
      WriterKey<DestinationT> key = new WriterKey<>(window, c.pane(), destination);
      Writer<DestinationT, OutputT> writer = writers.get(key);
      if (writer == null) {
        if (writers.size() <= maxNumWritersPerBundle) {
          String uuid = UUID.randomUUID().toString();
          LOG.info(
              "Opening writer {} for write operation {}, window {} pane {} destination {}",
              uuid,
              writeOperation,
              window,
              paneInfo,
              destination);
          writer = writeOperation.createWriter();
          if (windowedWrites) {
            writer.openWindowed(uuid, window, paneInfo, UNKNOWN_SHARDNUM, destination);
          } else {
            writer.openUnwindowed(uuid, UNKNOWN_SHARDNUM, destination);
          }
          writers.put(key, writer);
          LOG.debug("Done opening writer");
        } else {
          if (spilledShardNum == UNKNOWN_SHARDNUM) {
            // Cache the random value so we only call ThreadLocalRandom once per DoFn instance.
            spilledShardNum = ThreadLocalRandom.current().nextInt(SPILLED_RECORD_SHARDING_FACTOR);
          } else {
            spilledShardNum = (spilledShardNum + 1) % SPILLED_RECORD_SHARDING_FACTOR;
          }
          c.output(
              unwrittenRecordsTag,
              KV.of(
                  ShardedKey.of(hashDestination(destination, destinationCoder), spilledShardNum),
                  c.element()));
          return;
        }
      }
      writeOrClose(writer, getSink().getDynamicDestinations().formatRecord(c.element()));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      for (Map.Entry<WriterKey<DestinationT>, Writer<DestinationT, OutputT>> entry :
          writers.entrySet()) {
        Writer<DestinationT, OutputT> writer = entry.getValue();
        FileResult<DestinationT> result;
        try {
          result = writer.close();
        } catch (Exception e) {
          // If anything goes wrong, make sure to delete the temporary file.
          writer.cleanup();
          throw e;
        }
        BoundedWindow window = entry.getKey().window;
        c.output(result, window.maxTimestamp(), window);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(WriteFiles.this);
    }
  }

  enum ShardAssignment { ASSIGN_IN_FINALIZE, ASSIGN_WHEN_WRITING }

  /*
   * Like {@link WriteBundles}, but where the elements for each shard have been collected into a
   * single iterable.
   */
  private class WriteShardedBundles
      extends DoFn<KV<ShardedKey<Integer>, Iterable<UserT>>, FileResult<DestinationT>> {
    ShardAssignment shardNumberAssignment;
    WriteShardedBundles(ShardAssignment shardNumberAssignment) {
      this.shardNumberAssignment = shardNumberAssignment;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      sink.getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
      // Since we key by a 32-bit hash of the destination, there might be multiple destinations
      // in this iterable. The number of destinations is generally very small (1000s or less), so
      // there will rarely be hash collisions.
      Map<DestinationT, Writer<DestinationT, OutputT>> writers = Maps.newHashMap();
      for (UserT input : c.element().getValue()) {
        DestinationT destination = sink.getDynamicDestinations().getDestination(input);
        Writer<DestinationT, OutputT> writer = writers.get(destination);
        if (writer == null) {
          LOG.debug("Opening writer for write operation {}", writeOperation);
          writer = writeOperation.createWriter();
          if (windowedWrites) {
            int shardNumber =
                shardNumberAssignment == ShardAssignment.ASSIGN_WHEN_WRITING
                    ? c.element().getKey().getShardNumber()
                    : UNKNOWN_SHARDNUM;
            writer.openWindowed(
                UUID.randomUUID().toString(), window, c.pane(), shardNumber, destination);
          } else {
            writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, destination);
          }
          LOG.debug("Done opening writer");
          writers.put(destination, writer);
        }
        writeOrClose(writer, getSink().getDynamicDestinations().formatRecord(input));
      }

      // Close all writers.
      for (Map.Entry<DestinationT, Writer<DestinationT, OutputT>> entry : writers.entrySet()) {
        Writer<DestinationT, OutputT> writer = entry.getValue();
        FileResult<DestinationT> result;
        try {
          // Close the writer; if this throws let the error propagate.
          result = writer.close();
          c.output(result);
        } catch (Exception e) {
          // If anything goes wrong, make sure to delete the temporary file.
          writer.cleanup();
          throw e;
        }
      }
      }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(WriteFiles.this);
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

  private class ApplyShardingKey extends DoFn<UserT, KV<ShardedKey<Integer>, UserT>> {
    private final PCollectionView<Integer> numShardsView;
    private final ValueProvider<Integer> numShardsProvider;
    private final Coder<DestinationT> destinationCoder;

    private int shardNumber;

    ApplyShardingKey(
        PCollectionView<Integer> numShardsView,
        ValueProvider<Integer> numShardsProvider,
        Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
      this.numShardsView = numShardsView;
      this.numShardsProvider = numShardsProvider;
      shardNumber = UNKNOWN_SHARDNUM;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final int shardCount;
      if (numShardsView != null) {
        shardCount = context.sideInput(numShardsView);
      } else {
        checkNotNull(numShardsProvider);
        shardCount = numShardsProvider.get();
      }
      checkArgument(
          shardCount > 0,
          "Must have a positive number of shards specified for non-runner-determined sharding."
              + " Got %s",
          shardCount);
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
      DestinationT destination = sink.getDynamicDestinations().getDestination(context.element());
      context.output(
          KV.of(
              ShardedKey.of(hashDestination(destination, destinationCoder), shardNumber),
              context.element()));
    }
  }

  Multimap<DestinationT, FileResult<DestinationT>> perDestinationResults(
      Iterable<FileResult<DestinationT>> results) {
    Multimap<DestinationT, FileResult<DestinationT>> perDestination = ArrayListMultimap.create();
    for (FileResult<DestinationT> result : results) {
      perDestination.put(result.getDestination(), result);
    }
    return perDestination;
  }

  /**
   * A write is performed as sequence of three {@link ParDo}'s.
   *
   * <p>This singleton collection containing the WriteOperation is then used as a side input to a
   * ParDo over the PCollection of elements to write. In this bundle-writing phase, {@link
   * WriteOperation#createWriter} is called to obtain a {@link Writer}. {@link Writer#open} and
   * {@link Writer#close} are called in {@link DoFn.StartBundle} and {@link DoFn.FinishBundle},
   * respectively, and {@link Writer#write} method is called for every element in the bundle. The
   * output of this ParDo is a PCollection of <i>writer result</i> objects (see {@link
   * FileBasedSink} for a description of writer results)-one for each bundle.
   *
   * <p>The final do-once ParDo uses a singleton collection asinput and the collection of writer
   * results as a side-input. In this ParDo, {@link WriteOperation#finalize} is called to finalize
   * the write.
   *
   * <p>If the write of any element in the PCollection fails, {@link Writer#close} will be called
   * before the exception that caused the write to fail is propagated and the write result will be
   * discarded.
   *
   * <p>Since the {@link WriteOperation} is serialized after the initialization ParDo and
   * deserialized in the bundle-writing and finalization phases, any state change to the
   * WriteOperation object that occurs during initialization is visible in the latter phases.
   * However, the WriteOperation is not serialized after the bundle-writing phase. This is why
   * implementations should guarantee that {@link WriteOperation#createWriter} does not mutate
   * WriteOperation).
   */
  private PDone createWrite(PCollection<UserT> input) {
    Pipeline p = input.getPipeline();

    if (!windowedWrites) {
      // Re-window the data into the global window and remove any existing triggers.
      input =
          input.apply(
              Window.<UserT>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
    }

    // Perform the per-bundle writes as a ParDo on the input PCollection (with the
    // WriteOperation as a side input) and collect the results of the writes in a
    // PCollection. There is a dependency between this ParDo and the first (the
    // WriteOperation PCollection as a side input), so this will happen after the
    // initial ParDo.
    PCollection<FileResult<DestinationT>> results;
    final PCollectionView<Integer> numShardsView;
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> shardedWindowCoder =
        (Coder<BoundedWindow>) input.getWindowingStrategy().getWindowFn().windowCoder();
    final Coder<DestinationT> destinationCoder;
    try {
      destinationCoder =
          sink.getDynamicDestinations()
              .getDestinationCoderWithDefault(input.getPipeline().getCoderRegistry());
      destinationCoder.verifyDeterministic();
    } catch (CannotProvideCoderException | NonDeterministicException e) {
      throw new RuntimeException(e);
    }

    if (computeNumShards == null && numShardsProvider == null) {
      numShardsView = null;
      TupleTag<FileResult<DestinationT>> writtenRecordsTag = new TupleTag<>("writtenRecordsTag");
      TupleTag<KV<ShardedKey<Integer>, UserT>> unwrittedRecordsTag =
          new TupleTag<>("unwrittenRecordsTag");
      String writeName = windowedWrites ? "WriteWindowedBundles" : "WriteBundles";
      PCollectionTuple writeTuple =
          input.apply(
              writeName,
              ParDo.of(new WriteBundles(windowedWrites, unwrittedRecordsTag, destinationCoder))
                  .withSideInputs(sideInputs)
                  .withOutputTags(writtenRecordsTag, TupleTagList.of(unwrittedRecordsTag)));
      PCollection<FileResult<DestinationT>> writtenBundleFiles =
          writeTuple
              .get(writtenRecordsTag)
              .setCoder(FileResultCoder.of(shardedWindowCoder, destinationCoder));
      // Any "spilled" elements are written using WriteShardedBundles. Assign shard numbers in
      // finalize to stay consistent with what WriteWindowedBundles does.
      PCollection<FileResult<DestinationT>> writtenGroupedFiles =
          writeTuple
              .get(unwrittedRecordsTag)
              .setCoder(KvCoder.of(ShardedKeyCoder.of(VarIntCoder.of()), input.getCoder()))
              .apply("GroupUnwritten", GroupByKey.<ShardedKey<Integer>, UserT>create())
              .apply(
                  "WriteUnwritten",
                  ParDo.of(new WriteShardedBundles(ShardAssignment.ASSIGN_IN_FINALIZE))
                  .withSideInputs(sideInputs))
              .setCoder(FileResultCoder.of(shardedWindowCoder, destinationCoder));
      results =
          PCollectionList.of(writtenBundleFiles)
              .and(writtenGroupedFiles)
              .apply(Flatten.<FileResult<DestinationT>>pCollections());
    } else {
      List<PCollectionView<?>> shardingSideInputs = Lists.newArrayList();
      if (computeNumShards != null) {
        numShardsView = input.apply(computeNumShards);
        shardingSideInputs.add(numShardsView);
      } else {
        numShardsView = null;
      }
      PCollection<KV<ShardedKey<Integer>, Iterable<UserT>>> sharded =
          input
              .apply(
                  "ApplyShardLabel",
                  ParDo.of(
                          new ApplyShardingKey(
                              numShardsView,
                              (numShardsView != null) ? null : numShardsProvider,
                              destinationCoder))
                      .withSideInputs(shardingSideInputs))
              .setCoder(KvCoder.of(ShardedKeyCoder.of(VarIntCoder.of()), input.getCoder()))
              .apply("GroupIntoShards", GroupByKey.<ShardedKey<Integer>, UserT>create());
      shardedWindowCoder =
          (Coder<BoundedWindow>) sharded.getWindowingStrategy().getWindowFn().windowCoder();
      // Since this path might be used by streaming runners processing triggers, it's important
      // to assign shard numbers here so that they are deterministic. The ASSIGN_IN_FINALIZE
      // strategy works by sorting all FileResult objects and assigning them numbers, which is not
      // guaranteed to work well when processing triggers - if the finalize step retries it might
      // see a different Iterable of FileResult objects, and it will assign different shard numbers.
      results =
          sharded.apply(
              "WriteShardedBundles",
              ParDo.of(new WriteShardedBundles(ShardAssignment.ASSIGN_WHEN_WRITING))
                  .withSideInputs(sideInputs));
    }
    results.setCoder(FileResultCoder.of(shardedWindowCoder, destinationCoder));

    if (windowedWrites) {
      // When processing streaming windowed writes, results will arrive multiple times. This
      // means we can't share the below implementation that turns the results into a side input,
      // as new data arriving into a side input does not trigger the listening DoFn. Instead
      // we aggregate the result set using a singleton GroupByKey, so the DoFn will be triggered
      // whenever new data arrives.
      PCollection<KV<Void, FileResult<DestinationT>>> keyedResults =
          results.apply(
              "AttachSingletonKey", WithKeys.<Void, FileResult<DestinationT>>of((Void) null));
      keyedResults.setCoder(
          KvCoder.of(VoidCoder.of(), FileResultCoder.of(shardedWindowCoder, destinationCoder)));

      // Is the continuation trigger sufficient?
      keyedResults
          .apply("FinalizeGroupByKey", GroupByKey.<Void, FileResult<DestinationT>>create())
          .apply(
              "Finalize",
              ParDo.of(
                  new DoFn<KV<Void, Iterable<FileResult<DestinationT>>>, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                      Set<ResourceId> tempFiles = Sets.newHashSet();
                      Multimap<DestinationT, FileResult<DestinationT>> results =
                          perDestinationResults(c.element().getValue());
                      for (Map.Entry<DestinationT, Collection<FileResult<DestinationT>>> entry :
                          results.asMap().entrySet()) {
                        LOG.info(
                            "Finalizing write operation {} for destination {} num shards: {}.",
                            writeOperation,
                            entry.getKey(),
                            entry.getValue().size());
                        tempFiles.addAll(writeOperation.finalize(entry.getValue()));
                        LOG.debug("Done finalizing write operation for {}.", entry.getKey());
                      }
                      writeOperation.removeTemporaryFiles(tempFiles);
                      LOG.debug("Removed temporary files for {}.", writeOperation);
                    }
                  }));
    } else {
      final PCollectionView<Iterable<FileResult<DestinationT>>> resultsView =
          results.apply(View.<FileResult<DestinationT>>asIterable());
      ImmutableList.Builder<PCollectionView<?>> finalizeSideInputs =
          ImmutableList.<PCollectionView<?>>builder().add(resultsView);
      if (numShardsView != null) {
        finalizeSideInputs.add(numShardsView);
      }
      finalizeSideInputs.addAll(sideInputs);

      // Finalize the write in another do-once ParDo on the singleton collection containing the
      // Writer. The results from the per-bundle writes are given as an Iterable side input.
      // The WriteOperation's state is the same as after its initialization in the first
      // do-once ParDo. There is a dependency between this ParDo and the parallel write (the writer
      // results collection as a side input), so it will happen after the parallel write.
      // For the non-windowed case, we guarantee that  if no data is written but the user has
      // set numShards, then all shards will be written out as empty files. For this reason we
      // use a side input here.
      PCollection<Void> singletonCollection = p.apply(Create.of((Void) null));
      singletonCollection.apply(
          "Finalize",
          ParDo.of(
                  new DoFn<Void, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                      sink.getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
                      // We must always output at least 1 shard, and honor user-specified numShards
                      // if
                      // set.
                      int minShardsNeeded;
                      if (numShardsView != null) {
                        minShardsNeeded = c.sideInput(numShardsView);
                      } else if (numShardsProvider != null) {
                        minShardsNeeded = numShardsProvider.get();
                      } else {
                        minShardsNeeded = 1;
                      }
                      Set<ResourceId> tempFiles = Sets.newHashSet();
                      Multimap<DestinationT, FileResult<DestinationT>> perDestination =
                          perDestinationResults(c.sideInput(resultsView));
                      for (Map.Entry<DestinationT, Collection<FileResult<DestinationT>>> entry :
                          perDestination.asMap().entrySet()) {
                        tempFiles.addAll(
                            finalizeForDestinationFillEmptyShards(
                                entry.getKey(), entry.getValue(), minShardsNeeded));
                      }
                      if (perDestination.isEmpty()) {
                        // If there is no input at all, write empty files to the default
                        // destination.
                        tempFiles.addAll(
                            finalizeForDestinationFillEmptyShards(
                                getSink().getDynamicDestinations().getDefaultDestination(),
                                Lists.<FileResult<DestinationT>>newArrayList(),
                                minShardsNeeded));
                      }
                      writeOperation.removeTemporaryFiles(tempFiles);
                    }
                  })
              .withSideInputs(finalizeSideInputs.build()));
    }
    return PDone.in(input.getPipeline());
  }

  /**
   * Finalize a list of files for a single destination. If a minimum number of shards is needed,
   * this function will generate empty files for this destination to ensure that all shards are
   * generated.
   */
  private Set<ResourceId> finalizeForDestinationFillEmptyShards(
      DestinationT destination, Collection<FileResult<DestinationT>> results, int minShardsNeeded)
      throws Exception {
    checkState(!windowedWrites);

    LOG.info(
        "Finalizing write operation {} for destination {} num shards {}.",
        writeOperation,
        destination,
        results.size());
    int extraShardsNeeded = minShardsNeeded - results.size();
    if (extraShardsNeeded > 0) {
      LOG.info(
          "Creating {} empty output shards in addition to {} written "
              + "for a total of {} for destination {}.",
          extraShardsNeeded,
          results.size(),
          minShardsNeeded,
          destination);
      for (int i = 0; i < extraShardsNeeded; ++i) {
        Writer<DestinationT, OutputT> writer = writeOperation.createWriter();
        // Currently this code path is only called in the unwindowed case.
        writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, destination);
        FileResult<DestinationT> emptyWrite = writer.close();
        results.add(emptyWrite);
      }
      LOG.debug("Done creating extra shards for {}.", destination);
    }
    Set<ResourceId> tempFiles = writeOperation.finalize(results);
    LOG.debug("Done finalizing write operation {} for destination {}", writeOperation, destination);
    return tempFiles;
  }
}
