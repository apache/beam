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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FileResultCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that writes to a {@link FileBasedSink}. A write begins with a sequential
 * global initialization of a sink, followed by a parallel write, and ends with a sequential
 * finalization of the write. The output of a write is {@link PDone}.
 *
 * <p>By default, every bundle in the input {@link PCollection} will be processed by a
 * {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation}, so the number of output
 * will vary based on runner behavior, though at least 1 output will always be produced. The
 * exact parallelism of the write stage can be controlled using {@link WriteFiles#withNumShards},
 * typically used to control how many files are produced or to globally limit the number of
 * workers connecting to an external service. However, this option can often hurt performance: it
 * adds an additional {@link GroupByKey} to the pipeline.
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
public class WriteFiles<T> extends PTransform<PCollection<T>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFiles.class);

  private static final int UNKNOWN_SHARDNUM = -1;
  private static final int UNKNOWN_NUMSHARDS = -1;

  private FileBasedSink<T> sink;
  private FileBasedWriteOperation<T> writeOperation;
  // This allows the number of shards to be dynamically computed based on the input
  // PCollection.
  @Nullable
  private final PTransform<PCollection<T>, PCollectionView<Integer>> computeNumShards;
  // We don't use a side input for static sharding, as we want this value to be updatable
  // when a pipeline is updated.
  @Nullable
  private final ValueProvider<Integer> numShardsProvider;
  private boolean windowedWrites;

  /**
   * Creates a {@link WriteFiles} transform that writes to the given {@link FileBasedSink}, letting
   * the runner control how many different shards are produced.
   */
  public static <T> WriteFiles<T> to(FileBasedSink<T> sink) {
    checkNotNull(sink, "sink");
    return new WriteFiles<>(sink, null /* runner-determined sharding */, null, false);
  }

  private WriteFiles(
      FileBasedSink<T> sink,
      @Nullable PTransform<PCollection<T>, PCollectionView<Integer>> computeNumShards,
      @Nullable ValueProvider<Integer> numShardsProvider,
      boolean windowedWrites) {
    this.sink = sink;
    this.computeNumShards = computeNumShards;
    this.numShardsProvider = numShardsProvider;
    this.windowedWrites = windowedWrites;
  }

  @Override
  public PDone expand(PCollection<T> input) {
    checkArgument(IsBounded.BOUNDED == input.isBounded() || windowedWrites,
        "%s can only be applied to an unbounded PCollection if doing windowed writes",
        WriteFiles.class.getSimpleName());
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

  /**
   * Returns the {@link FileBasedSink} associated with this PTransform.
   */
  public FileBasedSink<T> getSink() {
    return sink;
  }

  /**
   * Gets the {@link PTransform} that will be used to determine sharding. This can be either a
   * static number of shards (as following a call to {@link #withNumShards(int)}), dynamic (by
   * {@link #withSharding(PTransform)}), or runner-determined (by {@link
   * #withRunnerDeterminedSharding()}.
   */
  @Nullable
  public PTransform<PCollection<T>, PCollectionView<Integer>> getSharding() {
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
  public WriteFiles<T> withNumShards(int numShards) {
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
  public WriteFiles<T> withNumShards(ValueProvider<Integer> numShardsProvider) {
    return new WriteFiles<>(sink, null, numShardsProvider, windowedWrites);
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} using the
   * specified {@link PTransform} to compute the number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link WriteFiles} for
   * more information.
   */
  public WriteFiles<T> withSharding(PTransform<PCollection<T>, PCollectionView<Integer>> sharding) {
    checkNotNull(
        sharding, "Cannot provide null sharding. Use withRunnerDeterminedSharding() instead");
    return new WriteFiles<>(sink, sharding, null, windowedWrites);
  }

  /**
   * Returns a new {@link WriteFiles} that will write to the current {@link FileBasedSink} with
   * runner-determined sharding.
   */
  public WriteFiles<T> withRunnerDeterminedSharding() {
    return new WriteFiles<>(sink, null, null, windowedWrites);
  }

  /**
   * Returns a new {@link WriteFiles} that writes preserves windowing on it's input.
   *
   * <p>If this option is not specified, windowing and triggering are replaced by
   * {@link GlobalWindows} and {@link DefaultTrigger}.
   *
   * <p>If there is no data for a window, no output shards will be generated for that window.
   * If a window triggers multiple times, then more than a single output shard might be
   * generated multiple times; it's up to the sink implementation to keep these output shards
   * unique.
   *
   * <p>This option can only be used if {@link #withNumShards(int)} is also set to a
   * positive value.
   */
  public WriteFiles<T> withWindowedWrites() {
    return new WriteFiles<>(sink, computeNumShards, numShardsProvider, true);
  }

  /**
   * Writes all the elements in a bundle using a {@link FileBasedWriter} produced by the
   * {@link FileBasedSink.FileBasedWriteOperation} associated with the {@link FileBasedSink}.
   */
  private class WriteBundles extends DoFn<T, FileResult> {
    // Writer that will write the records in this bundle. Lazily
    // initialized in processElement.
    private FileBasedWriter<T> writer = null;

    WriteBundles() {
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      // Lazily initialize the Writer
      if (writer == null) {
        LOG.info("Opening writer for write operation {}", writeOperation);
        writer = writeOperation.createWriter();

        if (windowedWrites) {
          writer.openWindowed(UUID.randomUUID().toString(), window, c.pane(), UNKNOWN_SHARDNUM,
              UNKNOWN_NUMSHARDS);
        } else {
          writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, UNKNOWN_NUMSHARDS);
        }
        LOG.debug("Done opening writer {} for operation {}", writer, writeOperation);
      }
      try {
        writer.write(c.element());
      } catch (Exception e) {
        // Discard write result and close the write.
        try {
          writer.close();
          // The writer does not need to be reset, as this DoFn cannot be reused.
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

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
      if (writer != null) {
        FileResult result = writer.close();
        c.output(result);
        // Reset state in case of reuse.
        writer = null;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(WriteFiles.this);
    }
  }

  /**
   * Like {@link WriteBundles}, but where the elements for each shard have been collected into
   * a single iterable.
   *
   * @see WriteBundles
   */
  private class WriteShardedBundles extends DoFn<KV<Integer, Iterable<T>>, FileResult> {
    private final PCollectionView<Integer> numShardsView;

    WriteShardedBundles(PCollectionView<Integer> numShardsView) {
      this.numShardsView = numShardsView;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      int numShards = numShardsView != null ? c.sideInput(numShardsView) : getNumShards().get();
      // In a sharded write, single input element represents one shard. We can open and close
      // the writer in each call to processElement.
      LOG.info("Opening writer for write operation {}", writeOperation);
      FileBasedWriter<T> writer = writeOperation.createWriter();
      if (windowedWrites) {
        writer.openWindowed(UUID.randomUUID().toString(), window, c.pane(), c.element().getKey(),
            numShards);
      } else {
        writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, UNKNOWN_NUMSHARDS);
      }
      LOG.debug("Done opening writer {} for operation {}", writer, writeOperation);

      try {
        try {
          for (T t : c.element().getValue()) {
            writer.write(t);
          }
        } catch (Exception e) {
          try {
            writer.close();
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

        // Close the writer; if this throws let the error propagate.
        FileResult result = writer.close();
        c.output(result);
      } catch (Exception e) {
        // If anything goes wrong, make sure to delete the temporary file.
        writer.cleanup();
        throw e;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(WriteFiles.this);
    }
  }

  private static class ApplyShardingKey<T> extends DoFn<T, KV<Integer, T>> {
    private final PCollectionView<Integer> numShardsView;
    private final ValueProvider<Integer> numShardsProvider;
    private int shardNumber;

    ApplyShardingKey(PCollectionView<Integer> numShardsView,
                     ValueProvider<Integer> numShardsProvider) {
      this.numShardsView = numShardsView;
      this.numShardsProvider = numShardsProvider;
      shardNumber = UNKNOWN_SHARDNUM;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      int shardCount = 0;
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
      context.output(KV.of(shardNumber, context.element()));
    }
  }

  /**
   * A write is performed as sequence of three {@link ParDo}'s.
   *
   * <p>This singleton collection containing the FileBasedWriteOperation is then used as a side
   * input to a ParDo over the PCollection of elements to write. In this bundle-writing phase,
   * {@link FileBasedWriteOperation#createWriter} is called to obtain a {@link FileBasedWriter}.
   * {@link FileBasedWriter#open} and {@link FileBasedWriter#close} are called in
   * {@link DoFn.StartBundle} and {@link DoFn.FinishBundle}, respectively, and
   * {@link FileBasedWriter#write} method is called for every element in the bundle. The output
   * of this ParDo is a PCollection of <i>writer result</i> objects (see {@link FileBasedSink}
   * for a description of writer results)-one for each bundle.
   *
   * <p>The final do-once ParDo uses a singleton collection asinput and the collection of writer
   * results as a side-input. In this ParDo, {@link FileBasedWriteOperation#finalize} is called
   * to finalize the write.
   *
   * <p>If the write of any element in the PCollection fails, {@link FileBasedWriter#close} will be
   * called before the exception that caused the write to fail is propagated and the write result
   * will be discarded.
   *
   * <p>Since the {@link FileBasedWriteOperation} is serialized after the initialization ParDo and
   * deserialized in the bundle-writing and finalization phases, any state change to the
   * FileBasedWriteOperation object that occurs during initialization is visible in the latter
   * phases. However, the FileBasedWriteOperation is not serialized after the bundle-writing
   * phase. This is why implementations should guarantee that
   * {@link FileBasedWriteOperation#createWriter} does not mutate FileBasedWriteOperation).
   */
  private PDone createWrite(PCollection<T> input) {
    Pipeline p = input.getPipeline();

    if (!windowedWrites) {
      // Re-window the data into the global window and remove any existing triggers.
      input =
          input.apply(
              Window.<T>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
    }


    // Perform the per-bundle writes as a ParDo on the input PCollection (with the
    // FileBasedWriteOperation as a side input) and collect the results of the writes in a
    // PCollection. There is a dependency between this ParDo and the first (the
    // FileBasedWriteOperation PCollection as a side input), so this will happen after the
    // initial ParDo.
    PCollection<FileResult> results;
    final PCollectionView<Integer> numShardsView;
    if (computeNumShards == null && numShardsProvider == null) {
      if (windowedWrites) {
        throw new IllegalStateException("When doing windowed writes, numShards must be set"
            + "explicitly to a positive value");
      }
      numShardsView = null;
      results = input
          .apply("WriteBundles",
              ParDo.of(new WriteBundles()));
    } else {
      if (computeNumShards != null) {
        numShardsView = input.apply(computeNumShards);
        results  = input
            .apply("ApplyShardLabel", ParDo.of(
                new ApplyShardingKey<T>(numShardsView, null)).withSideInputs(numShardsView))
            .apply("GroupIntoShards", GroupByKey.<Integer, T>create())
            .apply("WriteShardedBundles",
                ParDo.of(new WriteShardedBundles(numShardsView))
                    .withSideInputs(numShardsView));
      } else {
        numShardsView = null;
        results = input
            .apply("ApplyShardLabel", ParDo.of(new ApplyShardingKey<T>(null, numShardsProvider)))
            .apply("GroupIntoShards", GroupByKey.<Integer, T>create())
            .apply("WriteShardedBundles",
                ParDo.of(new WriteShardedBundles(null)));
      }
    }
    results.setCoder(FileResultCoder.of());

    if (windowedWrites) {
      // When processing streaming windowed writes, results will arrive multiple times. This
      // means we can't share the below implementation that turns the results into a side input,
      // as new data arriving into a side input does not trigger the listening DoFn. Instead
      // we aggregate the result set using a singleton GroupByKey, so the DoFn will be triggered
      // whenever new data arrives.
      PCollection<KV<Void, FileResult>> keyedResults =
          results.apply("AttachSingletonKey", WithKeys.<Void, FileResult>of((Void) null));
      keyedResults.setCoder(KvCoder.of(VoidCoder.of(), FileResultCoder.of()));

      // Is the continuation trigger sufficient?
      keyedResults
          .apply("FinalizeGroupByKey", GroupByKey.<Void, FileResult>create())
          .apply("Finalize", ParDo.of(new DoFn<KV<Void, Iterable<FileResult>>, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              LOG.info("Finalizing write operation {}.", writeOperation);
              List<FileResult> results = Lists.newArrayList(c.element().getValue());
              writeOperation.finalize(results);
              LOG.debug("Done finalizing write operation {}", writeOperation);
            }
          }));
    } else {
      final PCollectionView<Iterable<FileResult>> resultsView =
          results.apply(View.<FileResult>asIterable());
      ImmutableList.Builder<PCollectionView<?>> sideInputs =
          ImmutableList.<PCollectionView<?>>builder().add(resultsView);
      if (numShardsView != null) {
        sideInputs.add(numShardsView);
      }

      // Finalize the write in another do-once ParDo on the singleton collection containing the
      // Writer. The results from the per-bundle writes are given as an Iterable side input.
      // The FileBasedWriteOperation's state is the same as after its initialization in the first
      // do-once ParDo. There is a dependency between this ParDo and the parallel write (the writer
      // results collection as a side input), so it will happen after the parallel write.
      // For the non-windowed case, we guarantee that  if no data is written but the user has
      // set numShards, then all shards will be written out as empty files. For this reason we
      // use a side input here.
      PCollection<Void> singletonCollection = p.apply(Create.of((Void) null));
      singletonCollection
          .apply("Finalize", ParDo.of(new DoFn<Void, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              LOG.info("Finalizing write operation {}.", writeOperation);
              List<FileResult> results = Lists.newArrayList(c.sideInput(resultsView));
              LOG.debug("Side input initialized to finalize write operation {}.", writeOperation);

              // We must always output at least 1 shard, and honor user-specified numShards if
              // set.
              int minShardsNeeded;
              if (numShardsView != null) {
                minShardsNeeded = c.sideInput(numShardsView);
              } else if (numShardsProvider != null) {
                minShardsNeeded = numShardsProvider.get();
              } else {
                minShardsNeeded = 1;
              }
              int extraShardsNeeded = minShardsNeeded - results.size();
              if (extraShardsNeeded > 0) {
                LOG.info(
                    "Creating {} empty output shards in addition to {} written for a total of {}.",
                    extraShardsNeeded, results.size(), minShardsNeeded);
                for (int i = 0; i < extraShardsNeeded; ++i) {
                  FileBasedWriter<T> writer = writeOperation.createWriter();
                  writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM,
                      UNKNOWN_NUMSHARDS);
                  FileResult emptyWrite = writer.close();
                  results.add(emptyWrite);
                }
                LOG.debug("Done creating extra shards.");
              }
              writeOperation.finalize(results);
              LOG.debug("Done finalizing write operation {}", writeOperation);
            }
          }).withSideInputs(sideInputs.build()));
    }
    return PDone.in(input.getPipeline());
  }
}
