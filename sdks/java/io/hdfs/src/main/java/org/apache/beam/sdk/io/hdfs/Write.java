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
package org.apache.beam.sdk.io.hdfs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.hdfs.Sink.WriteOperation;
import org.apache.beam.sdk.io.hdfs.Sink.Writer;
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
 * This class is deprecated, and only exists currently for HDFSFileSink.
 */
@Deprecated
public class Write<T> extends PTransform<PCollection<T>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(Write.class);

  private static final int UNKNOWN_SHARDNUM = -1;
  private static final int UNKNOWN_NUMSHARDS = -1;

  private final Sink<T> sink;
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
   * Creates a {@link Write} transform that writes to the given {@link Sink}, letting the runner
   * control how many different shards are produced.
   */
  public static <T> Write<T> to(Sink<T> sink) {
    checkNotNull(sink, "sink");
    return new Write<>(sink, null /* runner-determined sharding */, null, false);
  }

  private Write(
      Sink<T> sink,
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
        Write.class.getSimpleName());
    return createWrite(input, sink.createWriteOperation());
  }

  @Override
  public void validate(PipelineOptions options) {
    sink.validate(options);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("sink", sink.getClass()).withLabel("Write Sink"))
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
   * Returns the {@link Sink} associated with this PTransform.
   */
  public Sink<T> getSink() {
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
   * Returns a new {@link Write} that will write to the current {@link Sink} using the
   * specified number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link Write} for
   * more information.
   *
   * <p>A value less than or equal to 0 will be equivalent to the default behavior of
   * runner-determined sharding.
   */
  public Write<T> withNumShards(int numShards) {
    if (numShards > 0) {
      return withNumShards(StaticValueProvider.of(numShards));
    }
    return withRunnerDeterminedSharding();
  }

  /**
   * Returns a new {@link Write} that will write to the current {@link Sink} using the
   * {@link ValueProvider} specified number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link Write} for
   * more information.
   */
  public Write<T> withNumShards(ValueProvider<Integer> numShardsProvider) {
    return new Write<>(sink, null, numShardsProvider, windowedWrites);
  }

  /**
   * Returns a new {@link Write} that will write to the current {@link Sink} using the
   * specified {@link PTransform} to compute the number of shards.
   *
   * <p>This option should be used sparingly as it can hurt performance. See {@link Write} for
   * more information.
   */
  public Write<T> withSharding(PTransform<PCollection<T>, PCollectionView<Integer>> sharding) {
    checkNotNull(
        sharding, "Cannot provide null sharding. Use withRunnerDeterminedSharding() instead");
    return new Write<>(sink, sharding, null, windowedWrites);
  }

  /**
   * Returns a new {@link Write} that will write to the current {@link Sink} with
   * runner-determined sharding.
   */
  public Write<T> withRunnerDeterminedSharding() {
    return new Write<>(sink, null, null, windowedWrites);
  }

  /**
   * Returns a new {@link Write} that writes preserves windowing on it's input.
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
  public Write<T> withWindowedWrites() {
    return new Write<>(sink, computeNumShards, numShardsProvider, true);
  }

  /**
   * Writes all the elements in a bundle using a {@link Writer} produced by the
   * {@link WriteOperation} associated with the {@link Sink}.
   */
  private class WriteBundles<WriteT> extends DoFn<T, WriteT> {
    // Writer that will write the records in this bundle. Lazily
    // initialized in processElement.
    private Writer<T, WriteT> writer = null;
    private final PCollectionView<WriteOperation<T, WriteT>> writeOperationView;

    WriteBundles(PCollectionView<WriteOperation<T, WriteT>> writeOperationView) {
      this.writeOperationView = writeOperationView;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      // Lazily initialize the Writer
      if (writer == null) {
        WriteOperation<T, WriteT> writeOperation = c.sideInput(writeOperationView);
        LOG.info("Opening writer for write operation {}", writeOperation);
        writer = writeOperation.createWriter(c.getPipelineOptions());

        if (windowedWrites) {
          writer.openWindowed(UUID.randomUUID().toString(), window, c.pane(), UNKNOWN_SHARDNUM,
              UNKNOWN_NUMSHARDS);
        } else {
          writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, UNKNOWN_NUMSHARDS);
        }
        LOG.debug("Done opening writer {} for operation {}", writer, writeOperationView);
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
        WriteT result = writer.close();
        c.output(result);
        // Reset state in case of reuse.
        writer = null;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(Write.this);
    }
  }

  /**
   * Like {@link WriteBundles}, but where the elements for each shard have been collected into
   * a single iterable.
   *
   * @see WriteBundles
   */
  private class WriteShardedBundles<WriteT> extends DoFn<KV<Integer, Iterable<T>>, WriteT> {
    private final PCollectionView<WriteOperation<T, WriteT>> writeOperationView;
    private final PCollectionView<Integer> numShardsView;

    WriteShardedBundles(PCollectionView<WriteOperation<T, WriteT>> writeOperationView,
                        PCollectionView<Integer> numShardsView) {
      this.writeOperationView = writeOperationView;
      this.numShardsView = numShardsView;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      int numShards = numShardsView != null ? c.sideInput(numShardsView) : getNumShards().get();
      // In a sharded write, single input element represents one shard. We can open and close
      // the writer in each call to processElement.
      WriteOperation<T, WriteT> writeOperation = c.sideInput(writeOperationView);
      LOG.info("Opening writer for write operation {}", writeOperation);
      Writer<T, WriteT> writer = writeOperation.createWriter(c.getPipelineOptions());
      if (windowedWrites) {
        writer.openWindowed(UUID.randomUUID().toString(), window, c.pane(), c.element().getKey(),
            numShards);
      } else {
        writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM, UNKNOWN_NUMSHARDS);
      }
      LOG.debug("Done opening writer {} for operation {}", writer, writeOperationView);

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
        WriteT result = writer.close();
        c.output(result);
      } catch (Exception e) {
        // If anything goes wrong, make sure to delete the temporary file.
        writer.cleanup();
        throw e;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(Write.this);
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
   * <p>In the first, a do-once ParDo is applied to a singleton PCollection containing the Sink's
   * {@link WriteOperation}. In this initialization ParDo, {@link WriteOperation#initialize} is
   * called. The output of this ParDo is a singleton PCollection
   * containing the WriteOperation.
   *
   * <p>This singleton collection containing the WriteOperation is then used as a side input to a
   * ParDo over the PCollection of elements to write. In this bundle-writing phase,
   * {@link WriteOperation#createWriter} is called to obtain a {@link Writer}.
   * {@link Writer#open} and {@link Writer#close} are called in {@link DoFn#startBundle} and
   * {@link DoFn#finishBundle}, respectively, and {@link Writer#write} method is called for
   * every element in the bundle. The output of this ParDo is a PCollection of
   * <i>writer result</i> objects (see {@link Sink} for a description of writer results)-one for
   * each bundle.
   *
   * <p>The final do-once ParDo uses the singleton collection of the WriteOperation as input and
   * the collection of writer results as a side-input. In this ParDo,
   * {@link WriteOperation#finalize} is called to finalize the write.
   *
   * <p>If the write of any element in the PCollection fails, {@link Writer#close} will be called
   * before the exception that caused the write to fail is propagated and the write result will be
   * discarded.
   *
   * <p>Since the {@link WriteOperation} is serialized after the initialization ParDo and
   * deserialized in the bundle-writing and finalization phases, any state change to the
   * WriteOperation object that occurs during initialization is visible in the latter phases.
   * However, the WriteOperation is not serialized after the bundle-writing phase.  This is why
   * implementations should guarantee that {@link WriteOperation#createWriter} does not mutate
   * WriteOperation).
   */
  private <WriteT> PDone createWrite(
      PCollection<T> input, WriteOperation<T, WriteT> writeOperation) {
    Pipeline p = input.getPipeline();
    writeOperation.setWindowedWrites(windowedWrites);

    // A coder to use for the WriteOperation.
    @SuppressWarnings("unchecked")
    Coder<WriteOperation<T, WriteT>> operationCoder =
        (Coder<WriteOperation<T, WriteT>>) SerializableCoder.of(writeOperation.getClass());

    // A singleton collection of the WriteOperation, to be used as input to a ParDo to initialize
    // the sink.
    PCollection<WriteOperation<T, WriteT>> operationCollection =
        p.apply("CreateOperationCollection", Create.of(writeOperation).withCoder(operationCoder));

    // Initialize the resource in a do-once ParDo on the WriteOperation.
    operationCollection = operationCollection
        .apply("Initialize", ParDo.of(
            new DoFn<WriteOperation<T, WriteT>, WriteOperation<T, WriteT>>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                WriteOperation<T, WriteT> writeOperation = c.element();
                LOG.info("Initializing write operation {}", writeOperation);
                writeOperation.initialize(c.getPipelineOptions());
                writeOperation.setWindowedWrites(windowedWrites);
                LOG.debug("Done initializing write operation {}", writeOperation);
                // The WriteOperation is also the output of this ParDo, so it can have mutable
                // state.
                c.output(writeOperation);
              }
            }))
        .setCoder(operationCoder);

    // Create a view of the WriteOperation to be used as a sideInput to the parallel write phase.
    final PCollectionView<WriteOperation<T, WriteT>> writeOperationView =
        operationCollection.apply(View.<WriteOperation<T, WriteT>>asSingleton());

    if (!windowedWrites) {
      // Re-window the data into the global window and remove any existing triggers.
      input =
          input.apply(
              Window.<T>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
    }


    // Perform the per-bundle writes as a ParDo on the input PCollection (with the WriteOperation
    // as a side input) and collect the results of the writes in a PCollection.
    // There is a dependency between this ParDo and the first (the WriteOperation PCollection
    // as a side input), so this will happen after the initial ParDo.
    PCollection<WriteT> results;
    final PCollectionView<Integer> numShardsView;
    if (computeNumShards == null && numShardsProvider == null) {
      if (windowedWrites) {
        throw new IllegalStateException("When doing windowed writes, numShards must be set"
            + "explicitly to a positive value");
      }
      numShardsView = null;
      results = input
          .apply("WriteBundles",
              ParDo.of(new WriteBundles<>(writeOperationView))
                  .withSideInputs(writeOperationView));
    } else {
      if (computeNumShards != null) {
        numShardsView = input.apply(computeNumShards);
        results  = input
            .apply("ApplyShardLabel", ParDo.of(
                new ApplyShardingKey<T>(numShardsView, null)).withSideInputs(numShardsView))
            .apply("GroupIntoShards", GroupByKey.<Integer, T>create())
            .apply("WriteShardedBundles",
                ParDo.of(new WriteShardedBundles<>(writeOperationView, numShardsView))
                    .withSideInputs(numShardsView, writeOperationView));
      } else {
        numShardsView = null;
        results = input
            .apply("ApplyShardLabel", ParDo.of(new ApplyShardingKey<T>(null, numShardsProvider)))
            .apply("GroupIntoShards", GroupByKey.<Integer, T>create())
            .apply("WriteShardedBundles",
                ParDo.of(new WriteShardedBundles<>(writeOperationView, null))
                    .withSideInputs(writeOperationView));
      }
    }
    results.setCoder(writeOperation.getWriterResultCoder());

    if (windowedWrites) {
      // When processing streaming windowed writes, results will arrive multiple times. This
      // means we can't share the below implementation that turns the results into a side input,
      // as new data arriving into a side input does not trigger the listening DoFn. Instead
      // we aggregate the result set using a singleton GroupByKey, so the DoFn will be triggered
      // whenever new data arrives.
      PCollection<KV<Void, WriteT>> keyedResults =
          results.apply("AttachSingletonKey", WithKeys.<Void, WriteT>of((Void) null));
      keyedResults.setCoder(KvCoder.<Void, WriteT>of(VoidCoder.of(), writeOperation
          .getWriterResultCoder()));

      // Is the continuation trigger sufficient?
      keyedResults
          .apply("FinalizeGroupByKey", GroupByKey.<Void, WriteT>create())
          .apply("Finalize", ParDo.of(new DoFn<KV<Void, Iterable<WriteT>>, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              WriteOperation<T, WriteT> writeOperation = c.sideInput(writeOperationView);
              LOG.info("Finalizing write operation {}.", writeOperation);
              List<WriteT> results = Lists.newArrayList(c.element().getValue());
              writeOperation.finalize(results, c.getPipelineOptions());
              LOG.debug("Done finalizing write operation {}", writeOperation);
            }
          }).withSideInputs(writeOperationView));
    } else {
      final PCollectionView<Iterable<WriteT>> resultsView =
          results.apply(View.<WriteT>asIterable());
      ImmutableList.Builder<PCollectionView<?>> sideInputs =
          ImmutableList.<PCollectionView<?>>builder().add(resultsView);
      if (numShardsView != null) {
        sideInputs.add(numShardsView);
      }

      // Finalize the write in another do-once ParDo on the singleton collection containing the
      // Writer. The results from the per-bundle writes are given as an Iterable side input.
      // The WriteOperation's state is the same as after its initialization in the first do-once
      // ParDo. There is a dependency between this ParDo and the parallel write (the writer
      // results collection as a side input), so it will happen after the parallel write.
      // For the non-windowed case, we guarantee that  if no data is written but the user has
      // set numShards, then all shards will be written out as empty files. For this reason we
      // use a side input here.
      operationCollection
          .apply("Finalize", ParDo.of(new DoFn<WriteOperation<T, WriteT>, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              WriteOperation<T, WriteT> writeOperation = c.element();
              LOG.info("Finalizing write operation {}.", writeOperation);
              List<WriteT> results = Lists.newArrayList(c.sideInput(resultsView));
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
                    "Creating {} empty output shards in addition to {} written for a total of "
                        + " {}.", extraShardsNeeded, results.size(), minShardsNeeded);
                for (int i = 0; i < extraShardsNeeded; ++i) {
                  Writer<T, WriteT> writer = writeOperation.createWriter(c.getPipelineOptions());
                  writer.openUnwindowed(UUID.randomUUID().toString(), UNKNOWN_SHARDNUM,
                      UNKNOWN_NUMSHARDS);
                  WriteT emptyWrite = writer.close();
                  results.add(emptyWrite);
                }
                LOG.debug("Done creating extra shards.");
              }
              writeOperation.finalize(results, c.getPipelineOptions());
              LOG.debug("Done finalizing write operation {}", writeOperation);
            }
          }).withSideInputs(sideInputs.build()));
    }
    return PDone.in(input.getPipeline());
  }
}
