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
package org.apache.beam.runners.spark.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.Dependency;
import org.apache.spark.HashPartitioner;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

/** Classes implementing Beam {@link Source} {@link RDD}s. */
public class SourceRDD {

  /**
   * A {@link SourceRDD.Bounded} reads input from a {@link BoundedSource} and creates a Spark {@link
   * RDD}. This is the default way for the SparkRunner to read data from Beam's BoundedSources.
   */
  public static class Bounded<T> extends RDD<WindowedValue<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceRDD.Bounded.class);

    private final BoundedSource<T> source;
    private final SerializablePipelineOptions options;
    private final int numPartitions;
    private final long bundleSize;
    private final String stepName;
    private final MetricsContainerStepMapAccumulator metricsAccum;

    // to satisfy Scala API.
    private static final scala.collection.immutable.Seq<Dependency<?>> NIL =
        JavaConversions.asScalaBuffer(Collections.<Dependency<?>>emptyList()).toList();

    public Bounded(
        SparkContext sc,
        BoundedSource<T> source,
        SerializablePipelineOptions options,
        String stepName) {
      super(sc, NIL, JavaSparkContext$.MODULE$.fakeClassTag());
      this.source = source;
      this.options = options;
      // the input parallelism is determined by Spark's scheduler backend.
      // when running on YARN/SparkDeploy it's the result of max(totalCores, 2).
      // when running on Mesos it's 8.
      // when running local it's the total number of cores (local = 1, local[N] = N,
      // local[*] = estimation of the machine's cores).
      // ** the configuration "spark.default.parallelism" takes precedence over all of the above **
      this.numPartitions = sc.defaultParallelism();
      checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");
      this.bundleSize = options.get().as(SparkPipelineOptions.class).getBundleSize();
      this.stepName = stepName;
      this.metricsAccum = MetricsAccumulator.getInstance();
    }

    private static final long DEFAULT_BUNDLE_SIZE = 64L * 1024L * 1024L;

    @Override
    public Partition[] getPartitions() {
      try {
        long desiredSizeBytes = (bundleSize > 0) ? bundleSize : DEFAULT_BUNDLE_SIZE;
        if (bundleSize == 0) {
          try {
            desiredSizeBytes = source.getEstimatedSizeBytes(options.get()) / numPartitions;
          } catch (Exception e) {
            LOG.warn(
                "Failed to get estimated bundle size for source {}, using default bundle "
                    + "size of {} bytes.",
                source,
                DEFAULT_BUNDLE_SIZE);
          }
        }

        List<? extends Source<T>> partitionedSources =
            source.split(desiredSizeBytes, options.get());
        Partition[] partitions = new SourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new SourcePartition<>(id(), i, partitionedSources.get(i));
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to create partitions for source " + source.getClass().getSimpleName(), e);
      }
    }

    private BoundedSource.BoundedReader<T> createReader(SourcePartition<T> partition) {
      try {
        return ((BoundedSource<T>) partition.source).createReader(options.get());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
      }
    }

    @Override
    public scala.collection.Iterator<WindowedValue<T>> compute(
        final Partition split, final TaskContext context) {
      final MetricsContainer metricsContainer = metricsAccum.value().getContainer(stepName);

      @SuppressWarnings("unchecked")
      final BoundedSource.BoundedReader<T> reader = createReader((SourcePartition<T>) split);

      final Iterator<WindowedValue<T>> readerIterator =
          new ReaderToIteratorAdapter<>(metricsContainer, reader);

      return new InterruptibleIterator<>(context, JavaConversions.asScalaIterator(readerIterator));
    }

    /**
     * Exposes an <code>Iterator</code>&lt;{@link WindowedValue}&gt; interface on top of a {@link
     * Source.Reader}.
     *
     * <p><code>hasNext</code> is idempotent and returns <code>true</code> iff further items are
     * available for reading using the underlying reader. Consequently, when the reader is closed,
     * or when the reader has no further elements available (i.e, {@link Source.Reader#advance()}
     * returned <code>false</code>), <code>hasNext</code> returns <code>false</code>.
     *
     * <p>Since this is a read-only iterator, an attempt to call <code>remove</code> will throw an
     * <code>UnsupportedOperationException</code>.
     */
    @VisibleForTesting
    static class ReaderToIteratorAdapter<T> implements Iterator<WindowedValue<T>> {

      private static final boolean FAILED_TO_OBTAIN_NEXT = false;
      private static final boolean SUCCESSFULLY_OBTAINED_NEXT = true;

      private final MetricsContainer metricsContainer;
      private final Source.Reader<T> reader;

      private boolean started = false;
      private boolean closed = false;
      private WindowedValue<T> next = null;

      ReaderToIteratorAdapter(
          final MetricsContainer metricsContainer, final Source.Reader<T> reader) {
        this.metricsContainer = metricsContainer;
        this.reader = reader;
      }

      private boolean tryProduceNext() {
        try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
          if (closed) {
            return FAILED_TO_OBTAIN_NEXT;
          } else {
            checkState(next == null, "unexpected non-null value for next");
            if (seekNext()) {
              next =
                  WindowedValue.timestampedValueInGlobalWindow(
                      reader.getCurrent(), reader.getCurrentTimestamp());
              return SUCCESSFULLY_OBTAINED_NEXT;
            } else {
              close();
              return FAILED_TO_OBTAIN_NEXT;
            }
          }
        } catch (final Exception e) {
          throw new RuntimeException("Failed to read data.", e);
        }
      }

      private void close() {
        closed = true;
        try {
          reader.close();
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }

      private boolean seekNext() throws IOException {
        if (!started) {
          started = true;
          return reader.start();
        } else {
          return !closed && reader.advance();
        }
      }

      private WindowedValue<T> consumeCurrent() {
        if (next == null) {
          throw new NoSuchElementException();
        } else {
          final WindowedValue<T> current = next;
          next = null;
          return current;
        }
      }

      private WindowedValue<T> consumeNext() {
        if (next == null) {
          tryProduceNext();
        }
        return consumeCurrent();
      }

      @Override
      public boolean hasNext() {
        return next != null || tryProduceNext();
      }

      @Override
      public WindowedValue<T> next() {
        return consumeNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }

  /** An input {@link Partition} wrapping the partitioned {@link Source}. */
  private static class SourcePartition<T> implements Partition {

    private final int rddId;
    private final int index;
    private final Source<T> source;

    SourcePartition(int rddId, int index, Source<T> source) {
      this.rddId = rddId;
      this.index = index;
      this.source = source;
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public int hashCode() {
      return 41 * (41 + rddId) + index;
    }

    Source<T> getSource() {
      return source;
    }
  }

  /**
   * A {@link SourceRDD.Unbounded} is the implementation of a micro-batch in a {@link
   * SourceDStream}.
   *
   * <p>This RDD is made of P partitions, each containing a single pair-element of the partitioned
   * {@link MicrobatchSource} and an optional starting {@link UnboundedSource.CheckpointMark}.
   */
  public static class Unbounded<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends RDD<scala.Tuple2<Source<T>, CheckpointMarkT>> {

    private final MicrobatchSource<T, CheckpointMarkT> microbatchSource;
    private final SerializablePipelineOptions options;
    private final Partitioner partitioner;

    // to satisfy Scala API.
    private static final scala.collection.immutable.List<Dependency<?>> NIL =
        JavaConversions.asScalaBuffer(Collections.<Dependency<?>>emptyList()).toList();

    public Unbounded(
        SparkContext sc,
        SerializablePipelineOptions options,
        MicrobatchSource<T, CheckpointMarkT> microbatchSource,
        int initialNumPartitions) {
      super(sc, NIL, JavaSparkContext$.MODULE$.fakeClassTag());
      this.options = options;
      this.microbatchSource = microbatchSource;
      this.partitioner = new HashPartitioner(initialNumPartitions);
    }

    @Override
    public Partition[] getPartitions() {
      try {
        final List<? extends Source<T>> partitionedSources = microbatchSource.split(options.get());
        final Partition[] partitions = new CheckpointableSourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] =
              new CheckpointableSourcePartition<>(
                  id(), i, partitionedSources.get(i), EmptyCheckpointMark.get());
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions.", e);
      }
    }

    @Override
    public Option<Partitioner> partitioner() {
      // setting the partitioner helps to "keep" the same partitioner in the following
      // mapWithState read for SplittableParDo.PrimitiveUnboundedRead, preventing a
      // post-mapWithState shuffle.
      return scala.Some.apply(partitioner);
    }

    @Override
    public scala.collection.Iterator<scala.Tuple2<Source<T>, CheckpointMarkT>> compute(
        Partition split, TaskContext context) {
      @SuppressWarnings("unchecked")
      CheckpointableSourcePartition<T, CheckpointMarkT> partition =
          (CheckpointableSourcePartition<T, CheckpointMarkT>) split;
      scala.Tuple2<Source<T>, CheckpointMarkT> tuple2 =
          new scala.Tuple2<>(partition.getSource(), partition.checkpointMark);
      return JavaConversions.asScalaIterator(Collections.singleton(tuple2).iterator());
    }
  }

  /** A {@link SourcePartition} with a {@link UnboundedSource.CheckpointMark}. */
  private static class CheckpointableSourcePartition<
          T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends SourcePartition<T> {
    private final CheckpointMarkT checkpointMark;

    CheckpointableSourcePartition(
        int rddId, int index, Source<T> source, CheckpointMarkT checkpointMark) {
      super(rddId, index, source);
      this.checkpointMark = checkpointMark;
    }
  }
}
