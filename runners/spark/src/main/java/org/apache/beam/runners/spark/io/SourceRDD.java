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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.SparkMetricsContainer;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.Accumulator;
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

/**
 * Classes implementing Beam {@link Source} {@link RDD}s.
 */
public class SourceRDD {

  /**
   * A {@link SourceRDD.Bounded} reads input from a {@link BoundedSource}
   * and creates a Spark {@link RDD}.
   * This is the default way for the SparkRunner to read data from Beam's BoundedSources.
   */
  public static class Bounded<T> extends RDD<WindowedValue<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceRDD.Bounded.class);

    private final BoundedSource<T> source;
    private final SparkRuntimeContext runtimeContext;
    private final int numPartitions;
    private final String stepName;
    private final Accumulator<SparkMetricsContainer> metricsAccum;

    // to satisfy Scala API.
    private static final scala.collection.immutable.Seq<Dependency<?>> NIL =
        scala.collection.JavaConversions
          .asScalaBuffer(Collections.<Dependency<?>>emptyList()).toList();

    public Bounded(
        SparkContext sc,
        BoundedSource<T> source,
        SparkRuntimeContext runtimeContext,
        String stepName) {
      super(sc, NIL, JavaSparkContext$.MODULE$.<WindowedValue<T>>fakeClassTag());
      this.source = source;
      this.runtimeContext = runtimeContext;
      // the input parallelism is determined by Spark's scheduler backend.
      // when running on YARN/SparkDeploy it's the result of max(totalCores, 2).
      // when running on Mesos it's 8.
      // when running local it's the total number of cores (local = 1, local[N] = N,
      // local[*] = estimation of the machine's cores).
      // ** the configuration "spark.default.parallelism" takes precedence over all of the above **
      this.numPartitions = sc.defaultParallelism();
      checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");
      this.stepName = stepName;
      this.metricsAccum = MetricsAccumulator.getInstance();
    }

    private static final long DEFAULT_BUNDLE_SIZE = 64 * 1024 * 1024;

    @Override
    public Partition[] getPartitions() {
      long desiredSizeBytes = DEFAULT_BUNDLE_SIZE;
      try {
        desiredSizeBytes = source.getEstimatedSizeBytes(
            runtimeContext.getPipelineOptions()) / numPartitions;
      } catch (Exception e) {
        LOG.warn("Failed to get estimated bundle size for source {}, using default bundle "
            + "size of {} bytes.", source, DEFAULT_BUNDLE_SIZE);
      }
      try {
        List<? extends Source<T>> partitionedSources = source.split(desiredSizeBytes,
            runtimeContext.getPipelineOptions());
        Partition[] partitions = new SourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new SourcePartition<>(id(), i, partitionedSources.get(i));
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions for source "
            + source.getClass().getSimpleName(), e);
      }
    }

    @Override
    public scala.collection.Iterator<WindowedValue<T>> compute(final Partition split,
                                                               TaskContext context) {
      final MetricsContainer metricsContainer = metricsAccum.localValue().getContainer(stepName);

      final Iterator<WindowedValue<T>> iter = new Iterator<WindowedValue<T>>() {
        @SuppressWarnings("unchecked")
        SourcePartition<T> partition = (SourcePartition<T>) split;
        BoundedSource.BoundedReader<T> reader = createReader(partition);

        private boolean finished = false;
        private boolean started = false;
        private boolean closed = false;

        @Override
        public boolean hasNext() {
          // Add metrics container to the scope of org.apache.beam.sdk.io.Source.Reader methods
          // since they may report metrics.
          try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
            try {
              if (!started) {
                started = true;
                finished = !reader.start();
              } else {
                finished = !reader.advance();
              }
              if (finished) {
                // safely close the reader if there are no more elements left to read.
                closeIfNotClosed();
              }
              return !finished;
            } catch (IOException e) {
              closeIfNotClosed();
              throw new RuntimeException("Failed to read from reader.", e);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public WindowedValue<T> next() {
          return WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(),
              reader.getCurrentTimestamp());
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove from partition iterator is not allowed.");
        }

        private void closeIfNotClosed() {
          if (!closed) {
            closed = true;
            try {
              reader.close();
            } catch (IOException e) {
              throw new RuntimeException("Failed to close Reader.", e);
            }
          }
        }
      };

      return new InterruptibleIterator<>(context,
          scala.collection.JavaConversions.asScalaIterator(iter));
    }

    private BoundedSource.BoundedReader<T> createReader(SourcePartition<T> partition) {
      try {
        return ((BoundedSource<T>) partition.source).createReader(
            runtimeContext.getPipelineOptions());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
      }
    }
  }

  /**
   * An input {@link Partition} wrapping the partitioned {@link Source}.
   */
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

    public Source<T> getSource() {
      return source;
    }
  }

  /**
   * A {@link SourceRDD.Unbounded} is the implementation of a micro-batch
   * in a {@link SourceDStream}.
   *
   * <p>This RDD is made of P partitions, each containing a single pair-element of the partitioned
   * {@link MicrobatchSource} and an optional starting {@link UnboundedSource.CheckpointMark}.
   */
  public static class Unbounded<T, CheckpointMarkT extends
        UnboundedSource.CheckpointMark> extends RDD<scala.Tuple2<Source<T>, CheckpointMarkT>> {

    private final MicrobatchSource<T, CheckpointMarkT> microbatchSource;
    private final SparkRuntimeContext runtimeContext;
    private final Partitioner partitioner;

    // to satisfy Scala API.
    private static final scala.collection.immutable.List<Dependency<?>> NIL =
        scala.collection.JavaConversions
            .asScalaBuffer(Collections.<Dependency<?>>emptyList()).toList();

    public Unbounded(SparkContext sc,
        SparkRuntimeContext runtimeContext,
        MicrobatchSource<T, CheckpointMarkT> microbatchSource,
        int initialNumPartitions) {
      super(sc, NIL,
          JavaSparkContext$.MODULE$.<scala.Tuple2<Source<T>, CheckpointMarkT>>fakeClassTag());
      this.runtimeContext = runtimeContext;
      this.microbatchSource = microbatchSource;
      this.partitioner = new HashPartitioner(initialNumPartitions);
    }

    @Override
    public Partition[] getPartitions() {
      try {
        List<? extends Source<T>> partitionedSources = microbatchSource.split(
            -1 /* ignored */, runtimeContext.getPipelineOptions());
        Partition[] partitions = new CheckpointableSourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new CheckpointableSourcePartition<>(id(), i, partitionedSources.get(i),
              EmptyCheckpointMark.get());
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions.", e);
      }
    }

    @Override
    public Option<Partitioner> partitioner() {
      // setting the partitioner helps to "keep" the same partitioner in the following
      // mapWithState read for Read.Unbounded, preventing a post-mapWithState shuffle.
      return scala.Some.apply(partitioner);
    }

    @Override
    public scala.collection.Iterator<scala.Tuple2<Source<T>, CheckpointMarkT>>
    compute(Partition split, TaskContext context) {
      @SuppressWarnings("unchecked")
      CheckpointableSourcePartition<T, CheckpointMarkT> partition =
          (CheckpointableSourcePartition<T, CheckpointMarkT>) split;
      scala.Tuple2<Source<T>, CheckpointMarkT> tuple2 =
          new scala.Tuple2<>(partition.getSource(), partition.checkpointMark);
      return scala.collection.JavaConversions.asScalaIterator(
          Collections.singleton(tuple2).iterator());
    }
  }

  /** A {@link SourcePartition} with a {@link UnboundedSource.CheckpointMark}. */
  private static class CheckpointableSourcePartition<T, CheckpointMarkT extends
      UnboundedSource.CheckpointMark> extends SourcePartition<T> {
    private final CheckpointMarkT checkpointMark;

    CheckpointableSourcePartition(int rddId,
                                  int index,
                                  Source<T> source,
                                  CheckpointMarkT checkpointMark) {
      super(rddId, index, source);
      this.checkpointMark = checkpointMark;
    }
  }
}
