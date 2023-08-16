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
package org.apache.beam.runners.spark.structuredstreaming.io;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.emptyList;
import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static scala.collection.JavaConverters.asScalaIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

public class BoundedDatasetFactory {
  private BoundedDatasetFactory() {}

  /**
   * Create a {@link Dataset} for a {@link BoundedSource} via a Spark {@link Table}.
   *
   * <p>Unfortunately tables are expected to return an {@link InternalRow}, requiring serialization.
   * This makes this approach at the time being significantly less performant than creating a
   * dataset from an RDD.
   */
  public static <T> Dataset<WindowedValue<T>> createDatasetFromRows(
      SparkSession session,
      BoundedSource<T> source,
      Supplier<PipelineOptions> options,
      Encoder<WindowedValue<T>> encoder) {
    Params<T> params = new Params<>(encoder, options, session.sparkContext().defaultParallelism());
    BeamTable<T> table = new BeamTable<>(source, params);
    LogicalPlan logicalPlan = DataSourceV2Relation.create(table, Option.empty(), Option.empty());
    return Dataset.ofRows(session, logicalPlan).as(encoder);
  }

  /**
   * Create a {@link Dataset} for a {@link BoundedSource} via a Spark {@link RDD}.
   *
   * <p>This is currently the most efficient approach as it avoid any serialization overhead.
   */
  public static <T> Dataset<WindowedValue<T>> createDatasetFromRDD(
      SparkSession session,
      BoundedSource<T> source,
      Supplier<PipelineOptions> options,
      Encoder<WindowedValue<T>> encoder) {
    Params<T> params = new Params<>(encoder, options, session.sparkContext().defaultParallelism());
    RDD<WindowedValue<T>> rdd = new BoundedRDD<>(session.sparkContext(), source, params);
    return session.createDataset(rdd, encoder);
  }

  /** An {@link RDD} for a bounded Beam source. */
  private static class BoundedRDD<T> extends RDD<WindowedValue<T>> {
    final BoundedSource<T> source;
    final Params<T> params;

    public BoundedRDD(SparkContext sc, BoundedSource<T> source, Params<T> params) {
      super(sc, emptyList(), ClassTag.apply(WindowedValue.class));
      this.source = source;
      this.params = params;
    }

    @Override
    public Iterator<WindowedValue<T>> compute(Partition split, TaskContext context) {
      return new InterruptibleIterator<>(
          context,
          asScalaIterator(new SourcePartitionIterator<>((SourcePartition<T>) split, params)));
    }

    @Override
    public Partition[] getPartitions() {
      return SourcePartition.partitionsOf(source, params).toArray(new Partition[0]);
    }
  }

  /** A Spark {@link Table} for a bounded Beam source supporting batch reads only. */
  private static class BeamTable<T> implements Table, SupportsRead {
    final BoundedSource<T> source;
    final Params<T> params;

    BeamTable(BoundedSource<T> source, Params<T> params) {
      this.source = source;
      this.params = params;
    }

    public Encoder<WindowedValue<T>> getEncoder() {
      return params.encoder;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap ignored) {
      return () ->
          new Scan() {
            @Override
            public StructType readSchema() {
              return params.encoder.schema();
            }

            @Override
            public Batch toBatch() {
              return new BeamBatch<>(source, params);
            }
          };
    }

    @Override
    public String name() {
      return "BeamSource<" + source.getClass().getName() + ">";
    }

    @Override
    public StructType schema() {
      return params.encoder.schema();
    }

    @Override
    public Set<TableCapability> capabilities() {
      return ImmutableSet.of(TableCapability.BATCH_READ);
    }

    private static class BeamBatch<T> implements Batch, Serializable {
      final BoundedSource<T> source;
      final Params<T> params;

      private BeamBatch(BoundedSource<T> source, Params<T> params) {
        this.source = source;
        this.params = params;
      }

      @Override
      public InputPartition[] planInputPartitions() {
        return SourcePartition.partitionsOf(source, params).toArray(new InputPartition[0]);
      }

      @Override
      public PartitionReaderFactory createReaderFactory() {
        return p -> new BeamPartitionReader<>(((SourcePartition<T>) p), params);
      }
    }

    private static class BeamPartitionReader<T> implements PartitionReader<InternalRow> {
      final SourcePartitionIterator<T> iterator;
      final Serializer<WindowedValue<T>> serializer;
      transient @Nullable InternalRow next;

      BeamPartitionReader(SourcePartition<T> partition, Params<T> params) {
        iterator = new SourcePartitionIterator<>(partition, params);
        serializer = ((ExpressionEncoder<WindowedValue<T>>) params.encoder).createSerializer();
      }

      @Override
      public boolean next() throws IOException {
        if (iterator.hasNext()) {
          next = serializer.apply(iterator.next());
          return true;
        }
        return false;
      }

      @Override
      public InternalRow get() {
        if (next == null) {
          throw new IllegalStateException("Next not available");
        }
        return next;
      }

      @Override
      public void close() throws IOException {
        next = null;
        iterator.close();
      }
    }
  }

  /** A Spark partition wrapping the partitioned Beam {@link BoundedSource}. */
  private static class SourcePartition<T> implements Partition, InputPartition {
    final BoundedSource<T> source;
    final int index;

    SourcePartition(BoundedSource<T> source, IntSupplier idxSupplier) {
      this.source = source;
      this.index = idxSupplier.getAsInt();
    }

    static <T> List<SourcePartition<T>> partitionsOf(BoundedSource<T> source, Params<T> params) {
      try {
        PipelineOptions options = params.options.get();
        long desiredSize = source.getEstimatedSizeBytes(options) / params.numPartitions;
        List<BoundedSource<T>> split = (List<BoundedSource<T>>) source.split(desiredSize, options);
        IntSupplier idxSupplier = new AtomicInteger(0)::getAndIncrement;
        return split.stream().map(s -> new SourcePartition<>(s, idxSupplier)).collect(toList());
      } catch (Exception e) {
        throw new RuntimeException(
            "Error splitting BoundedSource " + source.getClass().getCanonicalName(), e);
      }
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public int hashCode() {
      return index;
    }
  }

  /** A partition iterator on a partitioned Beam {@link BoundedSource}. */
  private static class SourcePartitionIterator<T> extends AbstractIterator<WindowedValue<T>>
      implements Closeable {
    BoundedReader<T> reader;
    boolean started = false;

    public SourcePartitionIterator(SourcePartition<T> partition, Params<T> params) {
      try {
        reader = partition.source.createReader(params.options.get());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
      }
    }

    @Override
    @SuppressWarnings("nullness") // ok, reader not used any longer
    public void close() throws IOException {
      if (reader != null) {
        endOfData();
        try {
          reader.close();
        } finally {
          reader = null;
        }
      }
    }

    @Override
    protected @CheckForNull WindowedValue<T> computeNext() {
      try {
        if (started ? reader.advance() : start()) {
          return timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
        } else {
          close();
          return endOfData();
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to start or advance reader.", e);
      }
    }

    private boolean start() throws IOException {
      started = true;
      return reader.start();
    }
  }

  /** Shared parameters. */
  private static class Params<T> implements Serializable {
    final Encoder<WindowedValue<T>> encoder;
    final Supplier<PipelineOptions> options;
    final int numPartitions;

    Params(
        Encoder<WindowedValue<T>> encoder, Supplier<PipelineOptions> options, int numPartitions) {
      checkArgument(numPartitions > 0, "Number of partitions must be greater than zero.");
      this.encoder = encoder;
      this.options = options;
      this.numPartitions = numPartitions;
    }
  }
}
