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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.translation.SchemaHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.RowHelpers;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

/**
 * This is a spark structured streaming {@link DataSourceV2} implementation that wraps an {@link
 * UnboundedSource}.
 *
 * <p>As Continuous streaming is tagged experimental in spark (no aggregation support + no exactly
 * once guaranty), this class does no implement {@link ContinuousReadSupport}.
 *
 * <p>Spark {@link Offset}s are ignored because:
 *
 * <ul>
 *   <li>resuming from checkpoint is supported by the Beam framework through {@link CheckpointMark}
 *   <li>{@link DatasetSourceStreaming} is a generic wrapper that could wrap a Beam {@link
 *       UnboundedSource} that cannot specify offset ranges
 * </ul>
 *
 * So, no matter the offset range specified by the spark framework, the Beam source will resume from
 * its {@link CheckpointMark} in case of failure.
 */
@SuppressFBWarnings("SE_BAD_FIELD") // make spotbugs happy
class DatasetSourceStreaming implements DataSourceV2, MicroBatchReadSupport {

  static final String BEAM_SOURCE_OPTION = "beam-source";
  static final String DEFAULT_PARALLELISM = "default-parallelism";
  static final String PIPELINE_OPTIONS = "pipeline-options";

  @Override
  public MicroBatchReader createMicroBatchReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    return new DatasetMicroBatchReader(checkpointLocation, options);
  }

  /** This class is mapped to Beam {@link UnboundedSource}. */
  private static class DatasetMicroBatchReader<
          T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      implements MicroBatchReader, Serializable {

    private int numPartitions;
    private UnboundedSource<T, CheckpointMarkT> source;
    private SerializablePipelineOptions serializablePipelineOptions;

    private final List<DatasetPartitionReader> partitionReaders = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private DatasetMicroBatchReader(String checkpointLocation, DataSourceOptions options) {
      if (!options.get(BEAM_SOURCE_OPTION).isPresent()) {
        throw new RuntimeException("Beam source was not set in DataSource options");
      }
      this.source =
          Base64Serializer.deserializeUnchecked(
              options.get(BEAM_SOURCE_OPTION).get(), UnboundedSource.class);

      if (!options.get(DEFAULT_PARALLELISM).isPresent()) {
        throw new RuntimeException("Spark default parallelism was not set in DataSource options");
      }
      this.numPartitions = Integer.parseInt(options.get(DEFAULT_PARALLELISM).get());
      checkArgument(numPartitions > 0, "Number of partitions must be greater than zero.");

      if (!options.get(PIPELINE_OPTIONS).isPresent()) {
        throw new RuntimeException("Beam pipelineOptions were not set in DataSource options");
      }
      this.serializablePipelineOptions =
          new SerializablePipelineOptions(options.get(PIPELINE_OPTIONS).get());
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
      // offsets are ignored see javadoc
    }

    @Override
    public Offset getStartOffset() {
      return EMPTY_OFFSET;
    }

    @Override
    public Offset getEndOffset() {
      return EMPTY_OFFSET;
    }

    @Override
    public Offset deserializeOffset(String json) {
      return EMPTY_OFFSET;
    }

    @Override
    public void commit(Offset end) {
      // offsets are ignored see javadoc
      for (DatasetPartitionReader partitionReader : partitionReaders) {
        try {
          // TODO: is checkpointMark stored in reliable storage ?
          partitionReader.reader.getCheckpointMark().finalizeCheckpoint();
        } catch (IOException e) {
          throw new RuntimeException(
              String.format(
                  "Commit of Offset %s failed, checkpointMark %s finalizeCheckpoint() failed",
                  end, partitionReader.reader.getCheckpointMark()));
        }
      }
    }

    @Override
    public void stop() {
      try {
        for (DatasetPartitionReader partitionReader : partitionReaders) {
          if (partitionReader.started) {
            partitionReader.close();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Error closing " + this + "partitionReaders", e);
      }
    }

    @Override
    public StructType readSchema() {
      // TODO: find a way to extend schema with a WindowedValue schema
      return SchemaHelpers.binarySchema();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      PipelineOptions options = serializablePipelineOptions.get();
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      try {
        List<? extends UnboundedSource<T, CheckpointMarkT>> splits =
            source.split(numPartitions, options);
        for (UnboundedSource<T, CheckpointMarkT> split : splits) {
          result.add(
              new InputPartition<InternalRow>() {

                @Override
                public InputPartitionReader<InternalRow> createPartitionReader() {
                  DatasetPartitionReader<T, CheckpointMarkT> datasetPartitionReader;
                  datasetPartitionReader =
                      new DatasetPartitionReader<>(split, serializablePipelineOptions);
                  partitionReaders.add(datasetPartitionReader);
                  return datasetPartitionReader;
                }
              });
        }
        return result;

      } catch (Exception e) {
        throw new RuntimeException(
            "Error in splitting UnboundedSource " + source.getClass().getCanonicalName(), e);
      }
    }
  }

  /** This class can be mapped to Beam {@link BoundedSource.BoundedReader}. */
  private static class DatasetPartitionReader<
          T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      implements InputPartitionReader<InternalRow> {
    private boolean started;
    private boolean closed;
    private final UnboundedSource<T, CheckpointMarkT> source;
    private UnboundedSource.UnboundedReader<T> reader;

    DatasetPartitionReader(
        UnboundedSource<T, CheckpointMarkT> source,
        SerializablePipelineOptions serializablePipelineOptions) {
      this.started = false;
      this.closed = false;
      this.source = source;
      // reader is not serializable so lazy initialize it
      try {
        reader =
            // In
            // https://blog.yuvalitzchakov.com/exploring-stateful-streaming-with-spark-structured-streaming/
            // "Structured Streaming stores and retrieves the offsets on our behalf when re-running
            // the application meaning we no longer have to store them externally."
            source.createReader(serializablePipelineOptions.get(), null);
      } catch (IOException e) {
        throw new RuntimeException("Error creating UnboundedReader ", e);
      }
    }

    @Override
    public boolean next() throws IOException {
      // TODO deal with watermark
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return !closed && reader.advance();
      }
    }

    @Override
    public InternalRow get() {
      WindowedValue<T> windowedValue =
          WindowedValue.timestampedValueInGlobalWindow(
              reader.getCurrent(), reader.getCurrentTimestamp());
      return RowHelpers.storeWindowedValueInRow(windowedValue, source.getOutputCoder());
    }

    @Override
    public void close() throws IOException {
      closed = true;
      reader.close();
    }
  }

  private static final Offset EMPTY_OFFSET =
      new Offset() {
        @Override
        public String json() {
          return "{offset : -1}";
        }
      };
}
