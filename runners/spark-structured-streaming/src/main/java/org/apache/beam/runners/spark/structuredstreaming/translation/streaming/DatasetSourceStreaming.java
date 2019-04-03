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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.SchemaHelpers;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
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
 * This is a spark structured streaming {@link DataSourceV2} implementation. As Continuous streaming
 * is tagged experimental in spark, this class does no implement {@link ContinuousReadSupport}.
 */
public class DatasetSourceStreaming implements DataSourceV2, MicroBatchReadSupport {

  static final String BEAM_SOURCE_OPTION = "beam-source";
  static final String DEFAULT_PARALLELISM = "default-parallelism";
  static final String PIPELINE_OPTIONS = "pipeline-options";

  @Override
  public MicroBatchReader createMicroBatchReader(Optional<StructType> schema,
      String checkpointLocation, DataSourceOptions options) {
    return new DatasetMicroBatchReader(checkpointLocation, options);
  }

  /** This class is mapped to Beam {@link UnboundedSource}. */
  private static class DatasetMicroBatchReader<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      implements MicroBatchReader, Serializable {

    private int numPartitions;
    private UnboundedSource<T, CheckpointMarkT> source;
    private SerializablePipelineOptions serializablePipelineOptions;

    @SuppressWarnings("unchecked")
    private DatasetMicroBatchReader(
        String checkpointLocation, DataSourceOptions options) {
      if (!options.get(BEAM_SOURCE_OPTION).isPresent()) {
        throw new RuntimeException("Beam source was not set in DataSource options");
      }
      this.source = Base64Serializer
          .deserializeUnchecked(options.get(BEAM_SOURCE_OPTION).get(), UnboundedSource.class);

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

    @Override public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
    }

    @Override public Offset getStartOffset() {
      return null;
    }

    @Override public Offset getEndOffset() {
      return null;
    }

    @Override public Offset deserializeOffset(String json) {
      return null;
    }

    @Override public void commit(Offset end) {
      //TODO no more to read after end Offset
    }

    @Override public void stop() {
    }

    @Override public StructType readSchema() {
      // TODO: find a way to extend schema with a WindowedValue schema
      return SchemaHelpers.binarySchema();
    }

    @Override public List<InputPartition<InternalRow>> planInputPartitions() {
      SparkPipelineOptions sparkPipelineOptions = serializablePipelineOptions.get()
          .as(SparkPipelineOptions.class);
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      try {
        List<? extends UnboundedSource<T, CheckpointMarkT>> splits = source
            .split(numPartitions, sparkPipelineOptions);
        for (UnboundedSource<T, CheckpointMarkT> split : splits) {
          result.add(new InputPartition<InternalRow>() {

            @Override public InputPartitionReader<InternalRow> createPartitionReader() {
              return new DatasetSourceStreaming.DatasetPartitionReader<>
              (split, serializablePipelineOptions);
            }
          });
        } return result;

      } catch (Exception e) {
        throw new RuntimeException(
            "Error in splitting UnboundedSource " + source.getClass().getCanonicalName(), e);
      }
    }
  }
  /** This class can be mapped to Beam {@link BoundedSource.BoundedReader}. */
  private static class DatasetPartitionReader<T, CheckpointMarkT extends UnboundedSource.CheckpointMark> implements InputPartitionReader<InternalRow> {
    private boolean started;
    private boolean closed;
    private UnboundedSource<T, CheckpointMarkT> source;
    private UnboundedSource.UnboundedReader<T> reader;

    DatasetPartitionReader(
        UnboundedSource<T, CheckpointMarkT> source, SerializablePipelineOptions serializablePipelineOptions) {
      this.started = false;
      this.closed = false;
      this.source = source;
      // reader is not serializable so lazy initialize it
      try {
        reader =
            // TODO restore from checkpoint
            source.createReader(serializablePipelineOptions.get().as(SparkPipelineOptions.class), null);
      } catch (IOException e) {
        throw new RuntimeException("Error creating UnboundedReader ", e);
      }
    }

    @Override
    public boolean next() throws IOException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return !closed && reader.advance();
      }
    }

    @Override
    public InternalRow get() {
/*
      WindowedValue<T> windowedValue =
          WindowedValue.timestampedValueInGlobalWindow(
              reader.getCurrent(), reader.getCurrentTimestamp());
      return RowHelpers.storeWindowedValueInRow(windowedValue, source.getOutputCoder());
*/
//TODO
return null;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      reader.close();
    }
  }

}
