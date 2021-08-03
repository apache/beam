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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.runners.spark.structuredstreaming.Constants.BEAM_SOURCE_OPTION;
import static org.apache.beam.runners.spark.structuredstreaming.Constants.DEFAULT_PARALLELISM;
import static org.apache.beam.runners.spark.structuredstreaming.Constants.PIPELINE_OPTIONS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.RowHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SchemaHelpers;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.parquet.Strings;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Spark DataSourceV2 API was removed in Spark3. This is a Beam source wrapper using the new spark 3
 * source API.
 */
public class DatasetSourceBatch implements TableProvider {

  private static final StructType BINARY_SCHEMA = SchemaHelpers.binarySchema();

  public DatasetSourceBatch() {}

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return BINARY_SCHEMA;
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new DatasetSourceBatchTable();
  }

  private static class DatasetSourceBatchTable implements SupportsRead {

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return new ScanBuilder() {

        @Override
        public Scan build() {
          return new Scan() { // scan for Batch reading

            @Override
            public StructType readSchema() {
              return BINARY_SCHEMA;
            }

            @Override
            public Batch toBatch() {
              return new BeamBatch<>(options);
            }
          };
        }
      };
    }

    @Override
    public String name() {
      return "BeamSource";
    }

    @Override
    public StructType schema() {
      return BINARY_SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
      final ImmutableSet<TableCapability> capabilities =
          ImmutableSet.of(TableCapability.BATCH_READ);
      return capabilities;
    }

    private static class BeamBatch<T> implements Batch, Serializable {

      private final int numPartitions;
      private final BoundedSource<T> source;
      private final SerializablePipelineOptions serializablePipelineOptions;

      private BeamBatch(CaseInsensitiveStringMap options) {
        if (Strings.isNullOrEmpty(options.get(BEAM_SOURCE_OPTION))) {
          throw new RuntimeException("Beam source was not set in DataSource options");
        }
        this.source =
            Base64Serializer.deserializeUnchecked(
                options.get(BEAM_SOURCE_OPTION), BoundedSource.class);

        if (Strings.isNullOrEmpty(DEFAULT_PARALLELISM)) {
          throw new RuntimeException("Spark default parallelism was not set in DataSource options");
        }
        this.numPartitions = Integer.parseInt(options.get(DEFAULT_PARALLELISM));
        checkArgument(numPartitions > 0, "Number of partitions must be greater than zero.");

        if (Strings.isNullOrEmpty(options.get(PIPELINE_OPTIONS))) {
          throw new RuntimeException("Beam pipelineOptions were not set in DataSource options");
        }
        this.serializablePipelineOptions =
            new SerializablePipelineOptions(options.get(PIPELINE_OPTIONS));
      }

      @Override
      public InputPartition[] planInputPartitions() {
        PipelineOptions options = serializablePipelineOptions.get();
        long desiredSizeBytes;

        try {
          desiredSizeBytes = source.getEstimatedSizeBytes(options) / numPartitions;
          List<? extends BoundedSource<T>> splits = source.split(desiredSizeBytes, options);
          InputPartition[] result = new InputPartition[splits.size()];
          int i = 0;
          for (BoundedSource<T> split : splits) {
            result[i++] = new BeamInputPartition<>(split);
          }
          return result;
        } catch (Exception e) {
          throw new RuntimeException(
              "Error in splitting BoundedSource " + source.getClass().getCanonicalName(), e);
        }
      }

      @Override
      public PartitionReaderFactory createReaderFactory() {
        return new PartitionReaderFactory() {

          @Override
          public PartitionReader<InternalRow> createReader(InputPartition partition) {
            return new BeamPartitionReader<T>(
                ((BeamInputPartition<T>) partition).getSource(), serializablePipelineOptions);
          }
        };
      }

      private static class BeamInputPartition<T> implements InputPartition {

        private final BoundedSource<T> source;

        private BeamInputPartition(BoundedSource<T> source) {
          this.source = source;
        }

        public BoundedSource<T> getSource() {
          return source;
        }
      }

      private static class BeamPartitionReader<T> implements PartitionReader<InternalRow> {

        private final BoundedSource<T> source;
        private final BoundedSource.BoundedReader<T> reader;
        private boolean started;
        private boolean closed;

        BeamPartitionReader(
            BoundedSource<T> source, SerializablePipelineOptions serializablePipelineOptions) {
          this.started = false;
          this.closed = false;
          this.source = source;
          // reader is not serializable so lazy initialize it
          try {
            reader =
                source.createReader(serializablePipelineOptions.get().as(PipelineOptions.class));
          } catch (IOException e) {
            throw new RuntimeException("Error creating BoundedReader ", e);
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
    }
  }
}
