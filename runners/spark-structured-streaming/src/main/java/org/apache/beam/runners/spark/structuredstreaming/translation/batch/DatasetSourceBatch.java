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

import static com.google.common.base.Preconditions.checkArgument;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

/**
 * This is a spark structured streaming {@link DataSourceV2} implementation. As Continuous streaming
 * is tagged experimental in spark, this class does no implement {@link ContinuousReadSupport}.
 */
public class DatasetSourceBatch<T> implements DataSourceV2, ReadSupport {

  private int numPartitions;
  private Long bundleSize;
  private TranslationContext context;
  private BoundedSource<T> source;


  @Override public DataSourceReader createReader(DataSourceOptions options) {
    this.numPartitions = context.getSparkSession().sparkContext().defaultParallelism();
    checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");
    this.bundleSize = context.getOptions().getBundleSize();
    return new DatasetReader();  }

  /** This class can be mapped to Beam {@link BoundedSource}. */
  private class DatasetReader implements DataSourceReader {

    private Optional<StructType> schema;
    private String checkpointLocation;
    private DataSourceOptions options;

    @Override
    public StructType readSchema() {
      return new StructType();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      long desiredSizeBytes;
      SparkPipelineOptions options = context.getOptions();
      try {
        desiredSizeBytes =
            (bundleSize == null)
                ? source.getEstimatedSizeBytes(options) / numPartitions
                : bundleSize;
        List<? extends BoundedSource<T>> sources = source.split(desiredSizeBytes, options);
        for (BoundedSource<T> source : sources) {
          result.add(
              new InputPartition<InternalRow>() {

                @Override
                public InputPartitionReader<InternalRow> createPartitionReader() {
                  BoundedReader<T> reader = null;
                  try {
                    reader = source.createReader(options);
                  } catch (IOException e) {
                    throw new RuntimeException(
                        "Error creating BoundedReader " + reader.getClass().getCanonicalName(), e);
                  }
                  return new DatasetPartitionReader(reader);
                }
              });
        }
        return result;

      } catch (Exception e) {
        throw new RuntimeException(
            "Error in splitting BoundedSource " + source.getClass().getCanonicalName(), e);
      }
    }
  }

  /** This class can be mapped to Beam {@link BoundedReader} */
  private class DatasetPartitionReader implements InputPartitionReader<InternalRow> {

    BoundedReader<T> reader;
    private boolean started;
    private boolean closed;

    DatasetPartitionReader(BoundedReader<T> reader) {
      this.reader = reader;
      this.started = false;
      this.closed = false;
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
      List<Object> list = new ArrayList<>();
      list.add(
          WindowedValue.timestampedValueInGlobalWindow(
              reader.getCurrent(), reader.getCurrentTimestamp()));
      return InternalRow.apply(asScalaBuffer(list).toList());
    }

    @Override
    public void close() throws IOException {
      closed = true;
      reader.close();
    }
  }
}
