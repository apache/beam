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

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

/**
 * This is a spark structured streaming {@link DataSourceV2} implementation. As Continuous streaming
 * is tagged experimental in spark, this class does no implement {@link ContinuousReadSupport}.
 */
public class DatasetSourceStreaming<T> implements DataSourceV2, MicroBatchReadSupport {

  static final String BEAM_SOURCE_OPTION = "beam-source";
  static final String DEFAULT_PARALLELISM = "default-parallelism";
  static final String PIPELINE_OPTIONS = "pipeline-options";

  @Override
  public MicroBatchReader createMicroBatchReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    return new DatasetMicroBatchReader(checkpointLocation, options);
  }

  /** This class can be mapped to Beam {@link BoundedSource}. */
  private static class DatasetMicroBatchReader implements MicroBatchReader {

    @SuppressWarnings("unused")
    private DatasetMicroBatchReader(String checkpointLocation, DataSourceOptions options) {
      //TODO deal with schema and options
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
      //TODO extension point for SDF
    }

    @Override
    public Offset getStartOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public Offset getEndOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public Offset deserializeOffset(String json) {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public void commit(Offset end) {
      //TODO no more to read after end Offset
    }

    @Override
    public void stop() {}

    @Override
    public StructType readSchema() {
      return null;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      return null;
    }
  }
}
