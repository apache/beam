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
package org.apache.beam.runners.spark.structuredstreaming.translation.io;

import static scala.collection.JavaConversions.asScalaBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.joda.time.Instant;

/**
 * This is a mock source that gives values between 0 and 999.
 */
public class DatasetSourceMock implements DataSourceV2, MicroBatchReadSupport {

  @Override public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    return new DatasetMicroBatchReader();
  }

  /** This class can be mapped to Beam {@link BoundedSource}. */
  private class DatasetMicroBatchReader implements MicroBatchReader {

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
    }

    @Override public void stop() {
    }

    @Override public StructType readSchema() {
      return null;
    }

    @Override public List<InputPartition<InternalRow>> planInputPartitions() {
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      result.add(new InputPartition<InternalRow>() {

        @Override public InputPartitionReader<InternalRow> createPartitionReader() {
          return new DatasetMicroBatchPartitionReaderMock();
        }
      });
      return result;
    }
  }

  /** This class is a mocked reader*/
  private class DatasetMicroBatchPartitionReaderMock implements InputPartitionReader<InternalRow> {

    private ArrayList<Integer> values;
    private int currentIndex = 0;

    private DatasetMicroBatchPartitionReaderMock() {
      for (int i = 0; i < 1000; i++){
        values.add(i);
      }
    }

    @Override public boolean next() throws IOException {
      currentIndex++;
      return (currentIndex <= values.size());
    }

    @Override public void close() throws IOException {
    }

    @Override public InternalRow get() {
      List<Object> list = new ArrayList<>();
      list.add(WindowedValue.timestampedValueInGlobalWindow(values.get(currentIndex), new Instant()));
      return InternalRow.apply(asScalaBuffer(list).toList());
    }
  }
}