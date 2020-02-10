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
package org.apache.beam.sdk.io.hbase;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/** A SplittableDoFn to read from HBase. */
@BoundedPerElement
class HBaseReadSplittableDoFn extends DoFn<HBaseQuery, Result> {
  private final SerializableConfiguration serializableConfiguration;

  private transient Connection connection;

  HBaseReadSplittableDoFn(SerializableConfiguration serializableConfiguration) {
    this.serializableConfiguration = serializableConfiguration;
  }

  @Setup
  public void setup() throws Exception {
    connection = ConnectionFactory.createConnection(serializableConfiguration.get());
  }

  private static Scan newScanInRange(Scan scan, ByteKeyRange range) throws IOException {
    return new Scan(scan)
        .setStartRow(range.getStartKey().getBytes())
        .setStopRow(range.getEndKey().getBytes());
  }

  @ProcessElement
  public void processElement(ProcessContext c, RestrictionTracker<ByteKeyRange, ByteKey> tracker)
      throws Exception {
    final HBaseQuery query = c.element();
    TableName tableName = TableName.valueOf(query.getTableId());
    Table table = connection.getTable(tableName);
    final ByteKeyRange range = tracker.currentRestriction();
    try (ResultScanner scanner = table.getScanner(newScanInRange(query.getScan(), range))) {
      for (Result result : scanner) {
        ByteKey key = ByteKey.copyFrom(result.getRow());
        if (!tracker.tryClaim(key)) {
          return;
        }
        c.output(result);
      }
      tracker.tryClaim(ByteKey.EMPTY);
    }
  }

  @GetInitialRestriction
  public ByteKeyRange getInitialRestriction(@Element HBaseQuery query) {
    return HBaseUtils.getByteKeyRange(query.getScan());
  }

  @SplitRestriction
  public void splitRestriction(
      @Element HBaseQuery query,
      @Restriction ByteKeyRange range,
      OutputReceiver<ByteKeyRange> receiver)
      throws Exception {
    List<HRegionLocation> regionLocations =
        HBaseUtils.getRegionLocations(connection, query.getTableId(), range);
    List<ByteKeyRange> splitRanges =
        HBaseUtils.getRanges(regionLocations, query.getTableId(), range);
    for (ByteKeyRange splitRange : splitRanges) {
      receiver.output(ByteKeyRange.of(splitRange.getStartKey(), splitRange.getEndKey()));
    }
  }

  @NewTracker
  public ByteKeyRangeTracker newTracker(@Restriction ByteKeyRange range) {
    return ByteKeyRangeTracker.of(range);
  }

  @Teardown
  public void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }
}
