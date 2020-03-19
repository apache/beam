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

import java.util.List;
import org.apache.beam.sdk.io.hbase.HBaseIO.Read;
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
import org.apache.hadoop.hbase.client.Table;

/** A SplittableDoFn to read from HBase. */
@BoundedPerElement
class HBaseReadSplittableDoFn extends DoFn<Read, Result> {
  HBaseReadSplittableDoFn() {}

  @ProcessElement
  public void processElement(
      @Element Read read,
      OutputReceiver<Result> out,
      RestrictionTracker<ByteKeyRange, ByteKey> tracker)
      throws Exception {
    Connection connection = ConnectionFactory.createConnection(read.getConfiguration());
    TableName tableName = TableName.valueOf(read.getTableId());
    Table table = connection.getTable(tableName);
    final ByteKeyRange range = tracker.currentRestriction();
    try (ResultScanner scanner =
        table.getScanner(HBaseUtils.newScanInRange(read.getScan(), range))) {
      for (Result result : scanner) {
        ByteKey key = ByteKey.copyFrom(result.getRow());
        if (!tracker.tryClaim(key)) {
          return;
        }
        out.output(result);
      }
      tracker.tryClaim(ByteKey.EMPTY);
    }
  }

  @GetInitialRestriction
  public ByteKeyRange getInitialRestriction(@Element Read read) {
    return HBaseUtils.getByteKeyRange(read.getScan());
  }

  @SplitRestriction
  public void splitRestriction(
      @Element Read read, @Restriction ByteKeyRange range, OutputReceiver<ByteKeyRange> receiver)
      throws Exception {
    Connection connection = ConnectionFactory.createConnection(read.getConfiguration());
    List<HRegionLocation> regionLocations =
        HBaseUtils.getRegionLocations(connection, read.getTableId(), range);
    List<ByteKeyRange> splitRanges =
        HBaseUtils.getRanges(regionLocations, read.getTableId(), range);
    for (ByteKeyRange splitRange : splitRanges) {
      receiver.output(ByteKeyRange.of(splitRange.getStartKey(), splitRange.getEndKey()));
    }
  }

  @NewTracker
  public ByteKeyRangeTracker newTracker(@Restriction ByteKeyRange range) {
    return ByteKeyRangeTracker.of(range);
  }
}
