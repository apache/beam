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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/** Utils to interact with an HBase cluster and get information on tables/regions. */
class HBaseUtils {

  /**
   * Estimates the size in bytes that a scan will cover for a given table based on HBase store file
   * information. The size can vary between calls because the data can be compressed based on the
   * HBase configuration.
   */
  static long estimateSizeBytes(Connection connection, String tableId, ByteKeyRange range)
      throws Exception {
    // This code is based on RegionSizeCalculator in hbase-server
    long estimatedSizeBytes = 0L;
    List<HRegionLocation> regionLocations = getRegionLocations(connection, tableId, range);

    // builds set of regions who are part of the table scan
    Set<byte[]> tableRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (HRegionLocation regionLocation : regionLocations) {
      tableRegions.add(regionLocation.getRegionInfo().getRegionName());
    }

    // calculate estimated size for the regions
    Admin admin = connection.getAdmin();
    ClusterStatus clusterStatus = admin.getClusterStatus();
    Collection<ServerName> servers = clusterStatus.getServers();
    for (ServerName serverName : servers) {
      ServerLoad serverLoad = clusterStatus.getLoad(serverName);
      for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
        byte[] regionId = regionLoad.getName();
        if (tableRegions.contains(regionId)) {
          long regionSizeBytes = regionLoad.getStorefileSizeMB() * 1_048_576L;
          estimatedSizeBytes += regionSizeBytes;
        }
      }
    }

    return estimatedSizeBytes;
  }

  /** Returns a list of region locations for a given table and scan. */
  static List<HRegionLocation> getRegionLocations(
      Connection connection, String tableId, ByteKeyRange range) throws Exception {
    byte[] startRow = range.getStartKey().getBytes();
    byte[] stopRow = range.getEndKey().getBytes();

    final List<HRegionLocation> regionLocations = new ArrayList<>();

    final boolean scanWithNoLowerBound = startRow.length == 0;
    final boolean scanWithNoUpperBound = stopRow.length == 0;

    TableName tableName = TableName.valueOf(tableId);
    RegionLocator regionLocator = connection.getRegionLocator(tableName);
    List<HRegionLocation> tableRegionInfos = regionLocator.getAllRegionLocations();
    for (HRegionLocation regionLocation : tableRegionInfos) {
      final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
      final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
      boolean isLastRegion = endKey.length == 0;
      // filters regions who are part of the scan
      if ((scanWithNoLowerBound || isLastRegion || Bytes.compareTo(startRow, endKey) < 0)
          && (scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {
        regionLocations.add(regionLocation);
      }
    }

    return regionLocations;
  }

  /** Returns the list of ranges intersecting a list of regions for the given table and scan. */
  static List<ByteKeyRange> getRanges(
      List<HRegionLocation> regionLocations, String tableId, ByteKeyRange range) {
    byte[] startRow = range.getStartKey().getBytes();
    byte[] stopRow = range.getEndKey().getBytes();

    final List<ByteKeyRange> splits = new ArrayList<>();
    final boolean scanWithNoLowerBound = startRow.length == 0;
    final boolean scanWithNoUpperBound = stopRow.length == 0;

    for (HRegionLocation regionLocation : regionLocations) {
      final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
      final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
      boolean isLastRegion = endKey.length == 0;

      final byte[] splitStart =
          (scanWithNoLowerBound || Bytes.compareTo(startKey, startRow) >= 0) ? startKey : startRow;
      final byte[] splitStop =
          (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0) && !isLastRegion
              ? endKey
              : stopRow;
      splits.add(ByteKeyRange.of(ByteKey.copyFrom(splitStart), ByteKey.copyFrom(splitStop)));
    }
    return splits;
  }

  static ByteKeyRange getByteKeyRange(Scan scan) {
    return ByteKeyRange.of(
        ByteKey.copyFrom(scan.getStartRow()), ByteKey.copyFrom(scan.getStopRow()));
  }

  static Scan newScanInRange(Scan scan, ByteKeyRange range) throws IOException {
    return new Scan(scan)
        .setStartRow(range.getStartKey().getBytes())
        .setStopRow(range.getEndKey().getBytes());
  }
}
