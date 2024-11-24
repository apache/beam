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
package org.apache.beam.sdk.io.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Source that reads all the data in a table described by an IcebergScanConfig. Supports only
 * initial spliting.
 */
class ScanSource extends BoundedSource<Row> {

  private IcebergScanConfig scanConfig;

  public ScanSource(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  private TableScan getTableScan() {
    TableScan tableScan =
        scanConfig
            .getTable()
            .newScan()
            .project(IcebergUtils.beamSchemaToIcebergSchema(scanConfig.getSchema()));

    if (scanConfig.getFilter() != null) {
      tableScan = tableScan.filter(scanConfig.getFilter());
    }
    if (scanConfig.getCaseSensitive() != null) {
      tableScan = tableScan.caseSensitive(scanConfig.getCaseSensitive());
    }
    if (scanConfig.getSnapshot() != null) {
      tableScan = tableScan.useSnapshot(scanConfig.getSnapshot());
    }
    if (scanConfig.getBranch() != null) {
      tableScan = tableScan.useRef(scanConfig.getBranch());
    } else if (scanConfig.getTag() != null) {
      tableScan = tableScan.useRef(scanConfig.getTag());
    }

    return tableScan;
  }

  private CombinedScanTask wholeTableReadTask() {
    // Always project to our destination schema
    return new BaseCombinedScanTask(ImmutableList.copyOf(getTableScan().planFiles()));
  }

  @Override
  public List<? extends BoundedSource<Row>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    ArrayList<ScanTaskSource> splits = new ArrayList<>();

    switch (scanConfig.getScanType()) {
      case TABLE:
        TableScan tableScan = getTableScan();
        if (desiredBundleSizeBytes > 0) {
          tableScan =
              tableScan.option(TableProperties.SPLIT_SIZE, Long.toString(desiredBundleSizeBytes));
        }

        try (CloseableIterable<CombinedScanTask> tasks = tableScan.planTasks()) {
          for (CombinedScanTask combinedScanTask : tasks) {
            splits.add(new ScanTaskSource(scanConfig, combinedScanTask));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      case BATCH:
        throw new UnsupportedOperationException("BATCH scan not supported");
      default:
        throw new UnsupportedOperationException("Unknown scan type: " + scanConfig.getScanType());
    }

    return splits;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return wholeTableReadTask().sizeBytes();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  @Override
  public Coder<Row> getOutputCoder() {
    return RowCoder.of(scanConfig.getSchema());
  }

  @Override
  public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
    return new ScanTaskReader(new ScanTaskSource(scanConfig, wholeTableReadTask()));
  }
}
