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
package org.apache.beam.io.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.io.iceberg.util.SchemaHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
public class IcebergBoundedSource extends BoundedSource<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergBoundedSource.class);

  private @Nullable CombinedScanTask task;
  private Iceberg.Scan scan;

  public IcebergBoundedSource(Iceberg.Scan scan, @Nullable CombinedScanTask task) {
    this.task = task;
    this.scan = scan;
  }

  public IcebergBoundedSource(Iceberg.Scan scan) {
    this(scan, null);
  }

  public @Nullable Catalog catalog() {
    return scan.getCatalog().catalog();
  }

  public @Nullable Table table() {
    Catalog catalog = catalog();
    if (catalog != null) {
      return catalog.loadTable(
          TableIdentifier.of(scan.getTable().toArray(new String[scan.getTable().size()])));
    } else {
      return null;
    }
  }

  @Override
  public List<? extends BoundedSource<Row>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    ArrayList<IcebergBoundedSource> tasks = new ArrayList<>();
    Table table = table();
    if (table != null) {

      switch (scan.getType()) {
        case TABLE:
          // Override the split size with our desired size
          TableScan tableScan = table.newScan();

          if (desiredBundleSizeBytes > 0) {
            tableScan = tableScan.option(TableProperties.SPLIT_SIZE, "" + desiredBundleSizeBytes);
          }

          // Always project to our destination schema
          tableScan = tableScan.project(SchemaHelper.convert(scan.getSchema()));

          if (scan.getFilter() != null) {
            tableScan = tableScan.filter(scan.getFilter());
          }
          if (scan.getCaseSensitive() != null) {
            tableScan = tableScan.caseSensitive(scan.getCaseSensitive());
          }
          if (scan.getSnapshot() != null) {
            tableScan = tableScan.useSnapshot(scan.getSnapshot());
          }
          if (scan.getBranch() != null) {
            tableScan = tableScan.useRef(scan.getBranch());
          } else if (scan.getTag() != null) {
            tableScan = tableScan.useRef(scan.getTag());
          }
          try (CloseableIterable<CombinedScanTask> t = tableScan.planTasks()) {
            for (CombinedScanTask c : t) {
              tasks.add(new IcebergBoundedSource(scan, c));
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
        case BATCH:
          // TODO: Add batch scan
          break;
      }
    }
    return tasks;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (task == null) {
      return 0;
    } else {
      return task.sizeBytes();
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  @Override
  public Coder<Row> getOutputCoder() {
    return RowCoder.of(scan.getSchema());
  }

  @Override
  public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
    return new CombinedScanReader(this, task, scan.getSchema());
  }
}
