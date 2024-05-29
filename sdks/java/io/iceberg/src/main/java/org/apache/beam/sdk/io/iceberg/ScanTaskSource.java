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
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Source that reads the data described by a single CombinedScanTask. This source is not splittable.
 */
class ScanTaskSource extends BoundedSource<Row> {

  private final CombinedScanTask task;
  private final IcebergScanConfig scanConfig;

  public ScanTaskSource(IcebergScanConfig scanConfig, CombinedScanTask task) {
    this.scanConfig = scanConfig;
    this.task = task;
  }

  @Pure
  Table getTable() {
    return scanConfig.getTable();
  }

  @Pure
  CombinedScanTask getTask() {
    return task;
  }

  @Pure
  Schema getSchema() {
    return scanConfig.getSchema();
  }

  @Override
  public List<? extends BoundedSource<Row>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    return ImmutableList.of(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return task.sizeBytes();
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
    return new ScanTaskReader(this);
  }
}
