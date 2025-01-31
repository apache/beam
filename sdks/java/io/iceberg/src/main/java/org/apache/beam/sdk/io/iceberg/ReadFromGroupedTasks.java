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

import static org.apache.beam.sdk.io.iceberg.ReadFromTasks.getReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Unbounded read implementation.
 *
 * <p>An SDF that takes a batch of {@link ReadTask}s. For each task, reads Iceberg {@link Record}s,
 * and converts to Beam {@link Row}s.
 *
 * <p>The split granularity is set to the incoming batch size, i.e. the number of potential splits
 * equals the batch size.
 */
@DoFn.BoundedPerElement
class ReadFromGroupedTasks
    extends DoFn<KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>>, Row> {
  private final IcebergCatalogConfig catalogConfig;

  ReadFromGroupedTasks(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void process(
      @Element KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws IOException, ExecutionException {
    String tableIdentifier = element.getKey().getKey().getTableIdentifierString();
    List<ReadTask> readTasks = Lists.newArrayList(element.getValue());
    Table table = TableCache.get(tableIdentifier, catalogConfig.catalog());
    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(table.schema());
    @Nullable String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping =
        nameMapping != null ? NameMappingParser.fromJson(nameMapping) : NameMapping.empty();

    // SDF can split by the number of read tasks
    for (long taskIndex = tracker.currentRestriction().getFrom();
        taskIndex < tracker.currentRestriction().getTo();
        ++taskIndex) {
      if (!tracker.tryClaim(taskIndex)) {
        return;
      }
      FileScanTask task = readTasks.get((int) taskIndex).getFileScanTask();
      try (CloseableIterable<Record> reader = getReader(task, table, mapping)) {
        for (Record record : reader) {
          Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
          out.output(row);
        }
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(
      @Element KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>> element) {
    return new OffsetRange(0, Iterables.size(element.getValue()));
  }

  @GetSize
  public double getSize(
      @Element KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>> element,
      @Restriction OffsetRange restriction)
      throws Exception {
    double size = 0;
    Iterator<ReadTask> iterator = element.getValue().iterator();
    for (long i = 0; i < restriction.getTo() && iterator.hasNext(); i++) {
      ReadTask task = iterator.next();
      if (i >= restriction.getFrom()) {
        size += task.getByteSize();
      }
    }
    return size;
  }
}
