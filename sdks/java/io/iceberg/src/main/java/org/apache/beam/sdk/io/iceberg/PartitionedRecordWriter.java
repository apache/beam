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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer that opens and closes {@link RecordWriter}s as necessary for multiple tables, and
 * multiple partitions within each table. If the Iceberg {@link Table} is un-partitioned, the data
 * is written normally.
 *
 * <p>Each table has a {@link DestinationState} that creates a new {@link RecordWriter} for each
 * partition encountered. If a {@link RecordWriter} is inactive for 5 minutes, the {@link
 * DestinationState} will automatically close it to free up resources.
 *
 * <p>At any moment, the number of open data writers is at most equal to the number of partitions
 * across destinations. Closing this {@link PartitionedRecordWriter} will close all {@link
 * RecordWriter}s contained within.
 */
public class PartitionedRecordWriter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedRecordWriter.class);

  class DestinationState {
    private final IcebergDestination icebergDestination;
    private final PartitionKey partitionKey;
    private final org.apache.iceberg.Schema schema;
    private final Map<IcebergDestination, List<WindowedValue<ManifestFile>>> manifestFiles =
        Maps.newHashMap();
    private final Cache<PartitionKey, WindowedValue<RecordWriter>> writers;

    DestinationState(
        IcebergDestination icebergDestination,
        PartitionSpec partitionSpec,
        org.apache.iceberg.Schema schema) {
      this.icebergDestination = icebergDestination;
      this.schema = schema;
      this.partitionKey = new PartitionKey(partitionSpec, schema);

      // build a cache of RecordWriters
      // writers will expire after 5min of idle time
      // when a writer expires, its manifest file is collected
      this.writers =
          CacheBuilder.newBuilder()
              .expireAfterAccess(5, TimeUnit.MINUTES)
              .removalListener(
                  (RemovalNotification<PartitionKey, WindowedValue<RecordWriter>> removal) -> {
                    final @Nullable PartitionKey partitionKey = removal.getKey();
                    String message =
                        partitionKey == null
                            ? ""
                            : String.format(", partition '%s'.", partitionKey);
                    final @Nullable WindowedValue<RecordWriter> recordWriter = removal.getValue();
                    if (recordWriter != null) {
                      try {
                        LOG.info(
                            "Closing record writer for table '{}'" + message,
                            icebergDestination.getTableIdentifier());
                        recordWriter.getValue().close();
                        manifestFiles
                            .computeIfAbsent(icebergDestination, d -> Lists.newArrayList())
                            .add(
                                WindowedValue.of(
                                    recordWriter.getValue().getManifestFile(),
                                    recordWriter.getTimestamp(),
                                    recordWriter.getWindows(),
                                    recordWriter.getPane()));
                      } catch (IOException e) {
                        throw new RuntimeException(
                            "Encountered an error when closing data writer for table "
                                + icebergDestination.getTableIdentifier()
                                + message,
                            e);
                      }
                    }
                  })
              .build();
    }

    void write(Record record, BoundedWindow window, PaneInfo pane)
        throws IOException, ExecutionException {
      partitionKey.partition(record);
      RecordWriter writer = fetchWriterForPartition(partitionKey, window, pane);
      writer.write(record);
    }

    private RecordWriter fetchWriterForPartition(
        PartitionKey partitionKey, BoundedWindow window, PaneInfo paneInfo)
        throws ExecutionException {
      RecordWriter recordWriter =
          writers
              .get(
                  partitionKey,
                  () ->
                      WindowedValue.of(
                          createWriter(partitionKey), window.maxTimestamp(), window, paneInfo))
              .getValue();

      if (recordWriter.bytesWritten() > maxFileSize) {
        writers.invalidate(partitionKey);
        recordWriter = createWriter(partitionKey);
        writers.put(
            partitionKey,
            WindowedValue.of(createWriter(partitionKey), window.maxTimestamp(), window, paneInfo));
      }
      return recordWriter;
    }

    private RecordWriter createWriter(PartitionKey partitionKey) {
      try {
        return new RecordWriter(
            catalog, icebergDestination, fileSuffix + "-" + UUID.randomUUID(), partitionKey);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Encountered an error when creating a RecordWriter for table '%s', partition %s.",
                icebergDestination.getTableIdentifier(), partitionKey),
            e);
      }
    }

    void closeAllWriters() throws Exception {
      List<Exception> exceptionList = Lists.newArrayList();
      for (PartitionKey pk : writers.asMap().keySet()) {
        try {
          writers.invalidate(pk);
        } catch (Exception e) {
          exceptionList.add(e);
        }
      }
      if (!exceptionList.isEmpty()) {
        Exception e = new IOException("Exception closing some writers. See suppressed exceptions.");
        for (Exception thrown : exceptionList) {
          e.addSuppressed(thrown);
        }
        throw e;
      }
    }
  }

  private final Catalog catalog;
  private final String fileSuffix;
  private final long maxFileSize;
  private final int maxNumWriters;
  private Map<IcebergDestination, DestinationState> destinations = Maps.newHashMap();
  private final Map<IcebergDestination, List<WindowedValue<ManifestFile>>> totalManifestFiles =
      Maps.newHashMap();
  private boolean isClosed = false;

  PartitionedRecordWriter(Catalog catalog, String fileSuffix, long maxFileSize, int maxNumWriters) {
    this.catalog = catalog;
    this.fileSuffix = fileSuffix;
    this.maxFileSize = maxFileSize;
    this.maxNumWriters = maxNumWriters;
  }

  /**
   * Fetches the {@link RecordWriter} for the appropriate partition in this destination and writes
   * the record.
   */
  public boolean write(
      IcebergDestination icebergDestination, Row row, BoundedWindow window, PaneInfo pane)
      throws IOException, ExecutionException {
    if (destinations.size() > maxNumWriters) {
      return false;
    }
    DestinationState destinationState =
        destinations.computeIfAbsent(
            icebergDestination,
            destination -> {
              Table table = catalog.loadTable(destination.getTableIdentifier());
              return new DestinationState(destination, table.spec(), table.schema());
            });

    Record icebergRecord = IcebergUtils.beamRowToIcebergRecord(destinationState.schema, row);
    destinationState.write(icebergRecord, window, pane);
    return true;
  }

  /** Closes all remaining writers and collects all their {@link ManifestFile}s. */
  public void close() throws Exception {
    for (DestinationState state : destinations.values()) {
      state.closeAllWriters();
      for (Map.Entry<IcebergDestination, List<WindowedValue<ManifestFile>>> entry :
          state.manifestFiles.entrySet()) {
        totalManifestFiles
            .computeIfAbsent(entry.getKey(), icebergDestination -> Lists.newArrayList())
            .addAll(entry.getValue());
      }
      state.manifestFiles.clear();
    }
    destinations.clear();
    isClosed = true;
  }

  /**
   * Returns a mapping of {@link IcebergDestination}s to accumulated {@link ManifestFile}s outputted
   * by all {@link RecordWriter}s.
   */
  public Map<IcebergDestination, List<WindowedValue<ManifestFile>>> getManifestFiles() {
    Preconditions.checkArgument(
        isClosed,
        "Please close this %s before retrieving its manifest files.",
        getClass().getSimpleName());
    return totalManifestFiles;
  }
}
