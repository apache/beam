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

import static org.apache.beam.sdk.io.iceberg.AssignDestinationsAndPartitions.DESTINATION;
import static org.apache.beam.sdk.io.iceberg.AssignDestinationsAndPartitions.PARTITION;
import static org.apache.beam.sdk.io.iceberg.RecordWriterManager.getPartitionDataPath;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WritePartitionedRowsToFiles
    extends PTransform<PCollection<KV<Row, Iterable<Row>>>, PCollection<FileWriteResult>> {
  private static final Logger LOG = LoggerFactory.getLogger(WritePartitionedRowsToFiles.class);
  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
  }

  @Override
  public PCollection<FileWriteResult> expand(PCollection<KV<Row, Iterable<Row>>> input) {
    Schema dataSchema =
        ((RowCoder)
                ((IterableCoder<Row>)
                        ((KvCoder<Row, Iterable<Row>>) input.getCoder()).getValueCoder())
                    .getElemCoder())
            .getSchema();
    return input.apply(
        ParDo.of(new WriteDoFn(catalogConfig, dynamicDestinations, filePrefix, dataSchema)));
  }

  private static class WriteDoFn extends DoFn<KV<Row, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final String filePrefix;
    private final Schema dataSchema;
    static final Cache<TableIdentifier, LastRefreshedTable> LAST_REFRESHED_TABLE_CACHE =
        CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    private @MonotonicNonNull Map<String, PartitionField> partitionFieldMap;

    WriteDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        String filePrefix,
        Schema dataSchema) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.dataSchema = dataSchema;
    }

    private long id = UUID.randomUUID().getLeastSignificantBits();

    @Setup
    public void setup() {
      id = UUID.randomUUID().getLeastSignificantBits();
    }

    @StartBundle
    public void startBundle() {
      System.out.printf("[%s] new bundle\n", id);
    }

    @ProcessElement
    public void processElement(
        @Element KV<Row, Iterable<Row>> element, OutputReceiver<FileWriteResult> out)
        throws Exception {
      System.out.println(String.format("[%s] partition key: %s\n", id, element.getKey()));
      String tableIdentifier = checkStateNotNull(element.getKey().getString(DESTINATION));
      String partitionPath = checkStateNotNull(element.getKey().getString(PARTITION));

      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      Table table = getOrCreateTable(destination, dataSchema);

      if (partitionFieldMap == null) {
        partitionFieldMap = Maps.newHashMap();
        for (PartitionField partitionField : table.spec().fields()) {
          partitionFieldMap.put(partitionField.name(), partitionField);
        }
      }
      partitionPath = getPartitionDataPath(partitionPath, partitionFieldMap);

      StructLike partitionData =
          table.spec().isPartitioned()
              ? DataFiles.data(table.spec(), partitionPath)
              : new PartitionKey(table.spec(), table.schema());

      String fileName =
          destination
              .getFileFormat()
              .addExtension(String.format("%s-%s", filePrefix, UUID.randomUUID()));

      RecordWriter writer =
          new RecordWriter(table, destination.getFileFormat(), fileName, partitionData);
      try {
        for (Row row : element.getValue()) {
          Record record = IcebergUtils.beamRowToIcebergRecord(table.schema(), row);
          writer.write(record);
        }
      } finally {
        writer.close();
      }

      SerializableDataFile sdf = SerializableDataFile.from(writer.getDataFile(), partitionPath);
      out.output(
          FileWriteResult.builder()
              .setTableIdentifier(destination.getTableIdentifier())
              .setSerializableDataFile(sdf)
              .build());
    }

    static final class LastRefreshedTable {
      final Table table;
      volatile Instant lastRefreshTime;
      static final Duration STALENESS_THRESHOLD = Duration.ofMinutes(2);

      LastRefreshedTable(Table table, Instant lastRefreshTime) {
        this.table = table;
        this.lastRefreshTime = lastRefreshTime;
      }

      /**
       * Refreshes the table metadata if it is considered stale (older than 2 minutes).
       *
       * <p>This method first performs a non-synchronized check on the table's freshness. This
       * provides a lock-free fast path that avoids synchronization overhead in the common case
       * where the table does not need to be refreshed. If the table might be stale, it then enters
       * a synchronized block to ensure that only one thread performs the refresh operation.
       */
      void refreshIfStale() {
        // Fast path: Avoid entering the synchronized block if the table is not stale.
        if (lastRefreshTime.isAfter(Instant.now().minus(STALENESS_THRESHOLD))) {
          return;
        }
        synchronized (this) {
          if (lastRefreshTime.isBefore(Instant.now().minus(STALENESS_THRESHOLD))) {
            table.refresh();
            lastRefreshTime = Instant.now();
          }
        }
      }
    }

    Table getOrCreateTable(IcebergDestination destination, Schema dataSchema) {
      Catalog catalog = catalogConfig.catalog();
      TableIdentifier identifier = destination.getTableIdentifier();
      @Nullable
      LastRefreshedTable lastRefreshedTable = LAST_REFRESHED_TABLE_CACHE.getIfPresent(identifier);
      if (lastRefreshedTable != null && lastRefreshedTable.table != null) {
        lastRefreshedTable.refreshIfStale();
        return lastRefreshedTable.table;
      }

      Namespace namespace = identifier.namespace();
      @Nullable IcebergTableCreateConfig createConfig = destination.getTableCreateConfig();
      PartitionSpec partitionSpec =
          createConfig != null ? createConfig.getPartitionSpec() : PartitionSpec.unpartitioned();
      Map<String, String> tableProperties =
          createConfig != null && createConfig.getTableProperties() != null
              ? createConfig.getTableProperties()
              : Maps.newHashMap();

      @Nullable Table table = null;
      synchronized (LAST_REFRESHED_TABLE_CACHE) {
        lastRefreshedTable = LAST_REFRESHED_TABLE_CACHE.getIfPresent(identifier);
        if (lastRefreshedTable != null && lastRefreshedTable.table != null) {
          lastRefreshedTable.refreshIfStale();
          return lastRefreshedTable.table;
        }

        // Create namespace if it does not exist yet
        if (!namespace.isEmpty() && catalog instanceof SupportsNamespaces) {
          SupportsNamespaces supportsNamespaces = (SupportsNamespaces) catalog;
          if (!supportsNamespaces.namespaceExists(namespace)) {
            try {
              supportsNamespaces.createNamespace(namespace);
              LOG.info("Created new namespace '{}'.", namespace);
            } catch (AlreadyExistsException ignored) {
              // race condition: another worker already created this namespace
              LOG.info("Namespace `{}` already exists.", namespace);
            }
          }
        }

        // If table exists, just load it
        // Note: the implementation of catalog.tableExists() will load the table to check its
        // existence. We don't use it here to avoid double loadTable() calls.
        try {
          table = catalog.loadTable(identifier);
        } catch (NoSuchTableException e) { // Otherwise, create the table
          org.apache.iceberg.Schema tableSchema =
              IcebergUtils.beamSchemaToIcebergSchema(dataSchema);
          try {
            table = catalog.createTable(identifier, tableSchema, partitionSpec, tableProperties);
            LOG.info(
                "Created Iceberg table '{}' with schema: {}\n"
                    + ", partition spec: {}, table properties: {}",
                identifier,
                tableSchema,
                partitionSpec,
                tableProperties);
          } catch (AlreadyExistsException ignored) {
            // race condition: another worker already created this table
            table = catalog.loadTable(identifier);
          }
        }
      }
      lastRefreshedTable = new LastRefreshedTable(table, Instant.now());
      LAST_REFRESHED_TABLE_CACHE.put(identifier, lastRefreshedTable);
      return table;
    }
  }
}
