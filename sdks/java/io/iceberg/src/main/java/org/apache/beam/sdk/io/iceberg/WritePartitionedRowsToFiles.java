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
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.util.PropertyUtil;
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

    @ProcessElement
    public void processElement(
        @Element KV<Row, Iterable<Row>> element,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputReceiver<FileWriteResult> out)
        throws Exception {
      String tableIdentifier = checkStateNotNull(element.getKey().getString(DESTINATION));

      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      Table table = getOrCreateTable(destination, dataSchema);

      long maxFileSize =
          PropertyUtil.propertyAsLong(
              table.properties(),
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      WindowedValue<IcebergDestination> windowedDestination =
          WindowedValues.of(destination, window.maxTimestamp(), window, paneInfo);
      RecordWriterManager writer =
          new RecordWriterManager(catalogConfig, filePrefix, maxFileSize, Integer.MAX_VALUE);
      try {
        for (Row row : element.getValue()) {
          writer.write(windowedDestination, row);
        }
      } finally {
        writer.close();
      }

      @Nullable
      List<SerializableDataFile> serializableDataFiles =
          writer.getSerializableDataFiles().get(windowedDestination);
      if (serializableDataFiles == null) {
        return;
      }
      for (SerializableDataFile dataFile : serializableDataFiles) {
        out.output(
            FileWriteResult.builder()
                .setTableIdentifier(destination.getTableIdentifier())
                .setSerializableDataFile(dataFile)
                .build());
      }
    }

    Table getOrCreateTable(IcebergDestination destination, Schema dataSchema) {
      TableIdentifier identifier = destination.getTableIdentifier();
      return TableCache.getAndRefreshIfStale(
          catalogConfig,
          identifier,
          () -> loadOrCreateTable(catalogConfig.catalog(), destination, dataSchema));
    }

    private Table loadOrCreateTable(
        Catalog catalog, IcebergDestination destination, Schema dataSchema) {
      TableIdentifier identifier = destination.getTableIdentifier();
      Namespace namespace = identifier.namespace();
      @Nullable IcebergTableCreateConfig createConfig = destination.getTableCreateConfig();
      PartitionSpec partitionSpec =
          createConfig != null ? createConfig.getPartitionSpec() : PartitionSpec.unpartitioned();
      Map<String, String> tableProperties =
          createConfig != null && createConfig.getTableProperties() != null
              ? createConfig.getTableProperties()
              : Maps.newHashMap();

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
        return catalog.loadTable(identifier);
      } catch (NoSuchTableException e) { // Otherwise, create the table
        org.apache.iceberg.Schema tableSchema = IcebergUtils.beamSchemaToIcebergSchema(dataSchema);
        try {
          Table table =
              catalog.createTable(identifier, tableSchema, partitionSpec, tableProperties);
          LOG.info(
              "Created Iceberg table '{}' with schema: {}\n"
                  + ", partition spec: {}, table properties: {}",
              identifier,
              tableSchema,
              partitionSpec,
              tableProperties);
          return table;
        } catch (AlreadyExistsException ignored) {
          // race condition: another worker already created this table
          return catalog.loadTable(identifier);
        }
      }
    }
  }
}
