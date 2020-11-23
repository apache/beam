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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static java.util.stream.Collectors.toMap;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.DataCatalogSettings;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.FullNameTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Uses DataCatalog to get the source type and schema for a table. */
public class DataCatalogTableProvider extends FullNameTableProvider implements AutoCloseable {

  private static final TableFactory PUBSUB_TABLE_FACTORY = new PubsubTableFactory();
  private static final TableFactory GCS_TABLE_FACTORY = new GcsTableFactory();

  private static final Map<String, TableProvider> DELEGATE_PROVIDERS =
      Stream.of(new PubsubTableProvider(), new BigQueryTableProvider(), new TextTableProvider())
          .collect(toMap(TableProvider::getTableType, p -> p));

  private final DataCatalogClient dataCatalog;
  private final Map<String, Table> tableCache;
  private final TableFactory tableFactory;

  private DataCatalogTableProvider(DataCatalogClient dataCatalog, boolean truncateTimestamps) {
    this.tableCache = new HashMap<>();
    this.dataCatalog = dataCatalog;
    this.tableFactory =
        ChainedTableFactory.of(
            PUBSUB_TABLE_FACTORY, GCS_TABLE_FACTORY, new BigQueryTableFactory(truncateTimestamps));
  }

  public static DataCatalogTableProvider create(DataCatalogPipelineOptions options) {
    return new DataCatalogTableProvider(
        createDataCatalogClient(options), options.getTruncateTimestamps());
  }

  @Override
  public String getTableType() {
    return "google.cloud.datacatalog";
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException(
        "Creating tables is not supported with DataCatalog table provider.");
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException(
        "Dropping tables is not supported with DataCatalog table provider");
  }

  @Override
  public Map<String, Table> getTables() {
    throw new UnsupportedOperationException("Loading all tables from DataCatalog is not supported");
  }

  @Override
  public @Nullable Table getTable(String tableName) {
    return loadTable(tableName);
  }

  @Override
  public Table getTableByFullName(TableName fullTableName) {

    ImmutableList<String> allNameParts =
        ImmutableList.<String>builder()
            .addAll(fullTableName.getPath())
            .add(fullTableName.getTableName())
            .build();

    String fullEscapedTableName = ZetaSqlIdUtils.escapeAndJoin(allNameParts);

    return loadTable(fullEscapedTableName);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    TableProvider tableProvider = DELEGATE_PROVIDERS.get(table.getType());
    if (tableProvider == null) {
      throw new RuntimeException("TableProvider is null");
    }
    return tableProvider.buildBeamSqlTable(table);
  }

  private Table loadTable(String tableName) {
    if (!tableCache.containsKey(tableName)) {
      tableCache.put(tableName, loadTableFromDC(tableName));
    }

    return tableCache.get(tableName);
  }

  private Table loadTableFromDC(String tableName) {
    try {
      return toCalciteTable(
          tableName,
          dataCatalog.lookupEntry(
              LookupEntryRequest.newBuilder().setSqlResource(tableName).build()));
    } catch (InvalidArgumentException | PermissionDeniedException | NotFoundException e) {
      throw new InvalidTableException("Could not resolve table in Data Catalog: " + tableName, e);
    }
  }

  private static DataCatalogClient createDataCatalogClient(DataCatalogPipelineOptions options) {
    try {
      return DataCatalogClient.create(
          DataCatalogSettings.newBuilder()
              .setCredentialsProvider(() -> options.as(GcpOptions.class).getGcpCredential())
              .setEndpoint(options.getDataCatalogEndpoint())
              .build());
    } catch (IOException e) {
      throw new RuntimeException("Error creating Data Catalog client", e);
    }
  }

  private Table toCalciteTable(String tableName, Entry entry) {
    if (entry.getSchema().getColumnsCount() == 0) {
      throw new UnsupportedOperationException(
          "Entry doesn't have a schema. Please attach a schema to '"
              + tableName
              + "' in Data Catalog: "
              + entry.toString());
    }
    Schema schema = SchemaUtils.fromDataCatalog(entry.getSchema());

    Optional<Table.Builder> tableBuilder = tableFactory.tableBuilder(entry);
    if (!tableBuilder.isPresent()) {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Data Catalog entry: %s",
              MoreObjects.toStringHelper(entry)
                  .add("linkedResource", entry.getLinkedResource())
                  .add("hasGcsFilesetSpec", entry.hasGcsFilesetSpec())
                  .toString()));
    }

    return tableBuilder.get().schema(schema).name(tableName).build();
  }

  @Override
  public void close() {
    dataCatalog.close();
  }
}
