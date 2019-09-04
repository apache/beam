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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.FullNameTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubJsonTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Uses DataCatalog to get the source type and schema for a table. */
public class DataCatalogTableProvider extends FullNameTableProvider {

  private Map<String, TableProvider> delegateProviders;
  private Map<String, Table> tableCache;
  private DataCatalogClientAdapter dataCatalog;

  private DataCatalogTableProvider(
      Map<String, TableProvider> delegateProviders, DataCatalogClientAdapter dataCatalogClient) {

    this.tableCache = new HashMap<>();
    this.delegateProviders = ImmutableMap.copyOf(delegateProviders);
    this.dataCatalog = dataCatalogClient;
  }

  public static DataCatalogTableProvider create(PipelineOptions pipelineOptions)
      throws IOException {

    DataCatalogPipelineOptions options = pipelineOptions.as(DataCatalogPipelineOptions.class);

    return new DataCatalogTableProvider(
        getSupportedProviders(), getDataCatalogClient(options.getDataCatalogEndpoint()));
  }

  private static Map<String, TableProvider> getSupportedProviders() {
    return Stream.of(
            new PubsubJsonTableProvider(), new BigQueryTableProvider(), new TextTableProvider())
        .collect(toMap(TableProvider::getTableType, p -> p));
  }

  private static DataCatalogClientAdapter getDataCatalogClient(String endpoint) throws IOException {
    return DataCatalogClientAdapter.withDefaultCredentials(endpoint);
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
  public @Nullable Table getTable(String tableNamePart) {
    throw new UnsupportedOperationException(
        "Loading a table by partial name '" + tableNamePart + "' is unsupported");
  }

  @Override
  public @Nullable Table getTableByFullName(TableName fullTableName) {

    ImmutableList<String> allNameParts =
        ImmutableList.<String>builder()
            .addAll(fullTableName.getPath())
            .add(fullTableName.getTableName())
            .build();

    String fullEscapedTableName = ZetaSqlIdUtils.escapeAndJoin(allNameParts);

    return loadTable(fullEscapedTableName);
  }

  private @Nullable Table loadTable(String tableName) {
    if (!tableCache.containsKey(tableName)) {
      tableCache.put(tableName, loadTableFromDC(tableName));
    }

    return tableCache.get(tableName);
  }

  private Table loadTableFromDC(String tableName) {
    try {
      return dataCatalog.getTable(tableName);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.INVALID_ARGUMENT)) {
        return null;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return delegateProviders.get(table.getType()).buildBeamSqlTable(table);
  }
}
