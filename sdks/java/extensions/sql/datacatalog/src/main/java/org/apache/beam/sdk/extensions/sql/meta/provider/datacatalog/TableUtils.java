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

import com.google.cloud.datacatalog.Entry;
import java.net.URI;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;

/** Common utilities to create Beam SQL tables from Data Catalog schemas. */
class TableUtils {

  interface TableFactory {
    Table.Builder tableBuilder(Entry entry);
  }

  private static final Map<String, TableFactory> TABLE_FACTORIES =
      ImmutableMap.<String, TableFactory>builder()
          .put("bigquery.googleapis.com", BigQueryUtils::tableBuilder)
          .put("pubsub.googleapis.com", PubsubUtils::tableBuilder)
          .build();

  static Table toBeamTable(String tableName, Entry entry) {
    if (entry.getSchema().getColumnsCount() == 0) {
      throw new UnsupportedOperationException(
          "Entry doesn't have a schema. Please attach a schema to '"
              + tableName
              + "' in Data Catalog: "
              + entry.toString());
    }
    Schema schema = SchemaUtils.fromDataCatalog(entry.getSchema());

    String service = URI.create(entry.getLinkedResource()).getAuthority().toLowerCase();

    Table.Builder table = null;
    if (TABLE_FACTORIES.containsKey(service)) {
      table = TABLE_FACTORIES.get(service).tableBuilder(entry);
    }

    if (GcsUtils.isGcs(entry)) {
      table = GcsUtils.tableBuilder(entry);
    }

    if (table != null) {
      return table.schema(schema).name(tableName).build();
    }

    throw new UnsupportedOperationException(
        "Unsupported SQL source kind: " + entry.getLinkedResource());
  }
}
