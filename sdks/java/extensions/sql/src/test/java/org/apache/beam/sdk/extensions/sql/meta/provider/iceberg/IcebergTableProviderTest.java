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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergTable.CATALOG_NAME_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergTable.CATALOG_PROPERTIES_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergTable.HADOOP_CONFIG_PROPERTIES_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergTable.TRIGGERING_FREQUENCY_FIELD;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** UnitTest for {@link IcebergTableProvider}. */
public class IcebergTableProviderTest {
  private final IcebergTableProvider provider =
      IcebergTableProvider.create()
          .withCatalogProperties(
              ImmutableMap.of(
                  "catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                  "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
                  "warehouse", "gs://bucket/warehouse"))
          .withHadoopConfProperties(
              ImmutableMap.of(
                  "fs.gs.project.id", "apache-beam-testing",
                  "foo", "bar"))
          .withCatalogName("my_catalog");

  @Test
  public void testGetTableType() {
    assertEquals("iceberg", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    ImmutableMap<String, Object> properties = ImmutableMap.of(TRIGGERING_FREQUENCY_FIELD, 30);

    ObjectMapper mapper = new ObjectMapper();
    String propertiesString = mapper.writeValueAsString(properties);
    Table table = fakeTableWithProperties("my_table", propertiesString);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof IcebergTable);

    IcebergTable icebergTable = (IcebergTable) sqlTable;
    IcebergCatalogConfig catalogConfig = icebergTable.catalogConfig;
    assertEquals("namespace.table", icebergTable.tableIdentifier);
    assertEquals(properties.get(CATALOG_NAME_FIELD), catalogConfig.getCatalogName());
    assertEquals(properties.get(CATALOG_PROPERTIES_FIELD), catalogConfig.getCatalogProperties());
    assertEquals(
        properties.get(HADOOP_CONFIG_PROPERTIES_FIELD), catalogConfig.getConfigProperties());
    assertEquals(properties.get(TRIGGERING_FREQUENCY_FIELD), icebergTable.triggeringFrequency);
  }

  private static Table fakeTableWithProperties(String name, String properties) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("namespace.table")
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("iceberg")
        .properties(TableUtils.parseProperties(properties))
        .build();
  }
}
