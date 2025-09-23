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

import static org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergTable.TRIGGERING_FREQUENCY_FIELD;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** UnitTest for {@link IcebergTableProvider}. */
public class IcebergTableProviderTest {
  private final IcebergCatalog catalog =
      new IcebergCatalog(
          "test_catalog",
          ImmutableMap.of(
              "catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
              "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
              "warehouse", "gs://bucket/warehouse",
              "beam.catalog.test_catalog.hadoop.fs.gs.project.id", "apache-beam-testing",
              "beam.catalog.test_catalog.hadoop.foo", "bar"));

  @Test
  public void testGetTableType() {
    assertNotNull(catalog.metaStore().getProvider("iceberg"));
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    ImmutableMap<String, Object> properties = ImmutableMap.of(TRIGGERING_FREQUENCY_FIELD, 30);

    ObjectMapper mapper = new ObjectMapper();
    String propertiesString = mapper.writeValueAsString(properties);
    Table table =
        fakeTableBuilder("my_table")
            .properties(TableUtils.parseProperties(propertiesString))
            .build();
    BeamSqlTable sqlTable = catalog.metaStore().buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof IcebergTable);

    IcebergTable icebergTable = (IcebergTable) sqlTable;
    assertEquals("namespace.my_table", icebergTable.tableIdentifier);
    assertEquals(catalog.catalogConfig, icebergTable.catalogConfig);
  }

  private static Table.Builder fakeTableBuilder(String name) {
    return Table.builder()
        .name(name)
        .location("namespace." + name)
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("iceberg");
  }
}
