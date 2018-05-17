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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.junit.Test;

/**
 * UnitTest for {@link BigQueryTableProvider}.
 */
public class BigQueryTableProviderTest {
  private BigQueryTableProvider provider = new BigQueryTableProvider();

  @Test
  public void testGetTableType() throws Exception {
    assertEquals("bigquery", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    Table table = fakeTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamBigQueryTable);

    BeamBigQueryTable bqTable = (BeamBigQueryTable) sqlTable;
    assertEquals("project:dataset.table", bqTable.getTableSpec());
  }

  private static Table fakeTable(String name) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("project:dataset.table")
        .schema(
            Stream.of(
                Schema.Field.of("id", TypeName.INT32.type()).withNullable(true),
                Schema.Field.of("name", RowSqlTypes.VARCHAR).withNullable(true))
                  .collect(toSchema()))
        .type("bigquery")
        .build();
  }
}
