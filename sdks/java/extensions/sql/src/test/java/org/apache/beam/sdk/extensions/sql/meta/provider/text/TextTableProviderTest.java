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
package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

/**
 * UnitTest for {@link TextTableProvider}.
 */
public class TextTableProviderTest {
  private TextTableProvider provider = new TextTableProvider();

  @Test
  public void testGetTableType() throws Exception {
    assertEquals("text", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    Table table = mockTable("hello", null);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamTextCSVTable);

    BeamTextCSVTable csvTable = (BeamTextCSVTable) sqlTable;
    assertEquals(CSVFormat.DEFAULT, csvTable.getCsvFormat());
    assertEquals("/home/admin/hello", csvTable.getFilePattern());
  }

  @Test
  public void testBuildBeamSqlTable_customizedFormat() throws Exception {
    Table table = mockTable("hello", "Excel");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamTextCSVTable);

    BeamTextCSVTable csvTable = (BeamTextCSVTable) sqlTable;
    assertEquals(CSVFormat.EXCEL, csvTable.getCsvFormat());
  }

  private static Table mockTable(String name, String format) {
    JSONObject properties = new JSONObject();
    if (format != null) {
      properties.put("format", format);
    }
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("/home/admin/" + name)
        .columns(ImmutableList.of(
            Column.builder()
                .name("id")
                .fieldType(TypeName.INT32.type())
                .primaryKey(true)
                .build(),
            Column.builder()
                .name("name")
                .fieldType(RowSqlTypes.VARCHAR)
            .primaryKey(false)
                .build()))
        .type("text")
        .properties(properties)
        .build();
  }
}
