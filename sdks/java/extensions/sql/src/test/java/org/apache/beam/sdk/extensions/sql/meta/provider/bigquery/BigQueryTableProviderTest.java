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

import static org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable.METHOD_PROPERTY;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable.WRITE_DISPOSITION_PROPERTY;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSON;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

/** UnitTest for {@link BigQueryTableProvider}. */
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
    assertTrue(sqlTable instanceof BigQueryTable);

    BigQueryTable bqTable = (BigQueryTable) sqlTable;
    assertEquals("project:dataset.table", bqTable.bqLocation);
  }

  @Test
  public void testDefaultMethod_whenPropertiesAreNotSet() {
    Table table = fakeTable("hello");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(Method.DIRECT_READ, sqlTable.method);
  }

  @Test
  public void testSelectDefaultMethodExplicitly() {
    Table table =
        fakeTableWithProperties(
            "hello", "{ " + METHOD_PROPERTY + ": " + "\"" + Method.DEFAULT.toString() + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(Method.DEFAULT, sqlTable.method);
  }

  @Test
  public void testSelectDirectReadMethod() {
    Table table =
        fakeTableWithProperties(
            "hello", "{ " + METHOD_PROPERTY + ": " + "\"" + Method.DIRECT_READ.toString() + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(Method.DIRECT_READ, sqlTable.method);
  }

  @Test
  public void testSelectExportMethod() {
    Table table =
        fakeTableWithProperties(
            "hello", "{ " + METHOD_PROPERTY + ": " + "\"" + Method.EXPORT.toString() + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(Method.EXPORT, sqlTable.method);
  }

  @Test
  public void testSelectWriteDispositionMethodTruncate() {
    Table table =
        fakeTableWithProperties(
            "hello",
            "{ "
                + WRITE_DISPOSITION_PROPERTY
                + ": "
                + "\""
                + WriteDisposition.WRITE_TRUNCATE.toString()
                + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(WriteDisposition.WRITE_TRUNCATE, sqlTable.writeDisposition);
  }

  @Test
  public void testSelectWriteDispositionMethodAppend() {
    Table table =
        fakeTableWithProperties(
            "hello",
            "{ "
                + WRITE_DISPOSITION_PROPERTY
                + ": "
                + "\""
                + WriteDisposition.WRITE_APPEND.toString()
                + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(WriteDisposition.WRITE_APPEND, sqlTable.writeDisposition);
  }

  @Test
  public void testSelectWriteDispositionMethodEmpty() {
    Table table =
        fakeTableWithProperties(
            "hello",
            "{ "
                + WRITE_DISPOSITION_PROPERTY
                + ": "
                + "\""
                + WriteDisposition.WRITE_EMPTY.toString()
                + "\" }");
    BigQueryTable sqlTable = (BigQueryTable) provider.buildBeamSqlTable(table);

    assertEquals(WriteDisposition.WRITE_EMPTY, sqlTable.writeDisposition);
  }

  @Test
  public void testRuntimeExceptionThrown_whenAnInvalidPropertyIsSpecified() {
    Table table = fakeTableWithProperties("hello", "{ " + METHOD_PROPERTY + ": \"blahblah\" }");

    assertThrows(
        RuntimeException.class,
        () -> {
          provider.buildBeamSqlTable(table);
        });
  }

  @Test
  public void testRuntimeExceptionThrown_whenAPropertyOfInvalidTypeIsSpecified() {
    Table table = fakeTableWithProperties("hello", "{ " + METHOD_PROPERTY + ": 1337 }");

    assertThrows(
        RuntimeException.class,
        () -> {
          provider.buildBeamSqlTable(table);
        });
  }

  private static Table fakeTable(String name) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("project:dataset.table")
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("bigquery")
        .build();
  }

  private static Table fakeTableWithProperties(String name, String properties) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("project:dataset.table")
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("bigquery")
        .properties(JSON.parseObject(properties))
        .build();
  }
}
