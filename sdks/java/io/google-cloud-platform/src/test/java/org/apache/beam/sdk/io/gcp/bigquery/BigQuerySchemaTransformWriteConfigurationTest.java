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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.*;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformWriteConfiguration}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformWriteConfigurationTest {
  private final Schema SCHEMA =
      Schema.of(
          Field.of("int64", FieldType.INT64),
          Field.of("bool", FieldType.BOOLEAN),
          Field.of("string", FieldType.STRING));
  private final TableSchema TABLE_SCHEMA = BigQueryUtils.toTableSchema(SCHEMA);

  List<TestCase> testCases =
      Arrays.asList(
          TestCase.of(
              BigQuerySchemaTransformWriteConfiguration.createLoad(
                  "dataset.table",
                  CreateDisposition.CREATE_IF_NEEDED,
                  WriteDisposition.WRITE_EMPTY),
              DisplayData.from(
                  BigQueryIO.writeTableRows()
                      .to("dataset.table")
                      .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                      .withWriteDisposition(WriteDisposition.WRITE_EMPTY)
                      .withSchema(TABLE_SCHEMA))),
          TestCase.of(
              BigQuerySchemaTransformWriteConfiguration.createLoad(
                  "project:dataset.table",
                  CreateDisposition.CREATE_NEVER,
                  WriteDisposition.WRITE_APPEND),
              DisplayData.from(
                  BigQueryIO.writeTableRows()
                      .to("project:dataset.table")
                      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                      .withSchema(TABLE_SCHEMA))),
          TestCase.of(
              BigQuerySchemaTransformWriteConfiguration.createLoad(
                  new TableReference()
                      .setProjectId("project")
                      .setDatasetId("dataset")
                      .setTableId("table"),
                  CreateDisposition.CREATE_IF_NEEDED,
                  WriteDisposition.WRITE_TRUNCATE),
              DisplayData.from(
                  BigQueryIO.writeTableRows()
                      .to("project:dataset.table")
                      .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                      .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                      .withSchema(TABLE_SCHEMA))));

  static class TestCase {
    BigQuerySchemaTransformWriteConfiguration input;
    DisplayData want;

    static TestCase of(BigQuerySchemaTransformWriteConfiguration input, DisplayData want) {
      TestCase caze = new TestCase();
      caze.input = input;
      caze.want = want;
      return caze;
    }
  }

  @Test
  public void testToWrite() {
    for (TestCase caze : testCases) {
      testToWrite(caze);
    }
  }

  private void testToWrite(TestCase caze) {
    BigQueryIO.Write<TableRow> gotWrite = caze.input.toWrite(SCHEMA);
    Map<Identifier, Item> gotDisplayData = DisplayData.from(gotWrite).asMap();
    Map<Identifier, Item> wantDisplayData = caze.want.asMap();
    Set<Identifier> keys = new HashSet<>();
    keys.addAll(gotDisplayData.keySet());
    keys.addAll(wantDisplayData.keySet());
    for (Identifier key : keys) {
      Item got = null;
      Item want = null;
      if (gotDisplayData.containsKey(key)) {
        got = gotDisplayData.get(key);
      }
      if (wantDisplayData.containsKey(key)) {
        want = wantDisplayData.get(key);
      }
      assertEquals(want, got);
    }
  }
}
