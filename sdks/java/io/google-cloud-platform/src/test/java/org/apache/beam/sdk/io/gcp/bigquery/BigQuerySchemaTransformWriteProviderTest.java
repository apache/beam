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

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformWriteProvider.BigQuerySchemaTransformWriteTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformWriteProvider}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformWriteProviderTest {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigQuerySchemaTransformWriteConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigQuerySchemaTransformWriteConfiguration.class);
  private static final SerializableFunction<BigQuerySchemaTransformWriteConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  private final Schema FAKE_SCHEMA =
      Schema.of(
          Field.of("int64", FieldType.INT64),
          Field.of("bool", FieldType.BOOLEAN),
          Field.of("string", FieldType.STRING));

  private final TableSchema FAKE_TABLE_SCHEMA = BigQueryUtils.toTableSchema(FAKE_SCHEMA);

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
                      .withSchema(FAKE_TABLE_SCHEMA))),
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
                      .withSchema(FAKE_TABLE_SCHEMA))),
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
                      .withSchema(FAKE_TABLE_SCHEMA))));

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
  public void testFrom() {
    for (TestCase caze : testCases) {
      testFrom(caze);
    }
  }

  private void testFrom(TestCase caze) {
    Row input = ROW_SERIALIZABLE_FUNCTION.apply(caze.input);
    SchemaTransformProvider schemaTransformProvider = new BigQuerySchemaTransformWriteProvider();
    SchemaTransform schemaTransform = schemaTransformProvider.from(input);
    BigQuerySchemaTransformWriteTransform schemaTransformWrite =
        (BigQuerySchemaTransformWriteTransform) schemaTransform.buildTransform();
    Map<Identifier, Item> gotDisplayData =
        DisplayData.from(schemaTransformWrite.toWrite(FAKE_SCHEMA)).asMap();
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
