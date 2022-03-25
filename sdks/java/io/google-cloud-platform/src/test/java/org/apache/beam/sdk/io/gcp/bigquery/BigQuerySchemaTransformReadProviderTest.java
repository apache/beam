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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformReadProvider.BigQuerySchemaTransformReadTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
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

/** Test for {@link BigQuerySchemaTransformReadProvider}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformReadProviderTest {
  private static final String QUERY = "select * from example";
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigQuerySchemaTransformReadConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigQuerySchemaTransformReadConfiguration.class);
  private static final SerializableFunction<BigQuerySchemaTransformReadConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  List<TestCase> testCases =
      Arrays.asList(
          TestCase.of(
              BigQuerySchemaTransformReadConfiguration.createQueryBuilder(QUERY),
              DisplayData.from(
                  BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY).usingStandardSql())),
          TestCase.of(
              BigQuerySchemaTransformReadConfiguration.createQueryBuilder(QUERY)
                  .setQueryLocation("some-location"),
              DisplayData.from(
                  BigQueryIO.readTableRowsWithSchema()
                      .fromQuery(QUERY)
                      .usingStandardSql()
                      .withQueryLocation("some-location"))),
          TestCase.of(
              BigQuerySchemaTransformReadConfiguration.createQueryBuilder(QUERY)
                  .setUseStandardSql(false),
              DisplayData.from(BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY))),
          TestCase.of(
              BigQuerySchemaTransformReadConfiguration.createExtractBuilder(
                  "project:dataset.table"),
              DisplayData.from(BigQueryIO.readTableRowsWithSchema().from("project:dataset.table"))),
          TestCase.of(
              BigQuerySchemaTransformReadConfiguration.createExtractBuilder(
                  new TableReference()
                      .setProjectId("project")
                      .setDatasetId("dataset")
                      .setTableId("table")),
              DisplayData.from(
                  BigQueryIO.readTableRowsWithSchema().from("project:dataset.table"))));

  static class TestCase {
    BigQuerySchemaTransformReadConfiguration.Builder input;
    DisplayData want;

    static TestCase of(BigQuerySchemaTransformReadConfiguration.Builder input, DisplayData want) {
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
    Row input = ROW_SERIALIZABLE_FUNCTION.apply(caze.input.build());
    SchemaTransformProvider provider = new BigQuerySchemaTransformReadProvider();
    BigQuerySchemaTransformReadTransform transform =
        (BigQuerySchemaTransformReadTransform) provider.from(input).buildTransform();

    Map<Identifier, Item> gotDisplayData = DisplayData.from(transform.toTypedRead()).asMap();
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
