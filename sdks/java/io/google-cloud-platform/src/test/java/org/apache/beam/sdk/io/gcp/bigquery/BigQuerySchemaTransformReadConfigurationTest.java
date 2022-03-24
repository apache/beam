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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformReadConfiguration}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformReadConfigurationTest {
  private static final String QUERY = "select * from example";

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
  public void testToTypedRead() {
    for (TestCase caze : testCases) {
      testToTypedRead(caze);
    }
  }

  private void testToTypedRead(TestCase caze) {
    BigQueryIO.TypedRead<TableRow> gotRead = caze.input.build().toTypedRead();
    Map<Identifier, Item> gotDisplayData = DisplayData.from(gotRead).asMap();
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
