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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryExportReadSchemaTransformProvider.BigQueryExportSchemaTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQueryExportReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class BigQueryExportReadSchemaTransformProviderTest {
  private static final String PROJECT = "fakeproject";
  private static final String DATASET = "fakedataset";
  private static final String TABLE_ID = "faketable";

  private static final String QUERY = "select * from `fakeproject.fakedataset.faketable`";
  private static final String LOCATION = "kingdom-of-figaro";

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId(TABLE_ID);

  private static final String TABLE_SPEC = BigQueryHelpers.toTableSpec(TABLE_REFERENCE);

  private static final Schema SCHEMA =
      Schema.of(Field.of("name", FieldType.STRING), Field.of("number", FieldType.INT64));

  private static final List<TableRow> RECORDS =
      Arrays.asList(
          new TableRow().set("name", "a").set("number", 1L),
          new TableRow().set("name", "b").set("number", 2L),
          new TableRow().set("name", "c").set("number", 3L));

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA).withFieldValue("name", "a").withFieldValue("number", 1L).build(),
          Row.withSchema(SCHEMA).withFieldValue("name", "b").withFieldValue("number", 2L).build(),
          Row.withSchema(SCHEMA).withFieldValue("name", "c").withFieldValue("number", 3L).build());

  private static final TableSchema TABLE_SCHEMA = BigQueryUtils.toTableSchema(SCHEMA);
  private static final BigQueryOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
  private final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private final FakeJobService fakeJobService = new FakeJobService();
  private final Table fakeTable = new Table();
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withJobService(fakeJobService)
          .withDatasetService(fakeDatasetService);

  @Before
  public void setUp() throws IOException, InterruptedException, ExecutionException {
    FakeDatasetService.setUp();
    FakeJobService.setUp();
    BigQueryIO.clearStaticCaches();
    fakeTable.setSchema(TABLE_SCHEMA);
    fakeTable.setTableReference(TABLE_REFERENCE);
    fakeDatasetService.createDataset(PROJECT, DATASET, LOCATION, "", null);
    fakeDatasetService.createTable(fakeTable);
    fakeDatasetService.insertAll(fakeTable.getTableReference(), RECORDS, null);
    temporaryFolder.create();
    OPTIONS.setProject(PROJECT);
    OPTIONS.setTempLocation(temporaryFolder.getRoot().getAbsolutePath());
  }

  @After
  public void tearDown() {
    temporaryFolder.delete();
  }

  @Rule
  public transient TestPipeline p =
      TestPipeline.fromOptions(OPTIONS).enableAbandonedNodeEnforcement(false);

  @Test
  public void testQuery() {
    // Previous attempts using FakeBigQueryServices with a Read configuration using a query failed.
    // For now, we test using DisplayData and the toTypedRead method.
    List<Pair<BigQueryExportReadSchemaTransformConfiguration.Builder, TypedRead<TableRow>>> cases =
        Arrays.asList(
            Pair.of(
                BigQueryExportReadSchemaTransformConfiguration.builder().setQuery(QUERY),
                BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY)),
            Pair.of(
                BigQueryExportReadSchemaTransformConfiguration.builder()
                    .setQuery(QUERY)
                    .setQueryLocation(LOCATION),
                BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY).withQueryLocation(LOCATION)),
            Pair.of(
                BigQueryExportReadSchemaTransformConfiguration.builder()
                    .setQuery(QUERY)
                    .setUseStandardSql(true),
                BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY).usingStandardSql()),
            Pair.of(
                BigQueryExportReadSchemaTransformConfiguration.builder()
                    .setQuery(QUERY)
                    .setUseStandardSql(false),
                BigQueryIO.readTableRowsWithSchema().fromQuery(QUERY)));

    for (Pair<BigQueryExportReadSchemaTransformConfiguration.Builder, TypedRead<TableRow>> caze :
        cases) {
      Map<Identifier, Item> want = DisplayData.from(caze.getRight()).asMap();
      BigQueryExportReadSchemaTransformProvider provider =
          new BigQueryExportReadSchemaTransformProvider();
      BigQueryExportReadSchemaTransformConfiguration configuration = caze.getLeft().build();
      BigQueryExportSchemaTransform schemaTransform =
          (BigQueryExportSchemaTransform) provider.from(configuration);
      Map<Identifier, Item> got = DisplayData.from(schemaTransform.toTypedRead()).asMap();
      assertEquals(want, got);
    }
  }

  @Test
  public void testExtract() {
    BigQueryExportReadSchemaTransformProvider provider =
        new BigQueryExportReadSchemaTransformProvider();
    BigQueryExportReadSchemaTransformConfiguration configuration =
        BigQueryExportReadSchemaTransformConfiguration.builder().setTableSpec(TABLE_SPEC).build();
    BigQueryExportSchemaTransform schemaTransform =
        (BigQueryExportSchemaTransform) provider.from(configuration);

    schemaTransform.setTestBigQueryServices(fakeBigQueryServices);
    PCollectionRowTuple input = PCollectionRowTuple.empty(p);
    String tag = provider.outputCollectionNames().get(0);
    PCollectionRowTuple output = input.apply(schemaTransform);
    assertTrue(output.has(tag));
    PCollection<Row> got = output.get(tag);
    PAssert.that(got).containsInAnyOrder(ROWS);

    p.run();
  }

  @Test
  public void testInvalidConfiguration() {
    BigQueryExportReadSchemaTransformProvider provider =
        new BigQueryExportReadSchemaTransformProvider();
    for (Pair<
            BigQueryExportReadSchemaTransformConfiguration.Builder,
            ? extends Class<? extends RuntimeException>>
        caze :
            Arrays.asList(
                Pair.of(
                    BigQueryExportReadSchemaTransformConfiguration.builder(),
                    IllegalArgumentException.class),
                Pair.of(
                    BigQueryExportReadSchemaTransformConfiguration.builder()
                        .setQuery(QUERY)
                        .setTableSpec(TABLE_SPEC),
                    IllegalStateException.class),
                Pair.of(
                    BigQueryExportReadSchemaTransformConfiguration.builder()
                        .setQueryLocation(LOCATION),
                    IllegalArgumentException.class),
                Pair.of(
                    BigQueryExportReadSchemaTransformConfiguration.builder()
                        .setUseStandardSql(true),
                    IllegalArgumentException.class))) {
      BigQueryExportSchemaTransform schemaTransform =
          (BigQueryExportSchemaTransform) provider.from(caze.getLeft().build());
      schemaTransform.setTestBigQueryServices(fakeBigQueryServices);
      PCollectionRowTuple empty = PCollectionRowTuple.empty(p);
      assertThrows(caze.getRight(), () -> empty.apply(schemaTransform));
    }
  }

  @Test
  public void testInvalidInput() {
    BigQueryExportReadSchemaTransformProvider provider =
        new BigQueryExportReadSchemaTransformProvider();
    BigQueryExportReadSchemaTransformConfiguration configuration =
        BigQueryExportReadSchemaTransformConfiguration.builder().setTableSpec(TABLE_SPEC).build();
    BigQueryExportSchemaTransform schemaTransform =
        (BigQueryExportSchemaTransform) provider.from(configuration);

    schemaTransform.setTestBigQueryServices(fakeBigQueryServices);
    PCollectionRowTuple input = PCollectionRowTuple.of("badinput", p.apply(Create.of(ROWS)));
    assertThrows(IllegalArgumentException.class, () -> input.apply(schemaTransform));
  }

  private void assertEquals(Map<Identifier, Item> want, Map<Identifier, Item> got) {
    Set<Identifier> keys = new HashSet<>();
    keys.addAll(want.keySet());
    keys.addAll(got.keySet());
    for (Identifier key : keys) {
      Item wantItem = null;
      Item gotItem = null;
      if (want.containsKey(key)) {
        wantItem = want.get(key);
      }
      if (got.containsKey(key)) {
        gotItem = got.get(key);
      }
      Assert.assertEquals(wantItem, gotItem);
    }
  }
}
