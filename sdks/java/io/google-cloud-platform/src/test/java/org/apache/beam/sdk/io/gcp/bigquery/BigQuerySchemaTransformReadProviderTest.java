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

import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformReadProvider.PCollectionRowTupleTransform;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformReadProvider}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformReadProviderTest {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigQuerySchemaTransformReadConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigQuerySchemaTransformReadConfiguration.class);
  private static final SerializableFunction<BigQuerySchemaTransformReadConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  private static final String PROJECT = "fakeproject";
  private static final String DATASET = "fakedataset";
  private static final String TABLE_ID = "faketable";

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
  public void setUp() throws IOException, InterruptedException {
    FakeDatasetService.setUp();
    FakeJobService.setUp();
    BigQueryIO.clearCreatedTables();
    fakeTable.setSchema(TABLE_SCHEMA);
    fakeTable.setTableReference(TABLE_REFERENCE);
    fakeTable.setNumBytes(1024L * 1024L);
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
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

  @Rule public transient TestPipeline p = TestPipeline.fromOptions(OPTIONS);

  @Test
  public void testExtract() {
    SchemaTransformProvider provider = new BigQuerySchemaTransformReadProvider();
    BigQuerySchemaTransformReadConfiguration configuration =
        BigQuerySchemaTransformReadConfiguration.createExtractBuilder(TABLE_SPEC).build();
    Row configurationRow = ROW_SERIALIZABLE_FUNCTION.apply(configuration);
    SchemaTransform schemaTransform = provider.from(configurationRow);
    PCollectionRowTupleTransform pCollectionRowTupleTransform =
        (PCollectionRowTupleTransform) schemaTransform.buildTransform();

    pCollectionRowTupleTransform.setTestBigQueryServices(fakeBigQueryServices);
    PCollectionRowTuple input = PCollectionRowTuple.empty(p);
    String tag = provider.outputCollectionNames().get(0);
    PCollectionRowTuple output = input.apply(pCollectionRowTupleTransform);
    assertTrue(output.has(tag));
    PCollection<Row> got = output.get(tag);
    PAssert.that(got).containsInAnyOrder(ROWS);

    p.run();
  }
}
