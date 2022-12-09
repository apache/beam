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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformWriteProvider.INPUT_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformWriteProvider.PCollectionRowTupleTransform;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformWriteProvider}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformWriteProviderTest {

  private static final String PROJECT = "fakeproject";
  private static final String DATASET = "fakedataset";
  private static final String TABLE_ID = "faketable";

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId(TABLE_ID);

  private static final Schema SCHEMA =
      Schema.of(Field.of("name", FieldType.STRING), Field.of("number", FieldType.INT64));

  private static final TableSchema TABLE_SCHEMA = BigQueryUtils.toTableSchema(SCHEMA);

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA).withFieldValue("name", "a").withFieldValue("number", 1L).build(),
          Row.withSchema(SCHEMA).withFieldValue("name", "b").withFieldValue("number", 2L).build(),
          Row.withSchema(SCHEMA).withFieldValue("name", "c").withFieldValue("number", 3L).build());

  private static final BigQueryOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
  private final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private final FakeJobService fakeJobService = new FakeJobService();
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withJobService(fakeJobService)
          .withDatasetService(fakeDatasetService);

  @Before
  public void setUp() throws IOException, InterruptedException {
    FakeDatasetService.setUp();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
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
  public void testLoad() throws IOException, InterruptedException {
    SchemaTransformProvider provider = new BigQuerySchemaTransformWriteProvider();
    BigQuerySchemaTransformWriteConfiguration configuration =
        BigQuerySchemaTransformWriteConfiguration.builder()
            .setTableSpec(BigQueryHelpers.toTableSpec(TABLE_REFERENCE))
            .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE.name())
            .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name())
            .build();
    Row configurationRow = configuration.toBeamRow();
    SchemaTransform schemaTransform = provider.from(configurationRow);
    PCollectionRowTupleTransform pCollectionRowTupleTransform =
        (PCollectionRowTupleTransform) schemaTransform.buildTransform();
    pCollectionRowTupleTransform.setTestBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);
    PCollectionRowTuple input =
        PCollectionRowTuple.of(tag, p.apply(Create.of(ROWS).withRowSchema(SCHEMA)));
    input.apply(pCollectionRowTupleTransform);

    p.run();

    assertNotNull(fakeDatasetService.getTable(TABLE_REFERENCE));
    assertEquals(ROWS.size(), fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE_ID).size());
  }

  @Test
  public void testValidatePipelineOptions() {
    List<Pair<BigQuerySchemaTransformWriteConfiguration.Builder, Class<? extends Exception>>>
        cases =
            Arrays.asList(
                Pair.of(
                    BigQuerySchemaTransformWriteConfiguration.builder()
                        .setTableSpec("project.doesnot.exist")
                        .setCreateDisposition(CreateDisposition.CREATE_NEVER.name())
                        .setWriteDisposition(WriteDisposition.WRITE_APPEND.name()),
                    InvalidConfigurationException.class),
                Pair.of(
                    BigQuerySchemaTransformWriteConfiguration.builder()
                        .setTableSpec(String.format("%s.%s.%s", PROJECT, DATASET, "doesnotexist"))
                        .setCreateDisposition(CreateDisposition.CREATE_NEVER.name())
                        .setWriteDisposition(WriteDisposition.WRITE_EMPTY.name()),
                    InvalidConfigurationException.class),
                Pair.of(
                    BigQuerySchemaTransformWriteConfiguration.builder()
                        .setTableSpec("project.doesnot.exist")
                        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name())
                        .setWriteDisposition(WriteDisposition.WRITE_APPEND.name()),
                    null));
    for (Pair<BigQuerySchemaTransformWriteConfiguration.Builder, Class<? extends Exception>> caze :
        cases) {
      PCollectionRowTupleTransform transform = transformFrom(caze.getLeft().build());
      if (caze.getRight() != null) {
        assertThrows(caze.getRight(), () -> transform.validate(p.getOptions()));
      } else {
        transform.validate(p.getOptions());
      }
    }
  }

  @Test
  public void testToWrite() {
    List<Pair<BigQuerySchemaTransformWriteConfiguration.Builder, BigQueryIO.Write<TableRow>>>
        cases =
            Arrays.asList(
                Pair.of(
                    BigQuerySchemaTransformWriteConfiguration.builder()
                        .setTableSpec(BigQueryHelpers.toTableSpec(TABLE_REFERENCE))
                        .setCreateDisposition(CreateDisposition.CREATE_NEVER.name())
                        .setWriteDisposition(WriteDisposition.WRITE_EMPTY.name()),
                    BigQueryIO.writeTableRows()
                        .to(TABLE_REFERENCE)
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_EMPTY)
                        .withSchema(TABLE_SCHEMA)),
                Pair.of(
                    BigQuerySchemaTransformWriteConfiguration.builder()
                        .setTableSpec(BigQueryHelpers.toTableSpec(TABLE_REFERENCE))
                        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name())
                        .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE.name()),
                    BigQueryIO.writeTableRows()
                        .to(TABLE_REFERENCE)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                        .withSchema(TABLE_SCHEMA)));
    for (Pair<BigQuerySchemaTransformWriteConfiguration.Builder, BigQueryIO.Write<TableRow>> caze :
        cases) {
      PCollectionRowTupleTransform transform = transformFrom(caze.getLeft().build());
      Map<Identifier, Item> gotDisplayData = DisplayData.from(transform.toWrite(SCHEMA)).asMap();
      Map<Identifier, Item> wantDisplayData = DisplayData.from(caze.getRight()).asMap();
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

  @Test
  public void validatePCollectionRowTupleInput() {
    PCollectionRowTuple empty = PCollectionRowTuple.empty(p);
    PCollectionRowTuple valid =
        PCollectionRowTuple.of(
            INPUT_TAG, p.apply("CreateRowsWithValidSchema", Create.of(ROWS)).setRowSchema(SCHEMA));

    PCollectionRowTuple invalid =
        PCollectionRowTuple.of(
            INPUT_TAG,
            p.apply(
                "CreateRowsWithInvalidSchema",
                Create.of(
                    Row.nullRow(
                        Schema.builder().addNullableField("name", FieldType.STRING).build()))));

    PCollectionRowTupleTransform transform =
        transformFrom(
            BigQuerySchemaTransformWriteConfiguration.builder()
                .setTableSpec(BigQueryHelpers.toTableSpec(TABLE_REFERENCE))
                .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name())
                .setWriteDisposition(WriteDisposition.WRITE_APPEND.name())
                .build());

    assertThrows(IllegalArgumentException.class, () -> transform.validate(empty));

    assertThrows(IllegalStateException.class, () -> transform.validate(invalid));

    transform.validate(valid);

    p.run();
  }

  private PCollectionRowTupleTransform transformFrom(
      BigQuerySchemaTransformWriteConfiguration configuration) {
    SchemaTransformProvider provider = new BigQuerySchemaTransformWriteProvider();
    PCollectionRowTupleTransform transform =
        (PCollectionRowTupleTransform) provider.from(configuration.toBeamRow()).buildTransform();

    transform.setTestBigQueryServices(fakeBigQueryServices);

    return transform;
  }
}
