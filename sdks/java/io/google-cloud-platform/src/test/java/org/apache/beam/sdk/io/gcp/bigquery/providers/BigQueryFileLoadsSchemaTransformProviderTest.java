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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryFileLoadsSchemaTransformProvider.BigQueryFileLoadsSchemaTransform;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQueryFileLoadsSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class BigQueryFileLoadsSchemaTransformProviderTest {

  private static final String PROJECT = "fakeproject";
  private static final String DATASET = "fakedataset";
  private static final String TABLE_ID = "faketable";

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId(TABLE_ID);

  private static final Schema SCHEMA =
      Schema.of(Field.of("name", FieldType.STRING), Field.of("number", FieldType.INT64));

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
    BigQueryFileLoadsSchemaTransformProvider provider =
        new BigQueryFileLoadsSchemaTransformProvider();
    BigQueryWriteConfiguration configuration =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryHelpers.toTableSpec(TABLE_REFERENCE))
            .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE.name())
            .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name())
            .build();
    BigQueryFileLoadsSchemaTransform schemaTransform =
        (BigQueryFileLoadsSchemaTransform) provider.from(configuration);
    schemaTransform.setTestBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);
    PCollectionRowTuple input =
        PCollectionRowTuple.of(tag, p.apply(Create.of(ROWS).withRowSchema(SCHEMA)));
    input.apply(schemaTransform);

    p.run();

    assertNotNull(fakeDatasetService.getTable(TABLE_REFERENCE));
    assertEquals(ROWS.size(), fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE_ID).size());
  }

  @Test
  public void testWriteToPortableDynamicDestinations() throws Exception {
    String destinationTemplate =
        String.format("%s:%s.dynamic_write_{name}_{number}", PROJECT, DATASET);
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(destinationTemplate)
            .setDrop(Collections.singletonList("number"))
            .build();
    BigQueryFileLoadsSchemaTransform write =
        (BigQueryFileLoadsSchemaTransform)
            new BigQueryFileLoadsSchemaTransformProvider().from(config);
    write.setTestBigQueryServices(fakeBigQueryServices);

    PCollection<Row> inputRows = p.apply(Create.of(ROWS)).setRowSchema(SCHEMA);
    PCollectionRowTuple.of("input", inputRows).apply(write);
    p.run().waitUntilFinish();

    RowFilter rowFilter = new RowFilter(SCHEMA).drop(Collections.singletonList("number"));
    assertEquals(
        rowFilter.filter(ROWS.get(0)),
        BigQueryUtils.toBeamRow(
            rowFilter.outputSchema(),
            fakeDatasetService.getAllRows(PROJECT, DATASET, "dynamic_write_a_1").get(0)));
    assertEquals(
        rowFilter.filter(ROWS.get(1)),
        BigQueryUtils.toBeamRow(
            rowFilter.outputSchema(),
            fakeDatasetService.getAllRows(PROJECT, DATASET, "dynamic_write_b_2").get(0)));
    assertEquals(
        rowFilter.filter(ROWS.get(2)),
        BigQueryUtils.toBeamRow(
            rowFilter.outputSchema(),
            fakeDatasetService.getAllRows(PROJECT, DATASET, "dynamic_write_c_3").get(0)));
  }

  @Test
  public void testManagedChoosesFileLoadsForBoundedWrites() {
    PCollection<Row> batchInput = p.apply(Create.of(ROWS)).setRowSchema(SCHEMA);
    batchInput.apply(
        Managed.write(Managed.BIGQUERY)
            .withConfig(ImmutableMap.of("table", "project.dataset.table")));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> writeTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr ->
                    tr.getUniqueName()
                        .contains(BigQueryFileLoadsSchemaTransform.class.getSimpleName()))
            .collect(Collectors.toList());
    assertThat(writeTransformProto.size(), greaterThan(0));
    p.enableAbandonedNodeEnforcement(false);
  }
}
