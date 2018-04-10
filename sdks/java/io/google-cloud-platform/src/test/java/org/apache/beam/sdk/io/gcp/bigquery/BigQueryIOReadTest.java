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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics4;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Streamingbuffer;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link BigQueryIO#read}. */
@RunWith(JUnit4.class)
public class BigQueryIOReadTest implements Serializable {
  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline p;

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          // We need to set up the temporary folder, and then set up the TestPipeline based on the
          // chosen folder. Unfortunately, since rule evaluation order is unspecified and unrelated
          // to field order, and is separate from construction, that requires manually creating this
          // TestRule.
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  options.as(BigQueryOptions.class).setProject("project-id");
                  options
                      .as(BigQueryOptions.class)
                      .setTempLocation(testFolder.getRoot().getAbsolutePath());
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  private void checkReadTableObject(
      BigQueryIO.Read read, String project, String dataset, String table) {
    checkReadTableObjectWithValidate(read, project, dataset, table, true);
  }

  private void checkReadQueryObject(BigQueryIO.Read read, String query) {
    checkReadQueryObjectWithValidate(read, query, true);
  }

  private void checkReadTableObjectWithValidate(
      BigQueryIO.Read read, String project, String dataset, String table, boolean validate) {
    assertEquals(project, read.getTable().getProjectId());
    assertEquals(dataset, read.getTable().getDatasetId());
    assertEquals(table, read.getTable().getTableId());
    assertNull(read.getQuery());
    assertEquals(validate, read.getValidate());
  }

  private void checkReadQueryObjectWithValidate(
      BigQueryIO.Read read, String query, boolean validate) {
    assertNull(read.getTable());
    assertEquals(query, read.getQuery().get());
    assertEquals(validate, read.getValidate());
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    FakeDatasetService.setUp();
    BigQueryIO.clearCreatedTables();
    fakeDatasetService.createDataset("project-id", "dataset-id", "", "", null);
  }

  @Test
  public void testBuildTableBasedSource() {
    BigQueryIO.Read read = BigQueryIO.read().from("foo.com:project:somedataset.sometable");
    checkReadTableObject(read, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testBuildQueryBasedSource() {
    BigQueryIO.Read read = BigQueryIO.read().fromQuery("foo_query");
    checkReadQueryObject(read, "foo_query");
  }

  @Test
  public void testBuildTableBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read read =
        BigQueryIO.read().from("foo.com:project:somedataset.sometable").withoutValidation();
    checkReadTableObjectWithValidate(read, "foo.com:project", "somedataset", "sometable", false);

  }

  @Test
  public void testBuildQueryBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read read =
        BigQueryIO.read().fromQuery("some_query").withoutValidation();
    checkReadQueryObjectWithValidate(read, "some_query", false);
  }

  @Test
  public void testBuildTableBasedSourceWithDefaultProject() {
    BigQueryIO.Read read =
        BigQueryIO.read().from("somedataset.sometable");
    checkReadTableObject(read, null, "somedataset", "sometable");
  }

  @Test
  public void testBuildSourceWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Read read = BigQueryIO.read().from(table);
    checkReadTableObject(read, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testValidateReadSetsDefaultProject() throws Exception {
    String tableId = "sometable";
    TableReference tableReference =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId(tableId);
    fakeDatasetService.createTable(new Table()
        .setTableReference(tableReference)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(fakeDatasetService);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));
    fakeDatasetService.insertAll(tableReference, expected, null);

    TableReference tableRef = new TableReference().setDatasetId("dataset-id").setTableId(tableId);

    PCollection<KV<String, Long>> output =
        p.apply(BigQueryIO.read().from(tableRef).withTestServices(fakeBqServices))
            .apply(ParDo.of(new DoFn<TableRow, KV<String, Long>>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                c.output(KV.of((String) c.element().get("name"),
                    Long.valueOf((String) c.element().get("number"))));
              }
            }));
    PAssert.that(output).containsInAnyOrder(ImmutableList.of(KV.of("a", 1L), KV.of("b", 2L),
        KV.of("c", 3L), KV.of("d", 4L), KV.of("e", 5L), KV.of("f", 6L)));
    p.run();
  }

  @Test
  public void testBuildSourceWithTableAndFlatten() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a result flattening preference,"
            + " which only applies to queries");
    p.apply("ReadMyTable",
        BigQueryIO.read()
            .from("foo.com:project:somedataset.sometable")
            .withoutResultFlattening());
    p.run();
  }

  @Test
  public void testBuildSourceWithTableAndFlattenWithoutValidation() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a result flattening preference,"
            + " which only applies to queries");
    p.apply(
        BigQueryIO.read()
            .from("foo.com:project:somedataset.sometable")
            .withoutValidation()
            .withoutResultFlattening());
    p.run();
  }

  @Test
  public void testBuildSourceWithTableAndSqlDialect() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a SQL dialect preference,"
            + " which only applies to queries");
    p.apply(
        BigQueryIO.read()
            .from("foo.com:project:somedataset.sometable")
            .usingStandardSql());
    p.run();
  }

  @Test
  public void testReadFromTableWithoutTemplateCompatibility()
      throws IOException, InterruptedException {
    testReadFromTable(false, false);
  }

  @Test
  public void testReadFromTableWithTemplateCompatibility()
      throws IOException, InterruptedException {
    testReadFromTable(true, false);
  }

  @Test
  public void testReadTableRowsFromTableWithoutTemplateCompatibility()
      throws IOException, InterruptedException {
    testReadFromTable(false, true);
  }

  @Test
  public void testReadTableRowsFromTableWithTemplateCompatibility()
      throws IOException, InterruptedException {
    testReadFromTable(true, true);
  }

  private void testReadFromTable(boolean useTemplateCompatibility, boolean useReadTableRows)
      throws IOException, InterruptedException {
    Table sometable = new Table();
    sometable.setSchema(
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER"))));
    sometable.setTableReference(
        new TableReference()
            .setProjectId("non-executing-project")
            .setDatasetId("somedataset")
            .setTableId("sometable"));
    sometable.setNumBytes(1024L * 1024L);
    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset("non-executing-project", "somedataset", "", "", null);
    fakeDatasetService.createTable(sometable);

    List<TableRow> records = Lists.newArrayList(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L));
    fakeDatasetService.insertAll(sometable.getTableReference(), records, null);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(fakeDatasetService);

    PTransform<PBegin, PCollection<TableRow>> readTransform;
    if (useReadTableRows) {
      BigQueryIO.Read read =
          BigQueryIO.read()
              .from("non-executing-project:somedataset.sometable")
              .withTestServices(fakeBqServices)
              .withoutValidation();
      readTransform = useTemplateCompatibility ? read.withTemplateCompatibility() : read;
    } else {
      BigQueryIO.TypedRead<TableRow> read =
          BigQueryIO.readTableRows()
              .from("non-executing-project:somedataset.sometable")
              .withTestServices(fakeBqServices)
              .withoutValidation();
      readTransform = useTemplateCompatibility ? read.withTemplateCompatibility() : read;
    }
    PCollection<KV<String, Long>> output =
        p.apply(readTransform)
            .apply(
                ParDo.of(
                    new DoFn<TableRow, KV<String, Long>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        c.output(
                            KV.of(
                                (String) c.element().get("name"),
                                Long.valueOf((String) c.element().get("number"))));
                      }
                    }));

    PAssert.that(output)
        .containsInAnyOrder(ImmutableList.of(KV.of("a", 1L), KV.of("b", 2L), KV.of("c", 3L)));
    p.run();
  }

  @Test
  public void testBuildSourceDisplayDataTable() {
    String tableSpec = "project:dataset.tableid";

    BigQueryIO.Read read = BigQueryIO.read()
        .from(tableSpec)
        .withoutResultFlattening()
        .usingStandardSql()
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("table", tableSpec));
    assertThat(displayData, hasDisplayItem("flattenResults", false));
    assertThat(displayData, hasDisplayItem("useLegacySql", false));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testBuildSourceDisplayDataQuery() {
    BigQueryIO.Read read = BigQueryIO.read()
        .fromQuery("myQuery")
        .withoutResultFlattening()
        .usingStandardSql()
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("query", "myQuery"));
    assertThat(displayData, hasDisplayItem("flattenResults", false));
    assertThat(displayData, hasDisplayItem("useLegacySql", false));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testTableSourcePrimitiveDisplayData() throws IOException, InterruptedException {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigQueryIO.Read read = BigQueryIO.read()
        .from("project:dataset.tableId")
        .withTestServices(new FakeBigQueryServices()
            .withDatasetService(new FakeDatasetService())
            .withJobService(new FakeJobService()))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("BigQueryIO.Read should include the table spec in its primitive display data",
        displayData, hasItem(hasDisplayItem("table")));
  }

  @Test
  public void testQuerySourcePrimitiveDisplayData() throws IOException, InterruptedException {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigQueryIO.Read read = BigQueryIO.read()
        .fromQuery("foobar")
        .withTestServices(new FakeBigQueryServices()
            .withDatasetService(new FakeDatasetService())
            .withJobService(new FakeJobService()))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("BigQueryIO.Read should include the query in its primitive display data",
        displayData, hasItem(hasDisplayItem("query")));
  }

  @Test
  public void testBigQueryIOGetName() {
    assertEquals("BigQueryIO.Read",
        BigQueryIO.read().from("somedataset.sometable").getName());
  }

  @Test
  public void testBigQueryTableSourceInitSplit() throws Exception {
    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    TableReference table = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    fakeDatasetService.createDataset("project", "data_set", "", "", null);
    fakeDatasetService.createTable(new Table().setTableReference(table)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));
    fakeDatasetService.insertAll(table, expected, null);

    String stepUuid = "testStepUuid";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        stepUuid,
        ValueProvider.StaticValueProvider.of(table),
        fakeBqServices,
        TableRowJsonCoder.of(),
        BigQueryIO.TableRowParser.INSTANCE);

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(testFolder.getRoot().getAbsolutePath());
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setProject("project");

    List<TableRow> read =
        convertStringsToLong(
            SourceTestUtils.readFromSplitsOfSource(bqSource, 0L /* ignored */, options));
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
    // Simulate a repeated call to split(), like a Dataflow worker will sometimes do.
    sources = bqSource.split(200, options);
    assertEquals(2, sources.size());

    // A repeated call to split() should not have caused a duplicate extract job.
    assertEquals(1, fakeJobService.getNumExtractJobCalls());
  }

  @Test
  public void testEstimatedSizeWithoutStreamingBuffer() throws Exception {
    List<TableRow> data = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    TableReference table = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    fakeDatasetService.createDataset("project", "data_set", "", "", null);
    fakeDatasetService.createTable(new Table().setTableReference(table)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));
    fakeDatasetService.insertAll(table, data, null);

    String stepUuid = "testStepUuid";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        stepUuid,
        ValueProvider.StaticValueProvider.of(table),
        fakeBqServices,
        TableRowJsonCoder.of(),
        BigQueryIO.TableRowParser.INSTANCE);

    PipelineOptions options = PipelineOptionsFactory.create();
    assertEquals(108, bqSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testEstimatedSizeWithStreamingBuffer() throws Exception {
    List<TableRow> data = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    TableReference table = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    fakeDatasetService.createDataset("project", "data_set", "", "", null);
    fakeDatasetService.createTable(new Table().setTableReference(table)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER"))))
        .setStreamingBuffer(new Streamingbuffer().setEstimatedBytes(BigInteger.valueOf(10))));
    fakeDatasetService.insertAll(table, data, null);

    String stepUuid = "testStepUuid";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        stepUuid,
        ValueProvider.StaticValueProvider.of(table),
        fakeBqServices,
        TableRowJsonCoder.of(),
        BigQueryIO.TableRowParser.INSTANCE);

    PipelineOptions options = PipelineOptionsFactory.create();
    assertEquals(118, bqSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testBigQueryQuerySourceInitSplit() throws Exception {
    TableReference dryRunTable = new TableReference();

    Job queryJob = new Job();
    JobStatistics queryJobStats = new JobStatistics();
    JobStatistics2 queryStats = new JobStatistics2();
    queryStats.setReferencedTables(ImmutableList.of(dryRunTable));
    queryJobStats.setQuery(queryStats);
    queryJob.setStatus(new JobStatus())
        .setStatistics(queryJobStats);

    Job extractJob = new Job();
    JobStatistics extractJobStats = new JobStatistics();
    JobStatistics4 extractStats = new JobStatistics4();
    extractStats.setDestinationUriFileCounts(ImmutableList.of(1L));
    extractJobStats.setExtract(extractStats);
    extractJob.setStatus(new JobStatus())
        .setStatistics(extractJobStats);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    PipelineOptions options = PipelineOptionsFactory.create();
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setProject("project");
    String stepUuid = "testStepUuid";

    TableReference tempTableReference = createTempTableReference(
        bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));
    fakeDatasetService.createDataset(
        bqOptions.getProject(), tempTableReference.getDatasetId(), "", "", null);
    fakeDatasetService.createTable(new Table()
        .setTableReference(tempTableReference)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));

    String query = FakeBigQueryServices.encodeQuery(expected);
    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        stepUuid,
        ValueProvider.StaticValueProvider.of(query),
        true /* flattenResults */,
        true /* useLegacySql */,
        fakeBqServices,
        TableRowJsonCoder.of(),
        BigQueryIO.TableRowParser.INSTANCE,
        QueryPriority.BATCH,
        null);
    options.setTempLocation(testFolder.getRoot().getAbsolutePath());

    TableReference queryTable = new TableReference()
        .setProjectId(bqOptions.getProject())
        .setDatasetId(tempTableReference.getDatasetId())
        .setTableId(tempTableReference.getTableId());

    fakeJobService.expectDryRunQuery(bqOptions.getProject(), query,
        new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)
                .setReferencedTables(ImmutableList.of(queryTable))));

    List<TableRow> read = convertStringsToLong(
        SourceTestUtils.readFromSplitsOfSource(bqSource, 0L /* ignored */, options));
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
  }

  @Test
  public void testBigQueryNoTableQuerySourceInitSplit() throws Exception {
    TableReference dryRunTable = new TableReference();

    Job queryJob = new Job();
    JobStatistics queryJobStats = new JobStatistics();
    JobStatistics2 queryStats = new JobStatistics2();
    queryStats.setReferencedTables(ImmutableList.of(dryRunTable));
    queryJobStats.setQuery(queryStats);
    queryJob.setStatus(new JobStatus())
        .setStatistics(queryJobStats);

    Job extractJob = new Job();
    JobStatistics extractJobStats = new JobStatistics();
    JobStatistics4 extractStats = new JobStatistics4();
    extractStats.setDestinationUriFileCounts(ImmutableList.of(1L));
    extractJobStats.setExtract(extractStats);
    extractJob.setStatus(new JobStatus())
        .setStatistics(extractJobStats);

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference("project-id", createJobIdToken(options.getJobName(), stepUuid));
    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));
    fakeDatasetService.createDataset(
        tempTableReference.getProjectId(), tempTableReference.getDatasetId(), "", "", null);
    Table table = new Table()
        .setTableReference(tempTableReference)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER"))));
    fakeDatasetService.createTable(table);

    String query = FakeBigQueryServices.encodeQuery(expected);
    fakeJobService.expectDryRunQuery("project-id", query,
        new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)
                .setReferencedTables(ImmutableList.of(table.getTableReference()))));

    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        stepUuid,
        ValueProvider.StaticValueProvider.of(query),
        true /* flattenResults */,
        true /* useLegacySql */,
        fakeBqServices,
        TableRowJsonCoder.of(),
        BigQueryIO.TableRowParser.INSTANCE,
        QueryPriority.BATCH,
        null);

    options.setTempLocation(testFolder.getRoot().getAbsolutePath());

    List<TableRow> read = convertStringsToLong(
        SourceTestUtils.readFromSplitsOfSource(bqSource, 0L /* ignored */, options));
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
  }

  @Test
  public void testPassThroughThenCleanup() throws Exception {

    PCollection<Integer> output =
        p.apply(Create.of(1, 2, 3))
            .apply(
                new PassThroughThenCleanup<>(
                    new PassThroughThenCleanup.CleanupOperation() {
                      @Override
                      void cleanup(PassThroughThenCleanup.ContextContainer c) throws Exception {
                        // no-op
                      }
                    },
                    p.apply("Create1", Create.of("")).apply(View.asSingleton())));

    PAssert.that(output).containsInAnyOrder(1, 2, 3);

    p.run();
  }

  @Test
  public void testPassThroughThenCleanupExecuted() throws Exception {

    p.apply(Create.empty(VarIntCoder.of()))
        .apply(
            new PassThroughThenCleanup<>(
                new PassThroughThenCleanup.CleanupOperation() {
                  @Override
                  void cleanup(PassThroughThenCleanup.ContextContainer c) throws Exception {
                    throw new RuntimeException("cleanup executed");
                  }
                },
                p.apply("Create1", Create.of("")).apply(View.asSingleton())));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("cleanup executed");

    p.run();
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyInputTable() {
    BigQueryIO.Read read = BigQueryIO.read().from(
        p.newProvider("")).withoutValidation();
    // Test that this doesn't throw.
    DisplayData.from(read);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyInputQuery() {
    BigQueryIO.Read read = BigQueryIO.read().fromQuery(
        p.newProvider("")).withoutValidation();
    // Test that this doesn't throw.
    DisplayData.from(read);
  }

  List<TableRow> convertStringsToLong(List<TableRow> toConvert) {
    // The numbers come back as String after JSON serialization. Change them back to
    // longs so that we can assert the output.
    List<TableRow> converted = Lists.newArrayList();
    for (TableRow entry : toConvert) {
      TableRow convertedEntry = entry.clone();
      convertedEntry.set("number", Long.parseLong((String) convertedEntry.get("number")));
      converted.add(convertedEntry);
    }
    return converted;
  }

  @Test
  public void testCoderInference() {
    // Lambdas erase too much type information - use an anonymous class here.
    SerializableFunction<SchemaAndRecord, KV<ByteString, Mutation>> parseFn =
        new SerializableFunction<SchemaAndRecord, KV<ByteString, Mutation>>() {
          @Override
          public KV<ByteString, Mutation> apply(SchemaAndRecord input) {
            return null;
          }
        };

    assertEquals(
        KvCoder.of(ByteStringCoder.of(), ProtoCoder.of(Mutation.class)),
        BigQueryIO.read(parseFn).inferCoder(CoderRegistry.createDefault()));
  }
}
