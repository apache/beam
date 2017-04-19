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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.toJsonString;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics4;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonSchemaToTableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.PassThroughThenCleanup.CleanupOperation;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.SourceTestUtils.ExpectedSplitOutcome;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for BigQueryIO.
 */
@RunWith(JUnit4.class)
public class BigQueryIOTest implements Serializable {

  private static Path tempFolder;

  // Table information must be static, as each ParDo will get a separate instance of
  // FakeDatasetServices, and they must all modify the same storage.
  static com.google.common.collect.Table<String, String, Map<String, TableContainer>>
      tables = HashBasedTable.create();

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient ExpectedLogs loggedBigQueryIO = ExpectedLogs.none(BigQueryIO.class);
  @Rule public transient ExpectedLogs loggedWriteRename = ExpectedLogs.none(WriteRename.class);
  @Rule public transient ExpectedLogs loggedWriteTables = ExpectedLogs.none(WriteTables.class);
  @Rule public transient TemporaryFolder testFolder = new TemporaryFolder();

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

  private void checkWriteObject(
      BigQueryIO.Write write, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, String tableDescription) {
    checkWriteObjectWithValidate(
        write,
        project,
        dataset,
        table,
        schema,
        createDisposition,
        writeDisposition,
        tableDescription,
        true);
  }

  private void checkWriteObjectWithValidate(
      BigQueryIO.Write<TableRow> write, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, String tableDescription, boolean validate) {
    assertEquals(project, write.getTable().get().getProjectId());
    assertEquals(dataset, write.getTable().get().getDatasetId());
    assertEquals(table, write.getTable().get().getTableId());
    assertEquals(schema, write.getSchema());
    assertEquals(createDisposition, write.getCreateDisposition());
    assertEquals(writeDisposition, write.getWriteDisposition());
    assertEquals(tableDescription, write.getTableDescription());
    assertEquals(validate, write.getValidate());
  }

  @BeforeClass
  public static void setupClass() throws IOException {
    tempFolder = Files.createTempDirectory("BigQueryIOTest");
  }

  @Before
  public void setUp() throws IOException {
    tables = HashBasedTable.create();
    BigQueryIO.clearCreatedTables();
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
    String projectId = "someproject";
    String datasetId = "somedataset";
    String tableId = "sometable";
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject(projectId);

    Path baseDir = Files.createTempDirectory(tempFolder, "testValidateReadSetsDefaultProject");
    bqOptions.setTempLocation(baseDir.toString());

    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(projectId, datasetId, "", "");
    TableReference tableReference =
        new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
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

    Pipeline p = TestPipeline.create(bqOptions);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId(tableId);

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
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
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
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
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
    BigQueryOptions bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
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
  public void testReadFromTable() throws IOException, InterruptedException {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    Job job = new Job();
    JobStatus status = new JobStatus();
    job.setStatus(status);
    JobStatistics jobStats = new JobStatistics();
    job.setStatistics(jobStats);
    JobStatistics4 extract = new JobStatistics4();
    jobStats.setExtract(extract);
    extract.setDestinationUriFileCounts(ImmutableList.of(1L));

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
    fakeDatasetService.createDataset("non-executing-project", "somedataset", "", "");
    fakeDatasetService.createTable(sometable);

    List<TableRow> records = Lists.newArrayList(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L));
    fakeDatasetService.insertAll(sometable.getTableReference(), records, null);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(fakeDatasetService);

    Pipeline p = TestPipeline.create(bqOptions);
    PCollection<KV<String, Long>> output = p
        .apply(BigQueryIO.read().from("non-executing-project:somedataset.sometable")
            .withTestServices(fakeBqServices)
            .withoutValidation())
        .apply(ParDo.of(new DoFn<TableRow, KV<String, Long>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(KV.of((String) c.element().get("name"),
                Long.valueOf((String) c.element().get("number"))));
          }
        }));

    PAssert.that(output)
        .containsInAnyOrder(ImmutableList.of(KV.of("a", 1L), KV.of("b", 2L), KV.of("c", 3L)));
    p.run();
  }

  @Test
  public void testWrite() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);

    datasetService.createDataset("defaultproject", "dataset-id", "", "");

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
    .apply(BigQueryIO.writeTableRows().to("dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(new TableSchema().setFields(
            ImmutableList.of(
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("number").setType("INTEGER"))))
        .withTestServices(fakeBqServices)
        .withoutValidation());
    p.run();

    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 0);
  }

  @Test
  public void testStreamingWrite() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    datasetService.createDataset("project-id", "dataset-id", "", "");
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
            .withDatasetService(datasetService);

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3),
        new TableRow().set("name", "d").set("number", 4))
          .withCoder(TableRowJsonCoder.of()))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
            .apply(BigQueryIO.writeTableRows().to("project-id:dataset-id.table-id")
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(new TableSchema().setFields(
                    ImmutableList.of(
                        new TableFieldSchema().setName("name").setType("STRING"),
                        new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();


    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow().set("name", "a").set("number", 1),
            new TableRow().set("name", "b").set("number", 2),
            new TableRow().set("name", "c").set("number", 3),
            new TableRow().set("name", "d").set("number", 4)));
  }

  /**
   * A generic window function that allows partitioning data into windows by a string value.
   *
   * <p>Logically, creates multiple global windows, and the user provides a function that
   * decides which global window a value should go into.
   */
  private static class PartitionedGlobalWindows<T> extends
      NonMergingWindowFn<T, PartitionedGlobalWindow> {
    private SerializableFunction<T, String> extractPartition;

    public PartitionedGlobalWindows(SerializableFunction<T, String> extractPartition) {
      this.extractPartition = extractPartition;
    }

    @Override
    public Collection<PartitionedGlobalWindow> assignWindows(AssignContext c) {
      return Collections.singletonList(new PartitionedGlobalWindow(
          extractPartition.apply(c.element())));
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> o) {
      return o instanceof PartitionedGlobalWindows;
    }

    @Override
    public Coder<PartitionedGlobalWindow> windowCoder() {
      return new PartitionedGlobalWindowCoder();
    }

    @Override
    public WindowMappingFn<PartitionedGlobalWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException(
          "PartitionedGlobalWindows is not allowed in side inputs");
    }

    @Override
    public Instant getOutputTime(Instant inputTimestamp, PartitionedGlobalWindow window) {
      return inputTimestamp;
    }
  }

  /**
   * Custom Window object that encodes a String value.
   */
  private static class PartitionedGlobalWindow extends BoundedWindow {
    String value;

    public PartitionedGlobalWindow(String value) {
      this.value = value;
    }

    @Override
    public Instant maxTimestamp() {
      return GlobalWindow.INSTANCE.maxTimestamp();
    }

    // The following methods are only needed due to BEAM-1022. Once this issue is fixed, we will
    // no longer need these.
    @Override
    public boolean equals(Object other) {
      if (other instanceof PartitionedGlobalWindow) {
        return value.equals(((PartitionedGlobalWindow) other).value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  /**
   * Coder for @link{PartitionedGlobalWindow}.
   */
  private static class PartitionedGlobalWindowCoder extends AtomicCoder<PartitionedGlobalWindow> {
    @Override
    public void encode(PartitionedGlobalWindow window, OutputStream outStream, Context context)
        throws IOException, CoderException {
      StringUtf8Coder.of().encode(window.value, outStream, context);
    }

    @Override
    public PartitionedGlobalWindow decode(InputStream inStream, Context context)
        throws IOException, CoderException {
      return new PartitionedGlobalWindow(StringUtf8Coder.of().decode(inStream, context));
    }
  }

  @Test
  public void testStreamingWriteWithDynamicTables() throws Exception {
    testWriteWithDynamicTables(true);
  }

  @Test
  public void testBatchWriteWithDynamicTables() throws Exception {
    testWriteWithDynamicTables(false);
  }

  public void testWriteWithDynamicTables(boolean streaming) throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    datasetService.createDataset("project-id", "dataset-id", "", "");
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withDatasetService(datasetService)
        .withJobService(new FakeJobService());

    List<Integer> inserts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      inserts.add(i);
    }

    // Create a windowing strategy that puts the input into five different windows depending on
    // record value.
    WindowFn<Integer, PartitionedGlobalWindow> window = new PartitionedGlobalWindows(
        new SerializableFunction<Integer, String>() {
          @Override
          public String apply(Integer i) {
            return Integer.toString(i % 5);
          }
        }
    );

    SerializableFunction<ValueInSingleWindow<Integer>, TableDestination> tableFunction =
        new SerializableFunction<ValueInSingleWindow<Integer>, TableDestination>() {
          @Override
          public TableDestination apply(ValueInSingleWindow<Integer> input) {
            PartitionedGlobalWindow window = (PartitionedGlobalWindow) input.getWindow();
            // Check that we can access the element as well here.
            checkArgument(window.value.equals(Integer.toString(input.getValue() % 5)),
                "Incorrect element");
            return new TableDestination("project-id:dataset-id.table-id-" + window.value, "");
          }
    };

    Pipeline p = TestPipeline.create(bqOptions);
    PCollection<Integer> input = p.apply(Create.of(inserts));
    if (streaming) {
      input = input.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    }
    input.apply(Window.<Integer>into(window))
        .apply(BigQueryIO.<Integer>write()
            .to(tableFunction)
            .withFormatFunction(new SerializableFunction<Integer, TableRow>() {
              @Override
              public TableRow apply(Integer i) {
                return new TableRow().set("name", "number" + i).set("number", i);
              }})
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(new TableSchema().setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withTestServices(fakeBqServices)
            .withoutValidation());
    p.run();


    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id-0"),
        containsInAnyOrder(
            new TableRow().set("name", "number0").set("number", 0),
            new TableRow().set("name", "number5").set("number", 5)));
    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id-1"),
        containsInAnyOrder(
            new TableRow().set("name", "number1").set("number", 1),
            new TableRow().set("name", "number6").set("number", 6)));
    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id-2"),
        containsInAnyOrder(
            new TableRow().set("name", "number2").set("number", 2),
            new TableRow().set("name", "number7").set("number", 7)));
    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id-3"),
        containsInAnyOrder(
            new TableRow().set("name", "number3").set("number", 3),
            new TableRow().set("name", "number8").set("number", 8)));
    assertThat(datasetService.getAllRows("project-id", "dataset-id", "table-id-4"),
        containsInAnyOrder(
            new TableRow().set("name", "number4").set("number", 4),
            new TableRow().set("name", "number9").set("number", 9)));
  }

  @Test
  public void testWriteUnknown() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);
    datasetService.createDataset("project-id", "dataset-id", "", "");
    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
    .apply(BigQueryIO.writeTableRows().to("project-id:dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withTestServices(fakeBqServices)
        .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to create load job");
    try {
      p.run();
    } finally {
      File tempDir = new File(bqOptions.getTempLocation());
      testNumFiles(tempDir, 0);
    }
  }

  @Test
  public void testWriteFailedJobs() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
        .apply(BigQueryIO.writeTableRows().to("dataset-id.table-id")
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withTestServices(fakeBqServices)
            .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to create load job with id prefix");
    thrown.expectMessage("reached max retries");
    thrown.expectMessage("last failed load job");

    try {
      p.run();
    } finally {
      File tempDir = new File(bqOptions.getTempLocation());
      testNumFiles(tempDir, 0);
    }
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
  @Ignore("[BEAM-436] DirectRunner tempLocation configuration insufficient")
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
  @Ignore("[BEAM-436] DirectRunner tempLocation configuration insufficient")
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
  public void testBuildWrite() {
    BigQueryIO.Write<TableRow> write =
            BigQueryIO.writeTableRows().to("foo.com:project:somedataset.sometable");
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  @Ignore("[BEAM-436] DirectRunner tempLocation configuration insufficient")
  public void testBatchWritePrimitiveDisplayData() throws IOException, InterruptedException {
    testWritePrimitiveDisplayData(/* streaming: */ false);
  }

  @Test
  @Ignore("[BEAM-436] DirectRunner tempLocation configuration insufficient")
  public void testStreamingWritePrimitiveDisplayData() throws IOException, InterruptedException {
    testWritePrimitiveDisplayData(/* streaming: */ true);
  }

  private void testWritePrimitiveDisplayData(boolean streaming) throws IOException,
      InterruptedException {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(streaming);
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create(options);

    BigQueryIO.Write write = BigQueryIO.writeTableRows()
        .to("project:dataset.table")
        .withSchema(new TableSchema().set("col1", "type1").set("col2", "type2"))
        .withTestServices(new FakeBigQueryServices()
          .withDatasetService(new FakeDatasetService())
          .withJobService(new FakeJobService()))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("BigQueryIO.Write should include the table spec in its primitive display data",
        displayData, hasItem(hasDisplayItem("tableSpec")));

    assertThat("BigQueryIO.Write should include the table schema in its primitive display data",
        displayData, hasItem(hasDisplayItem("schema")));
  }

  @Test
  public void testBuildWriteWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Write write =
        BigQueryIO.<TableRow>write().to("foo.com:project:somedataset.sometable")
            .withoutValidation();
    checkWriteObjectWithValidate(
        write,
        "foo.com:project",
        "somedataset",
        "sometable",
        null,
        CreateDisposition.CREATE_IF_NEEDED,
        WriteDisposition.WRITE_EMPTY,
        "",
        false);
  }

  @Test
  public void testBuildWriteDefaultProject() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to("somedataset" + ".sometable");
    checkWriteObject(
        write, null, "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY,
        "");
  }

  @Test
  public void testBuildWriteWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows().to(table);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  public void testBuildWriteWithSchema() {
    TableSchema schema = new TableSchema();
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.<TableRow>write().to("foo.com:project:somedataset.sometable").withSchema(schema);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        schema, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  public void testBuildWriteWithCreateDispositionNever() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.<TableRow>write()
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_NEVER, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  public void testBuildWriteWithCreateDispositionIfNeeded() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  public void testBuildWriteWithWriteDispositionTruncate() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.<TableRow>write()
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_TRUNCATE, "");
  }

  @Test
  public void testBuildWriteWithWriteDispositionAppend() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND, "");
  }

  @Test
  public void testBuildWriteWithWriteDispositionEmpty() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.<TableRow>write()
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_EMPTY);
    checkWriteObject(
        write, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, "");
  }

  @Test
  public void testBuildWriteWithWriteWithTableDescription() {
    final String tblDescription = "foo bar table";
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to("foo.com:project:somedataset.sometable")
        .withTableDescription(tblDescription);
    checkWriteObject(
        write,
        "foo.com:project",
        "somedataset",
        "sometable",
        null,
        CreateDisposition.CREATE_IF_NEEDED,
        WriteDisposition.WRITE_EMPTY,
        tblDescription);
  }

  @Test
  public void testBuildWriteDisplayData() {
    String tableSpec = "project:dataset.table";
    TableSchema schema = new TableSchema().set("col1", "type1").set("col2", "type2");
    final String tblDescription = "foo bar table";

    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to(tableSpec)
        .withSchema(schema)
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withTableDescription(tblDescription)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("table"));
    assertThat(displayData, hasDisplayItem("schema"));
    assertThat(displayData,
        hasDisplayItem("createDisposition", CreateDisposition.CREATE_IF_NEEDED.toString()));
    assertThat(displayData,
        hasDisplayItem("writeDisposition", WriteDisposition.WRITE_APPEND.toString()));
    assertThat(displayData,
        hasDisplayItem("tableDescription", tblDescription));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  private void testWriteValidatesDataset(boolean unbounded) throws Exception {
    String projectId = "someproject";
    String datasetId = "somedataset";

    BigQueryOptions options = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    options.setProject(projectId);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(new FakeDatasetService());

    Pipeline p = TestPipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId("sometable");

    PCollection<TableRow> tableRows;
    if (unbounded) {
      tableRows =
          p.apply(CountingInput.unbounded())
              .apply(
                  MapElements.via(
                      new SimpleFunction<Long, TableRow>() {
                        @Override
                        public TableRow apply(Long input) {
                          return null;
                        }
                      }))
              .setCoder(TableRowJsonCoder.of());
    } else {
      tableRows = p
          .apply(Create.empty(TableRowJsonCoder.of()));
    }

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    tableRows
        .apply(
            BigQueryIO.writeTableRows().to(tableRef)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(new TableSchema())
                .withTestServices(fakeBqServices));
  }

  @Test
  public void testWriteValidatesDatasetBatch() throws Exception {
    testWriteValidatesDataset(false);
  }

  @Test
  public void testWriteValidatesDatasetStreaming() throws Exception {
    testWriteValidatesDataset(true);
  }

  @Test
  public void testCreateNeverWithStreaming() throws Exception {
    BigQueryOptions options = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    options.setProject("project");
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId("dataset");
    tableRef.setTableId("sometable");

    PCollection<TableRow> tableRows =
        p.apply(CountingInput.unbounded())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, TableRow>() {
                  @Override
                  public TableRow apply(Long input) {
                    return null;
                  }
                }))
        .setCoder(TableRowJsonCoder.of());
    tableRows
        .apply(BigQueryIO.writeTableRows().to(tableRef)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withoutValidation());
  }

  @Test
  public void testTableParsing() {
    TableReference ref = BigQueryHelpers
        .parseTableSpec("my-project:data_set.table_name");
    Assert.assertEquals("my-project", ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_validPatterns() {
    BigQueryHelpers.parseTableSpec("a123-456:foo_bar.d");
    BigQueryHelpers.parseTableSpec("a12345:b.c");
    BigQueryHelpers.parseTableSpec("b12345.c");
  }

  @Test
  public void testTableParsing_noProjectId() {
    TableReference ref = BigQueryHelpers
        .parseTableSpec("data_set.table_name");
    Assert.assertEquals(null, ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsingError() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("0123456:foo.bar");
  }

  @Test
  public void testTableParsingError_2() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("myproject:.bar");
  }

  @Test
  public void testTableParsingError_3() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec(":a.b");
  }

  @Test
  public void testTableParsingError_slash() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("a\\b12345:c.d");
  }

  // Test that BigQuery's special null placeholder objects can be encoded.
  @Test
  public void testCoder_nullCell() throws CoderException {
    TableRow row = new TableRow();
    row.set("temperature", Data.nullOf(Object.class));
    row.set("max_temperature", Data.nullOf(Object.class));

    byte[] bytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), row);

    TableRow newRow = CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), bytes);
    byte[] newBytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), newRow);

    Assert.assertArrayEquals(bytes, newBytes);
  }

  @Test
  public void testBigQueryIOGetName() {
    assertEquals("BigQueryIO.Read",
        BigQueryIO.read().from("somedataset.sometable").getName());
    assertEquals("BigQueryIO.Write",
        BigQueryIO.<TableRow>write().to("somedataset.sometable").getName());
  }

  @Test
  public void testWriteValidateFailsCreateNoSchema() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("no schema was provided");
    p
        .apply(Create.empty(TableRowJsonCoder.of()))
        .apply(BigQueryIO.writeTableRows()
            .to("dataset.table")
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testBigQueryTableSourceThroughJsonAPI() throws Exception {
    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", "1"),
        new TableRow().set("name", "b").set("number", "2"),
        new TableRow().set("name", "c").set("number", "3"),
        new TableRow().set("name", "d").set("number", "4"),
        new TableRow().set("name", "e").set("number", "5"),
        new TableRow().set("name", "f").set("number", "6"));

    TableReference table = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    datasetService.createDataset(table.getProjectId(), table.getDatasetId(), "", "");
    datasetService.createTable(new Table().setTableReference(table));
    datasetService.insertAll(table, expected, null);

    Path baseDir = Files.createTempDirectory(tempFolder, "testBigQueryTableSourceThroughJsonAPI");
    String jobIdToken = "testJobIdToken";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of(table),
        baseDir.toString(), fakeBqServices,
        StaticValueProvider.of("project"));

    PipelineOptions options = PipelineOptionsFactory.create();
    Assert.assertThat(
        SourceTestUtils.readFromSource(bqSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);
  }

  @Test
  public void testBigQueryTableSourceInitSplit() throws Exception {
    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    FakeJobService fakeJobService = new FakeJobService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(fakeJobService)
        .withDatasetService(fakeDatasetService);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    TableReference table = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    fakeDatasetService.createDataset("project", "data_set", "", "");
    fakeDatasetService.createTable(new Table().setTableReference(table)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));
    fakeDatasetService.insertAll(table, expected, null);

    Path baseDir = Files.createTempDirectory(tempFolder, "testBigQueryTableSourceInitSplit");

    String jobIdToken = "testJobIdToken";
    String extractDestinationDir = baseDir.toString();
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of(table),
        extractDestinationDir, fakeBqServices, StaticValueProvider.of("project"));


    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(baseDir.toString());

    List<TableRow> read = SourceTestUtils.readFromSource(bqSource, options);
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
    // Simulate a repeated call to split(), like a Dataflow worker will sometimes do.
    sources = bqSource.split(200, options);
    assertEquals(2, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));

    // A repeated call to split() should not have caused a duplicate extract job.
    assertEquals(1, fakeJobService.getNumExtractJobCalls());
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

    FakeJobService fakeJobService = new FakeJobService();
    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(fakeJobService)
        .withDatasetService(fakeDatasetService);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));

    TableReference destinationTable = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    fakeDatasetService.createDataset("project", "data_set", "", "");
    fakeDatasetService.createTable(new Table()
        .setTableReference(destinationTable)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));
    Path baseDir = Files.createTempDirectory(tempFolder, "testBigQueryQuerySourceInitSplit");

    String jobIdToken = "testJobIdToken";
    String query = FakeBigQueryServices.encodeQuery(expected);
    String extractDestinationDir = baseDir.toString();
    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of(query),
        StaticValueProvider.of(destinationTable),
        true /* flattenResults */, true /* useLegacySql */,
        extractDestinationDir, fakeBqServices);

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(extractDestinationDir);

    TableReference queryTable = new TableReference()
        .setProjectId("project")
        .setDatasetId("data_set")
        .setTableId("table_name");

    fakeJobService.expectDryRunQuery("project", query,
        new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)
                .setReferencedTables(ImmutableList.of(queryTable))));

    List<TableRow> read = SourceTestUtils.readFromSource(bqSource, options);
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));
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

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeJobService jobService = new FakeJobService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(jobService)
        .withDatasetService(datasetService);

    TableReference destinationTable = BigQueryHelpers.parseTableSpec("project:data_set.table_name");
    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L),
        new TableRow().set("name", "d").set("number", 4L),
        new TableRow().set("name", "e").set("number", 5L),
        new TableRow().set("name", "f").set("number", 6L));
    datasetService.createDataset(destinationTable.getProjectId(), destinationTable.getDatasetId(),
        "", "");
    Table table = new Table()
        .setTableReference(destinationTable)
        .setSchema(new TableSchema()
                .setFields(
                    ImmutableList.of(
                        new TableFieldSchema().setName("name").setType("STRING"),
                        new TableFieldSchema().setName("number").setType("INTEGER"))));
    datasetService.createTable(table);

    String query = FakeBigQueryServices.encodeQuery(expected);
    jobService.expectDryRunQuery("project", query,
        new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)
                .setReferencedTables(ImmutableList.of(table.getTableReference()))));

    Path baseDir = Files.createTempDirectory(tempFolder, "testBigQueryNoTableQuerySourceInitSplit");
    String jobIdToken = "testJobIdToken";
    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        StaticValueProvider.of(jobIdToken),
        StaticValueProvider.of(query),
        StaticValueProvider.of(destinationTable),
        true /* flattenResults */, true /* useLegacySql */, baseDir.toString(), fakeBqServices);



    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(baseDir.toString());
    List<TableRow> read = convertBigDecimaslToLong(
        SourceTestUtils.readFromSource(bqSource, options));
    assertThat(read, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.split(100, options);
    assertEquals(2, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));
  }

  @Test
  public void testTransformingSource() throws Exception {
    int numElements = 10000;
    @SuppressWarnings("deprecation")
    BoundedSource<Long> longSource = CountingSource.upTo(numElements);
    SerializableFunction<Long, String> toStringFn =
        new SerializableFunction<Long, String>() {
          @Override
          public String apply(Long input) {
            return input.toString();
         }};
    BoundedSource<String> stringSource = new TransformingSource<>(
        longSource, toStringFn, StringUtf8Coder.of());

    List<String> expected = Lists.newArrayList();
    for (int i = 0; i < numElements; i++) {
      expected.add(String.valueOf(i));
    }

    PipelineOptions options = PipelineOptionsFactory.create();
    Assert.assertThat(
        SourceTestUtils.readFromSource(stringSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        stringSource, 100, 0.3, ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT, options);

    SourceTestUtils.assertSourcesEqualReferenceSource(
        stringSource, stringSource.split(100, options), options);
  }

  @Test
  public void testTransformingSourceUnsplittable() throws Exception {
    int numElements = 10000;
    @SuppressWarnings("deprecation")
    BoundedSource<Long> longSource =
        SourceTestUtils.toUnsplittableSource(CountingSource.upTo(numElements));
    SerializableFunction<Long, String> toStringFn =
        new SerializableFunction<Long, String>() {
          @Override
          public String apply(Long input) {
            return input.toString();
          }
        };
    BoundedSource<String> stringSource =
        new TransformingSource<>(longSource, toStringFn, StringUtf8Coder.of());

    List<String> expected = Lists.newArrayList();
    for (int i = 0; i < numElements; i++) {
      expected.add(String.valueOf(i));
    }

    PipelineOptions options = PipelineOptionsFactory.create();
    Assert.assertThat(
        SourceTestUtils.readFromSource(stringSource, options), CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        stringSource, 100, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    SourceTestUtils.assertSourcesEqualReferenceSource(
        stringSource, stringSource.split(100, options), options);
  }

  @Test
  public void testPassThroughThenCleanup() throws Exception {

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3))
        .apply(new PassThroughThenCleanup<Integer>(new CleanupOperation() {
          @Override
          void cleanup(PipelineOptions options) throws Exception {
            // no-op
          }}));

    PAssert.that(output).containsInAnyOrder(1, 2, 3);

    p.run();
  }

  @Test
  public void testPassThroughThenCleanupExecuted() throws Exception {

    p.apply(Create.empty(VarIntCoder.of()))
        .apply(new PassThroughThenCleanup<Integer>(new CleanupOperation() {
          @Override
          void cleanup(PipelineOptions options) throws Exception {
            throw new RuntimeException("cleanup executed");
          }}));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("cleanup executed");

    p.run();
  }

  @Test
  public void testWritePartitionEmptyData() throws Exception {
    long numFiles = 0;
    long fileSize = 0;

    // An empty file is created for no input data. One partition is needed.
    long expectedNumPartitions = 1;
    testWritePartition(1, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionSinglePartition() throws Exception {
    long numFiles = BigQueryIO.Write.MAX_NUM_FILES;
    long fileSize = 1;

    // One partition is needed.
    long expectedNumPartitions = 1;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionManyFiles() throws Exception {
    long numFiles = BigQueryIO.Write.MAX_NUM_FILES * 3;
    long fileSize = 1;

    // One partition is needed for each group of BigQueryWrite.MAX_NUM_FILES files.
    long expectedNumPartitions = 3;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionLargeFileSize() throws Exception {
    long numFiles = 10;
    long fileSize = BigQueryIO.Write.MAX_SIZE_BYTES / 3;

    // One partition is needed for each group of three files.
    long expectedNumPartitions = 4;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  private void testWritePartition(long numTables, long numFilesPerTable, long fileSize,
                                  long expectedNumPartitionsPerTable)
      throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    // In the case where a static destination is specified (i.e. not through a dynamic table
    // function) and there is no input data, WritePartition will generate an empty table. This
    // code is to test that path.
    TableReference singletonReference = new TableReference()
        .setProjectId("projectid")
        .setDatasetId("dataset")
        .setTableId("table");
    String singletonDescription = "singleton";
    boolean isSingleton = numTables == 1 && numFilesPerTable == 0;

    List<ShardedKey<TableDestination>> expectedPartitions = Lists.newArrayList();
    if (isSingleton) {
      expectedPartitions.add(ShardedKey.of(
          new TableDestination(singletonReference, singletonDescription), 1));
    } else {
      for (int i = 0; i < numTables; ++i) {
        for (int j = 1; j <= expectedNumPartitionsPerTable; ++j) {
          String tableName = String.format("project-id:dataset-id.tables%05d", i);
          TableDestination destination = new TableDestination(tableName, tableName);
          expectedPartitions.add(ShardedKey.of(destination, j));
        }
      }
    }

    List<WriteBundlesToFiles.Result> files = Lists.newArrayList();
    Map<TableDestination, List<String>> filenamesPerTable = Maps.newHashMap();
    for (int i = 0; i < numTables; ++i) {
      String tableName = String.format("project-id:dataset-id.tables%05d", i);
      TableDestination destination = new TableDestination(tableName, tableName);
      List<String> filenames = filenamesPerTable.get(destination);
      if (filenames == null) {
        filenames = Lists.newArrayList();
        filenamesPerTable.put(destination, filenames);
      }
      for (int j = 0; j < numFilesPerTable; ++j) {
        String fileName = String.format("%s_files%05d", tableName, j);
        filenames.add(fileName);
        files.add(new Result(fileName, fileSize, destination));
      }
    }

    TupleTag<KV<ShardedKey<TableDestination>, List<String>>> multiPartitionsTag =
        new TupleTag<KV<ShardedKey<TableDestination>, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<ShardedKey<TableDestination>, List<String>>> singlePartitionTag =
        new TupleTag<KV<ShardedKey<TableDestination>, List<String>>>("singlePartitionTag") {};

    PCollection<WriteBundlesToFiles.Result> filesPCollection =
        p.apply(Create.of(files).withType(new TypeDescriptor<WriteBundlesToFiles.Result>() {}));
    PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView =
        PCollectionViews.iterableView(
        filesPCollection,
        WindowingStrategy.globalDefault(),
        WriteBundlesToFiles.ResultCoder.of());

    ValueProvider<String> singletonTable = null;
    if (isSingleton) {
      singletonTable = StaticValueProvider.of(BigQueryHelpers.toJsonString(singletonReference));
    }
    WritePartition writePartition =
        new WritePartition(singletonTable,
            "singleton", resultsView,
            multiPartitionsTag, singlePartitionTag);

    DoFnTester<String, KV<ShardedKey<TableDestination>, List<String>>> tester =
        DoFnTester.of(writePartition);
    tester.setSideInput(resultsView, GlobalWindow.INSTANCE, files);
    tester.processElement(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    List<KV<ShardedKey<TableDestination>, List<String>>> partitions;
    if (expectedNumPartitionsPerTable > 1) {
      partitions = tester.takeOutputElements(multiPartitionsTag);
    } else {
      partitions = tester.takeOutputElements(singlePartitionTag);
    }


    List<ShardedKey<TableDestination>> partitionsResult = Lists.newArrayList();
    Map<TableDestination, List<String>> filesPerTableResult = Maps.newHashMap();
    for (KV<ShardedKey<TableDestination>, List<String>> partition : partitions) {
      TableDestination table = partition.getKey().getKey();
      partitionsResult.add(partition.getKey());
      List<String> tableFilesResult = filesPerTableResult.get(table);
      if (tableFilesResult == null) {
        tableFilesResult = Lists.newArrayList();
        filesPerTableResult.put(table, tableFilesResult);
      }
      tableFilesResult.addAll(partition.getValue());
    }

    assertThat(partitionsResult,
        containsInAnyOrder(Iterables.toArray(expectedPartitions, ShardedKey.class)));

    if (isSingleton) {
      assertEquals(1, filesPerTableResult.size());
      List<String> singletonFiles = filesPerTableResult.values().iterator().next();
      assertTrue(Files.exists(Paths.get(singletonFiles.get(0))));
      assertThat(Files.readAllBytes(Paths.get(singletonFiles.get(0))).length,
          Matchers.equalTo(0));
    } else {
      assertEquals(filenamesPerTable, filesPerTableResult);
    }
  }

  @Test
  public void testWriteTables() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);
    datasetService.createDataset("project-id", "dataset-id", "", "");
    long numTables = 3;
    long numPartitions = 3;
    long numFilesPerPartition = 10;
    String jobIdToken = "jobIdToken";
    String tempFilePrefix = "tempFilePrefix";
    Map<TableDestination, List<String>> expectedTempTables = Maps.newHashMap();

    Path baseDir = Files.createTempDirectory(tempFolder, "testWriteTables");

    List<KV<ShardedKey<TableDestination>, List<String>>> partitions = Lists.newArrayList();
    for (int i = 0; i < numTables; ++i) {
      String tableName = String.format("project-id:dataset-id.table%05d", i);
      TableDestination tableDestination = new TableDestination(tableName, tableName);
      for (int j = 0; j < numPartitions; ++j) {
        String tempTableId = BigQueryHelpers.createJobId(jobIdToken, tableDestination, j);
        List<String> filesPerPartition = Lists.newArrayList();
        for (int k = 0; k < numFilesPerPartition; ++k) {
          String filename = Paths.get(baseDir.toString(),
              String.format("files0x%08x_%05d", tempTableId.hashCode(), k)).toString();
          try (WritableByteChannel channel = IOChannelUtils.create(filename, MimeTypes.TEXT)) {
            try (OutputStream output = Channels.newOutputStream(channel)) {
              TableRow tableRow = new TableRow().set("name", tableName);
              TableRowJsonCoder.of().encode(tableRow, output, Context.OUTER);
              output.write("\n".getBytes(StandardCharsets.UTF_8));
            }
          }
          filesPerPartition.add(filename);
        }
        partitions.add(KV.of(ShardedKey.of(tableDestination, j),
            filesPerPartition));

        List<String> expectedTables = expectedTempTables.get(tableDestination);
        if (expectedTables == null) {
          expectedTables = Lists.newArrayList();
          expectedTempTables.put(tableDestination, expectedTables);
        }
        String json = String.format(
            "{\"datasetId\":\"dataset-id\",\"projectId\":\"project-id\",\"tableId\":\"%s\"}",
            tempTableId);
        expectedTables.add(json);
      }
    }

    PCollection<String> jobIdTokenCollection = p.apply("CreateJobId", Create.of("jobId"));
    PCollectionView<String> jobIdTokenView =
        jobIdTokenCollection.apply(View.<String>asSingleton());

    WriteTables writeTables = new WriteTables(
        false,
        fakeBqServices,
        jobIdTokenView,
        tempFilePrefix,
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED,
        null);

    DoFnTester<KV<ShardedKey<TableDestination>, List<String>>,
        KV<TableDestination, String>> tester = DoFnTester.of(writeTables);
    tester.setSideInput(jobIdTokenView, GlobalWindow.INSTANCE, jobIdToken);
    for (KV<ShardedKey<TableDestination>, List<String>> partition : partitions) {
      tester.processElement(partition);
    }

    Map<TableDestination, List<String>> tempTablesResult = Maps.newHashMap();
    for (KV<TableDestination, String> element : tester.takeOutputElements()) {
      List<String> tables = tempTablesResult.get(element.getKey());
      if (tables == null) {
        tables = Lists.newArrayList();
        tempTablesResult.put(element.getKey(), tables);
      }
      tables.add(element.getValue());
    }
    assertEquals(expectedTempTables, tempTablesResult);
  }

  @Test
  public void testRemoveTemporaryFiles() throws Exception {
    BigQueryOptions bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("defaultproject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    int numFiles = 10;
    List<String> fileNames = Lists.newArrayList();
    String tempFilePrefix = bqOptions.getTempLocation() + "/";
    TableRowWriter writer = new TableRowWriter(tempFilePrefix);
    for (int i = 0; i < numFiles; ++i) {
      String fileName = String.format("files%05d", i);
      writer.open(fileName);
      fileNames.add(writer.close().filename);
    }
    fileNames.add(tempFilePrefix + String.format("files%05d", numFiles));

    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 10);

    WriteTables.removeTemporaryFiles(bqOptions, tempFilePrefix, fileNames);

    testNumFiles(tempDir, 0);

    for (String fileName : fileNames) {
      loggedWriteTables.verifyDebug("Removing file " + fileName);
    }
    loggedWriteTables.verifyDebug(fileNames.get(numFiles) + " does not exist.");
  }

  @Test
  public void testWriteRename() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    FakeDatasetService datasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(datasetService);
    datasetService.createDataset("project-id", "dataset-id", "", "");

    final int numFinalTables = 3;
    final int numTempTablesPerFinalTable = 3;
    final int numRecordsPerTempTable = 10;

    Map<TableDestination, List<TableRow>> expectedRowsPerTable = Maps.newHashMap();
    String jobIdToken = "jobIdToken";
    Map<TableDestination, Iterable<String>> tempTables = Maps.newHashMap();
    for (int i = 0; i < numFinalTables; ++i) {
      String tableName = "project-id:dataset-id.table_" + i;
      TableDestination tableDestination = new TableDestination(
          tableName, "table_" + i + "_desc");
      List<String> tables = Lists.newArrayList();
      tempTables.put(tableDestination, tables);

      List<TableRow> expectedRows = expectedRowsPerTable.get(tableDestination);
      if (expectedRows == null) {
        expectedRows = Lists.newArrayList();
        expectedRowsPerTable.put(tableDestination, expectedRows);
      }
      for (int j = 0; i < numTempTablesPerFinalTable; ++i) {
        TableReference tempTable = new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId(String.format("%s_%05d_%05d", jobIdToken, i, j));
        datasetService.createTable(new Table().setTableReference(tempTable));

        List<TableRow> rows = Lists.newArrayList();
        for (int k = 0; k < numRecordsPerTempTable; ++k) {
          rows.add(new TableRow().set("number", j * numTempTablesPerFinalTable + k));
        }
        datasetService.insertAll(tempTable, rows, null);
        expectedRows.addAll(rows);
        tables.add(BigQueryHelpers.toJsonString(tempTable));
      }
    }

    PCollection<KV<TableDestination, String>> tempTablesPCollection =
        p.apply(Create.of(tempTables)
            .withCoder(KvCoder.of(TableDestinationCoder.of(),
                IterableCoder.of(StringUtf8Coder.of()))))
            .apply(ParDo.of(new DoFn<KV<TableDestination, Iterable<String>>,
                KV<TableDestination, String>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                TableDestination tableDestination = c.element().getKey();
                for (String tempTable : c.element().getValue()) {
                  c.output(KV.of(tableDestination, tempTable));
                }
              }
            }));

    PCollectionView<Map<TableDestination, Iterable<String>>> tempTablesView =
        PCollectionViews.multimapView(
            tempTablesPCollection,
        WindowingStrategy.globalDefault(),
        KvCoder.of(TableDestinationCoder.of(),
            StringUtf8Coder.of()));

    PCollection<String> jobIdTokenCollection = p.apply("CreateJobId", Create.of("jobId"));
    PCollectionView<String> jobIdTokenView =
        jobIdTokenCollection.apply(View.<String>asSingleton());

    WriteRename writeRename = new WriteRename(
        fakeBqServices,
        jobIdTokenView,
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED,
        tempTablesView);

    DoFnTester<String, Void> tester = DoFnTester.of(writeRename);
    tester.setSideInput(tempTablesView, GlobalWindow.INSTANCE, tempTables);
    tester.setSideInput(jobIdTokenView, GlobalWindow.INSTANCE, jobIdToken);
    tester.processElement(null);

    for (Map.Entry<TableDestination, Iterable<String>> entry : tempTables.entrySet()) {
      TableDestination tableDestination = entry.getKey();
      TableReference tableReference = tableDestination.getTableReference();
      Table table = checkNotNull(datasetService.getTable(tableReference));
      assertEquals(tableReference.getTableId() + "_desc", tableDestination.getTableDescription());

      List<TableRow> expectedRows = expectedRowsPerTable.get(tableDestination);
      assertThat(datasetService.getAllRows(tableReference.getProjectId(),
          tableReference.getDatasetId(), tableReference.getTableId()),
          containsInAnyOrder(Iterables.toArray(expectedRows, TableRow.class)));

      // Temp tables should be deleted.
      for (String tempTableJson : entry.getValue()) {
        TableReference tempTable = BigQueryHelpers.fromJsonString(
            tempTableJson, TableReference.class);
        assertEquals(null, datasetService.getTable(tempTable));
      }
    }
  }

  @Test
  public void testRemoveTemporaryTables() throws Exception {
    FakeDatasetService datasetService = new FakeDatasetService();
    String projectId = "project";
    String datasetId = "dataset";
    datasetService.createDataset(projectId, datasetId, "", "");
    List<TableReference> tableRefs = Lists.newArrayList(
        BigQueryHelpers.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, "table1")),
        BigQueryHelpers.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, "table2")),
        BigQueryHelpers.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, "table3")));
    for (TableReference tableRef : tableRefs) {
      datasetService.createTable(new Table().setTableReference(tableRef));
    }

    // Add one more table to delete that does not actually exist.
    tableRefs.add(
        BigQueryHelpers.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, "table4")));

    WriteRename.removeTemporaryTables(datasetService, tableRefs);

    for (TableReference ref : tableRefs) {
      loggedWriteRename.verifyDebug("Deleting table " + toJsonString(ref));
      checkState(datasetService.getTable(ref) == null,
          "Table " + ref + " was not deleted!");
    }
  }

  /** Test options. **/
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getInputTable();
    void setInputTable(ValueProvider<String> value);

    ValueProvider<String> getInputQuery();
    void setInputQuery(ValueProvider<String> value);

    ValueProvider<String> getOutputTable();
    void setOutputTable(ValueProvider<String> value);

    ValueProvider<String> getOutputSchema();
    void setOutputSchema(ValueProvider<String> value);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyInputTable() {
    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setTempLocation("gs://testbucket/testdir");
    Pipeline pipeline = TestPipeline.create(options);
    BigQueryIO.Read read = BigQueryIO.read().from(
        options.getInputTable()).withoutValidation();
    pipeline.apply(read);
    // Test that this doesn't throw.
    DisplayData.from(read);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyInputQuery() {
    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setTempLocation("gs://testbucket/testdir");
    Pipeline pipeline = TestPipeline.create(options);
    BigQueryIO.Read read = BigQueryIO.read().fromQuery(
        options.getInputQuery()).withoutValidation();
    pipeline.apply(read);
    // Test that this doesn't throw.
    DisplayData.from(read);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyOutput() {
    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setTempLocation("gs://testbucket/testdir");
    Pipeline pipeline = TestPipeline.create(options);
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows()
        .to(options.getOutputTable())
        .withSchema(NestedValueProvider.of(
            options.getOutputSchema(), new JsonSchemaToTableSchema()))
        .withoutValidation();
    pipeline
        .apply(Create.empty(TableRowJsonCoder.of()))
        .apply(write);
    // Test that this doesn't throw.
    DisplayData.from(write);
  }

  private static void testNumFiles(File tempDir, int expectedNumFiles) {
    assertEquals(expectedNumFiles, tempDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isFile();
      }}).length);
  }

  @Test
  public void testShardedKeyCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(ShardedKeyCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testTableRowInfoCoderSerializable() {
    CoderProperties.coderSerializable(TableRowInfoCoder.of());
  }

  @Test
  public void testComplexCoderSerializable() {
    CoderProperties.coderSerializable(
        WindowedValue.getFullCoder(
            KvCoder.of(
                ShardedKeyCoder.of(StringUtf8Coder.of()),
                TableRowInfoCoder.of()),
            IntervalWindow.getCoder()));
  }

  List<TableRow> convertBigDecimaslToLong(List<TableRow> toConvert) {
    // The numbers come back as BigDecimal objects after JSON serialization. Change them back to
    // longs so that we can assert the output.
    List<TableRow> converted = Lists.newArrayList();
    for (TableRow entry : toConvert) {
      TableRow convertedEntry = entry.clone();
      Object num = convertedEntry.get("number");
      if (num instanceof BigDecimal) {
        convertedEntry.set("number", ((BigDecimal) num).longValue());
      }
      converted.add(convertedEntry);
    }
    return converted;
  }
}
