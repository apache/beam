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
package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.BigQueryServices;
import com.google.cloud.dataflow.sdk.util.BigQueryServices.Status;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

/**
 * Tests for BigQueryIO.
 */
@RunWith(JUnit4.class)
public class BigQueryIOTest {

  private static class FakeBigQueryServices implements BigQueryServices {

    private Object[] startLoadJobReturns;
    private Object[] pollJobStatusReturns;

    /**
     * Sets the return values for the mock {@link LoadService#startLoadJob}.
     *
     * <p>Throws if the {@link Object} is a {@link Exception}, returns otherwise.
     */
    private FakeBigQueryServices startLoadJobReturns(Object... startLoadJobReturns) {
      this.startLoadJobReturns = startLoadJobReturns;
      return this;
    }

    /**
     * Sets the return values for the mock {@link LoadService#pollJobStatus}.
     *
     * <p>Throws if the {@link Object} is a {@link Exception}, returns otherwise.
     */
    private FakeBigQueryServices pollJobStatusReturns(Object... pollJobStatusReturns) {
      this.pollJobStatusReturns = pollJobStatusReturns;
      return this;
    }

    @Override
    public LoadService getLoadService(BigQueryOptions bqOptions) {
      return new FakeLoadService(startLoadJobReturns, pollJobStatusReturns);
    }

    private static class FakeLoadService implements BigQueryServices.LoadService {

      private Object[] startLoadJobReturns;
      private Object[] pollJobStatusReturns;
      private int startLoadJobCallsCount;
      private int pollJobStatusCallsCount;

      public FakeLoadService(Object[] startLoadJobReturns, Object[] pollJobStatusReturns) {
        this.startLoadJobReturns = startLoadJobReturns;
        this.pollJobStatusReturns = pollJobStatusReturns;
        this.startLoadJobCallsCount = 0;
        this.pollJobStatusCallsCount = 0;
      }

      @Override
      public void startLoadJob(String jobId, JobConfigurationLoad loadConfig)
          throws InterruptedException, IOException {
        if (startLoadJobCallsCount < startLoadJobReturns.length) {
          Object ret = startLoadJobReturns[startLoadJobCallsCount++];
          if (ret instanceof IOException) {
            throw (IOException) ret;
          } else if (ret instanceof InterruptedException) {
            throw (InterruptedException) ret;
          } else {
            return;
          }
        } else {
          throw new RuntimeException(
              "Exceeded expected number of calls: " + startLoadJobReturns.length);
        }
      }

      @Override
      public Status pollJobStatus(String projectId, String jobId) throws InterruptedException {
        if (pollJobStatusCallsCount < pollJobStatusReturns.length) {
          Object ret = pollJobStatusReturns[pollJobStatusCallsCount++];
          if (ret instanceof Status) {
            return (Status) ret;
          } else if (ret instanceof InterruptedException) {
            throw (InterruptedException) ret;
          } else {
            throw new RuntimeException("Unexpected return type: " + ret.getClass());
          }
        } else {
          throw new RuntimeException(
              "Exceeded expected number of calls: " + pollJobStatusReturns.length);
        }
      }
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigQueryIO.class);
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  @Mock
  public BigQueryServices.LoadService mockBqLoadService;

  private BigQueryOptions bqOptions;

  private void checkReadTableObject(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table) {
    checkReadTableObjectWithValidate(bound, project, dataset, table, true);
  }

  private void checkReadQueryObject(
      BigQueryIO.Read.Bound bound, String query) {
    checkReadQueryObjectWithValidate(bound, query, true);
  }

  private void checkReadTableObjectWithValidate(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table, boolean validate) {
    assertEquals(project, bound.table.getProjectId());
    assertEquals(dataset, bound.table.getDatasetId());
    assertEquals(table, bound.table.getTableId());
    assertNull(bound.query);
    assertEquals(validate, bound.getValidate());
  }

  private void checkReadQueryObjectWithValidate(
      BigQueryIO.Read.Bound bound, String query, boolean validate) {
    assertNull(bound.table);
    assertEquals(query, bound.query);
    assertEquals(validate, bound.getValidate());
  }

  private void checkWriteObject(
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition) {
    checkWriteObjectWithValidate(
        bound, project, dataset, table, schema, createDisposition, writeDisposition, true);
  }

  private void checkWriteObjectWithValidate(
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, boolean validate) {
    assertEquals(project, bound.getTable().getProjectId());
    assertEquals(dataset, bound.getTable().getDatasetId());
    assertEquals(table, bound.getTable().getTableId());
    assertEquals(schema, bound.getSchema());
    assertEquals(createDisposition, bound.createDisposition);
    assertEquals(writeDisposition, bound.writeDisposition);
    assertEquals(validate, bound.validate);
  }

  @Before
  public void setUp() {
    bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.getRoot().getAbsolutePath() + "/BigQueryIOTest/");

    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBuildTableBasedSource() {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("foo.com:project:somedataset.sometable");
    checkReadTableObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testBuildQueryBasedSource() {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyQuery")
        .fromQuery("foo_query");
    checkReadQueryObject(bound, "foo_query");
  }

  @Test
  public void testBuildTableBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("foo.com:project:somedataset.sometable").withoutValidation();
    checkReadTableObjectWithValidate(bound, "foo.com:project", "somedataset", "sometable", false);
  }

  @Test
  public void testBuildQueryBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .fromQuery("some_query").withoutValidation();
    checkReadQueryObjectWithValidate(bound, "some_query", false);
  }

  @Test
  public void testBuildTableBasedSourceWithDefaultProject() {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("somedataset.sometable");
    checkReadTableObject(bound, null, "somedataset", "sometable");
  }

  @Test
  public void testBuildSourceWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from(table);
    checkReadTableObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testValidateReadSetsDefaultProject() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setProject("someproject");

    Pipeline p = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId("somedataset");
    tableRef.setTableId("sometable");

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    try {
      p.apply(BigQueryIO.Read.named("ReadMyTable").from(tableRef));
    } finally {
      Assert.assertEquals("someproject", tableRef.getProjectId());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBuildSourceWithoutTableOrQuery() {
    Pipeline p = TestPipeline.create();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQuery read operation, either table reference or query has to be set");
    p.apply(BigQueryIO.Read.named("ReadMyTable"));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBuildSourceWithTableAndQuery() {
    Pipeline p = TestPipeline.create();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQuery read operation. Specifies both a query and a table, only one of these"
        + " should be provided");
    p.apply(
        BigQueryIO.Read.named("ReadMyTable")
            .from("foo.com:project:somedataset.sometable")
            .fromQuery("query"));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBuildSourceWithTableAndFlatten() {
    Pipeline p = TestPipeline.create();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQuery read operation. Specifies a"
              + " table with a result flattening preference, which is not configurable");
    p.apply(
        BigQueryIO.Read.named("ReadMyTable")
            .from("foo.com:project:somedataset.sometable")
            .withoutResultFlattening());
    p.run();
  }

  @Test
  public void testCustomSink() throws Exception {
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .startLoadJobReturns("done", "done", "done")
        .pollJobStatusReturns(Status.FAILED, Status.FAILED, Status.SUCCEEDED);

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3)))
    .setCoder(TableRowJsonCoder.of())
    .apply(BigQueryIO.Write.to("project-id:dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(new TableSchema().setFields(
            ImmutableList.of(
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("number").setType("INTEGER"))))
        .withTestServices(fakeBqServices)
        .withoutValidation());
    p.run();

    logged.verifyInfo("Starting BigQuery load job");
    logged.verifyInfo("Previous load jobs failed, retrying.");
    File tempDir = new File(bqOptions.getTempLocation());
    assertEquals(0, tempDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isFile();
      }}).length);
  }

  @Test
  public void testCustomSinkUnknown() throws Exception {
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .startLoadJobReturns("done", "done")
        .pollJobStatusReturns(Status.FAILED, Status.UNKNOWN);

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3)))
    .setCoder(TableRowJsonCoder.of())
    .apply(BigQueryIO.Write.to("project-id:dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withTestServices(fakeBqServices)
        .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to poll the load job status.");
    p.run();

    File tempDir = new File(bqOptions.getTempLocation());
    assertEquals(0, tempDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isFile();
      }}).length);
  }

  @Test
  public void testBuildSink() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable");
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkwithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable").withoutValidation();
    checkWriteObjectWithValidate(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, false);
  }

  @Test
  public void testBuildSinkDefaultProject() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("somedataset.sometable");
    checkWriteObject(
        bound, null, "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to(table);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBuildSinkWithoutTable() {
    Pipeline p = TestPipeline.create();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must set the table reference");
    p.apply(Create.<TableRow>of().withCoder(TableRowJsonCoder.of()))
        .apply(BigQueryIO.Write.named("WriteMyTable"));
  }

  @Test
  public void testBuildSinkWithSchema() {
    TableSchema schema = new TableSchema();
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable").withSchema(schema);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        schema, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithCreateDispositionNever() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_NEVER, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithCreateDispositionIfNeeded() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithWriteDispositionTruncate() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_TRUNCATE);
  }

  @Test
  public void testBuildSinkWithWriteDispositionAppend() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND);
  }

  @Test
  public void testBuildSinkWithWriteDispositionEmpty() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_EMPTY);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  private void testWriteValidatesDataset(boolean streaming) {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setProject("someproject");
    options.setStreaming(streaming);

    Pipeline p = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId("somedataset");
    tableRef.setTableId("sometable");

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    p.apply(Create.<TableRow>of().withCoder(TableRowJsonCoder.of()))
     .apply(BigQueryIO.Write.named("WriteMyTable")
         .to(tableRef)
         .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
         .withSchema(new TableSchema()));
  }

  @Test
  public void testWriteValidatesDatasetBatch() {
    testWriteValidatesDataset(false);
  }

  @Test
  public void testWriteValidatesDatasetStreaming() {
    testWriteValidatesDataset(true);
  }

  @Test
  public void testTableParsing() {
    TableReference ref = BigQueryIO
        .parseTableSpec("my-project:data_set.table_name");
    Assert.assertEquals("my-project", ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_validPatterns() {
    BigQueryIO.parseTableSpec("a123-456:foo_bar.d");
    BigQueryIO.parseTableSpec("a12345:b.c");
    BigQueryIO.parseTableSpec("b12345.c");
  }

  @Test
  public void testTableParsing_noProjectId() {
    TableReference ref = BigQueryIO
        .parseTableSpec("data_set.table_name");
    Assert.assertEquals(null, ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsingError() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("0123456:foo.bar");
  }

  @Test
  public void testTableParsingError_2() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("myproject:.bar");
  }

  @Test
  public void testTableParsingError_3() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec(":a.b");
  }

  @Test
  public void testTableParsingError_slash() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("a\\b12345:c.d");
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
    assertEquals("BigQueryIO.Read", BigQueryIO.Read.from("somedataset.sometable").getName());
    assertEquals("BigQueryIO.Write", BigQueryIO.Write.to("somedataset.sometable").getName());
    assertEquals("ReadMyTable", BigQueryIO.Read.named("ReadMyTable").getName());
    assertEquals("WriteMyTable", BigQueryIO.Write.named("WriteMyTable").getName());
  }
}
