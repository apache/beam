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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.fromJsonString;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.toJsonString;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics4;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Table.Cell;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.BigQueryQuerySource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.BigQueryTableSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.PassThroughThenCleanup;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.PassThroughThenCleanup.CleanupOperation;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TransformingSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.TableRowWriter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WritePartition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteRename;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteTables;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
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
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for BigQueryIO.
 */
@RunWith(JUnit4.class)
public class BigQueryIOTest implements Serializable {

  // Status.UNKNOWN maps to null
  private static final Map<Status, Job> JOB_STATUS_MAP = ImmutableMap.of(
      Status.SUCCEEDED, new Job().setStatus(new JobStatus()),
      Status.FAILED, new Job().setStatus(new JobStatus().setErrorResult(new ErrorProto())));


  private static class FakeBigQueryServices implements BigQueryServices {

    private String[] jsonTableRowReturns = new String[0];
    private JobService jobService;
    private DatasetService datasetService;

    public FakeBigQueryServices withJobService(JobService jobService) {
      this.jobService = jobService;
      return this;
    }

    public FakeBigQueryServices withDatasetService(DatasetService datasetService) {
      this.datasetService = datasetService;
      return this;
    }

    public FakeBigQueryServices readerReturns(String... jsonTableRowReturns) {
      this.jsonTableRowReturns = jsonTableRowReturns;
      return this;
    }

    @Override
    public JobService getJobService(BigQueryOptions bqOptions) {
      return jobService;
    }

    @Override
    public DatasetService getDatasetService(BigQueryOptions bqOptions) {
      return datasetService;
    }

    @Override
    public BigQueryJsonReader getReaderFromTable(
        BigQueryOptions bqOptions, TableReference tableRef) {
      return new FakeBigQueryReader(jsonTableRowReturns);
    }

    @Override
    public BigQueryJsonReader getReaderFromQuery(
        BigQueryOptions bqOptions, String query, String projectId, @Nullable Boolean flatten,
        @Nullable Boolean useLegacySql) {
      return new FakeBigQueryReader(jsonTableRowReturns);
    }

    private static class FakeBigQueryReader implements BigQueryJsonReader {
      private static final int UNSTARTED = -1;
      private static final int CLOSED = Integer.MAX_VALUE;

      private String[] jsonTableRowReturns;
      private int currIndex;

      FakeBigQueryReader(String[] jsonTableRowReturns) {
        this.jsonTableRowReturns = jsonTableRowReturns;
        this.currIndex = UNSTARTED;
      }

      @Override
      public boolean start() throws IOException {
        assertEquals(UNSTARTED, currIndex);
        currIndex = 0;
        return currIndex < jsonTableRowReturns.length;
      }

      @Override
      public boolean advance() throws IOException {
        return ++currIndex < jsonTableRowReturns.length;
      }

      @Override
      public TableRow getCurrent() throws NoSuchElementException {
        if (currIndex >= jsonTableRowReturns.length) {
          throw new NoSuchElementException();
        }
        return fromJsonString(jsonTableRowReturns[currIndex], TableRow.class);
      }

      @Override
      public void close() throws IOException {
        currIndex = CLOSED;
      }
    }
  }

  private static class FakeJobService implements JobService, Serializable {

    private Object[] startJobReturns;
    private Object[] pollJobReturns;
    private Object[] getJobReturns;
    private String executingProject;
    // Both counts will be reset back to zeros after serialization.
    // This is a work around for DoFn's verifyUnmodified check.
    private transient int startJobCallsCount;
    private transient int pollJobStatusCallsCount;
    private transient int getJobCallsCount;

    public FakeJobService() {
      this.startJobReturns = new Object[0];
      this.pollJobReturns = new Object[0];
      this.getJobReturns = new Object[0];
      this.startJobCallsCount = 0;
      this.pollJobStatusCallsCount = 0;
      this.getJobCallsCount = 0;
    }

    /**
     * Sets the return values to mock {@link JobService#startLoadJob},
     * {@link JobService#startExtractJob} and {@link JobService#startQueryJob}.
     *
     * <p>Throws if the {@link Object} is a {@link Exception}, returns otherwise.
     */
    public FakeJobService startJobReturns(Object... startJobReturns) {
      this.startJobReturns = startJobReturns;
      return this;
    }

    /**
     * Sets the return values to mock {@link JobService#getJob}.
     *
     * <p>Throws if the {@link Object} is a {@link InterruptedException}, returns otherwise.
     */
    public FakeJobService getJobReturns(Object... getJobReturns) {
      this.getJobReturns = getJobReturns;
      return this;
    }

    /**
     * Sets the return values to mock {@link JobService#pollJob}.
     *
     * <p>Throws if the {@link Object} is a {@link Exception}, returns otherwise.
     */
    public FakeJobService pollJobReturns(Object... pollJobReturns) {
      this.pollJobReturns = pollJobReturns;
      return this;
    }

    /**
     * Verifies executing project.
     */
    public FakeJobService verifyExecutingProject(String executingProject) {
      this.executingProject = executingProject;
      return this;
    }

    @Override
    public void startLoadJob(JobReference jobRef, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException {
      startJob(jobRef, loadConfig);
    }

    @Override
    public void startExtractJob(JobReference jobRef, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException {
      startJob(jobRef, extractConfig);
    }

    @Override
    public void startQueryJob(JobReference jobRef, JobConfigurationQuery query)
        throws IOException, InterruptedException {
      startJob(jobRef, query);
    }

    @Override
    public void startCopyJob(JobReference jobRef, JobConfigurationTableCopy copyConfig)
        throws IOException, InterruptedException {
      startJob(jobRef, copyConfig);
    }

    @Override
    public Job pollJob(JobReference jobRef, int maxAttempts)
        throws InterruptedException {
      if (!Strings.isNullOrEmpty(executingProject)) {
        checkArgument(
            jobRef.getProjectId().equals(executingProject),
            "Project id: %s is not equal to executing project: %s",
            jobRef.getProjectId(), executingProject);
      }

      if (pollJobStatusCallsCount < pollJobReturns.length) {
        Object ret = pollJobReturns[pollJobStatusCallsCount++];
        if (ret instanceof Job) {
          return (Job) ret;
        } else if (ret instanceof Status) {
          return JOB_STATUS_MAP.get(ret);
        } else if (ret instanceof InterruptedException) {
          throw (InterruptedException) ret;
        } else {
          throw new RuntimeException("Unexpected return type: " + ret.getClass());
        }
      } else {
        throw new RuntimeException(
            "Exceeded expected number of calls: " + pollJobReturns.length);
      }
    }

    private void startJob(JobReference jobRef, GenericJson config)
        throws IOException, InterruptedException {
      if (!Strings.isNullOrEmpty(executingProject)) {
        checkArgument(
            jobRef.getProjectId().equals(executingProject),
            "Project id: %s is not equal to executing project: %s",
            jobRef.getProjectId(), executingProject);
      }

      if (startJobCallsCount < startJobReturns.length) {
        Object ret = startJobReturns[startJobCallsCount++];
        if (ret instanceof IOException) {
          throw (IOException) ret;
        } else if (ret instanceof InterruptedException) {
          throw (InterruptedException) ret;
        } else if (ret instanceof SerializableFunction) {
          SerializableFunction<GenericJson, Void> fn =
              (SerializableFunction<GenericJson, Void>) ret;
          fn.apply(config);
          return;
        } else {
          return;
        }
      } else {
        throw new RuntimeException(
            "Exceeded expected number of calls: " + startJobReturns.length);
      }
    }

    @Override
    public JobStatistics dryRunQuery(String projectId, JobConfigurationQuery query)
        throws InterruptedException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Job getJob(JobReference jobRef) throws InterruptedException {
      if (!Strings.isNullOrEmpty(executingProject)) {
        checkArgument(
            jobRef.getProjectId().equals(executingProject),
            "Project id: %s is not equal to executing project: %s",
            jobRef.getProjectId(), executingProject);
      }

      if (getJobCallsCount < getJobReturns.length) {
        Object ret = getJobReturns[getJobCallsCount++];
        if (ret == null) {
          return null;
        } else if (ret instanceof Job) {
          return (Job) ret;
        } else if (ret instanceof InterruptedException) {
          throw (InterruptedException) ret;
        } else {
          throw new RuntimeException("Unexpected return type: " + ret.getClass());
        }
      } else {
        throw new RuntimeException(
            "Exceeded expected number of calls: " + getJobReturns.length);
      }
    }

    ////////////////////////////////// SERIALIZATION METHODS ////////////////////////////////////
    private void writeObject(ObjectOutputStream out) throws IOException {
      out.writeObject(replaceJobsWithBytes(startJobReturns));
      out.writeObject(replaceJobsWithBytes(pollJobReturns));
      out.writeObject(replaceJobsWithBytes(getJobReturns));
      out.writeObject(executingProject);
    }

    private Object[] replaceJobsWithBytes(Object[] objs) {
      Object[] copy = Arrays.copyOf(objs, objs.length);
      for (int i = 0; i < copy.length; i++) {
        checkArgument(
            copy[i] == null || copy[i] instanceof Serializable || copy[i] instanceof Job,
            "Only serializable elements and jobs can be added add to Job Returns");
        if (copy[i] instanceof Job) {
          try {
            // Job is not serializable, so encode the job as a byte array.
            copy[i] = Transport.getJsonFactory().toByteArray(copy[i]);
          } catch (IOException e) {
            throw new IllegalArgumentException(
                String.format("Could not encode Job %s via available JSON factory", copy[i]));
          }
        }
      }
      return copy;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      this.startJobReturns = replaceBytesWithJobs(in.readObject());
      this.pollJobReturns = replaceBytesWithJobs(in.readObject());
      this.getJobReturns = replaceBytesWithJobs(in.readObject());
      this.executingProject = (String) in.readObject();
    }

    private Object[] replaceBytesWithJobs(Object obj) throws IOException {
      checkState(obj instanceof Object[]);
      Object[] objs = (Object[]) obj;
      Object[] copy = Arrays.copyOf(objs, objs.length);
      for (int i = 0; i < copy.length; i++) {
        if (copy[i] instanceof byte[]) {
          Job job = Transport.getJsonFactory()
              .createJsonParser(new ByteArrayInputStream((byte[]) copy[i]))
              .parse(Job.class);
          copy[i] = job;
        }
      }
      return copy;
    }
  }

  /** A fake dataset service that can be serialized, for use in testReadFromTable. */
  private static class FakeDatasetService implements DatasetService, Serializable {
    private com.google.common.collect.Table<String, String, Map<String, Table>> tables =
        HashBasedTable.create();

    public FakeDatasetService withTable(
        String projectId, String datasetId, String tableId, Table table) throws IOException {
      Map<String, Table> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        dataset = new HashMap<>();
        tables.put(projectId, datasetId, dataset);
      }
      dataset.put(tableId, table);
      return this;
    }

    @Override
    public Table getTable(String projectId, String datasetId, String tableId)
        throws InterruptedException, IOException {
      Map<String, Table> dataset =
          checkNotNull(
              tables.get(projectId, datasetId),
              "Tried to get a table %s:%s.%s from %s, but no such table was set",
              projectId,
              datasetId,
              tableId,
              FakeDatasetService.class.getSimpleName());
      return checkNotNull(dataset.get(tableId),
          "Tried to get a table %s:%s.%s from %s, but no such table was set",
          projectId,
          datasetId,
          tableId,
          FakeDatasetService.class.getSimpleName());
    }

    @Override
    public void deleteTable(String projectId, String datasetId, String tableId)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean isTableEmpty(String projectId, String datasetId, String tableId)
        throws IOException, InterruptedException {
      Long numBytes = getTable(projectId, datasetId, tableId).getNumBytes();
      return numBytes == null || numBytes == 0L;
    }

    @Override
    public Dataset getDataset(
        String projectId, String datasetId) throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void createDataset(
        String projectId, String datasetId, String location, String description)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void deleteDataset(String projectId, String datasetId)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public long insertAll(
        TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }

    ////////////////////////////////// SERIALIZATION METHODS ////////////////////////////////////
    private void writeObject(ObjectOutputStream out) throws IOException {
      out.writeObject(replaceTablesWithBytes(this.tables));
    }

    private com.google.common.collect.Table<String, String, Map<String, byte[]>>
    replaceTablesWithBytes(
        com.google.common.collect.Table<String, String, Map<String, Table>> toCopy)
        throws IOException {
      com.google.common.collect.Table<String, String, Map<String, byte[]>> copy =
          HashBasedTable.create();
      for (Cell<String, String, Map<String, Table>> cell : toCopy.cellSet()) {
        HashMap<String, byte[]> dataset = new HashMap<>();
        copy.put(cell.getRowKey(), cell.getColumnKey(), dataset);
        for (Map.Entry<String, Table> dsTables : cell.getValue().entrySet()) {
          dataset.put(
              dsTables.getKey(), Transport.getJsonFactory().toByteArray(dsTables.getValue()));
        }
      }
      return copy;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      com.google.common.collect.Table<String, String, Map<String, byte[]>> tablesTable =
          (com.google.common.collect.Table<String, String, Map<String, byte[]>>) in.readObject();
      this.tables = replaceBytesWithTables(tablesTable);
    }

    private com.google.common.collect.Table<String, String, Map<String, Table>>
    replaceBytesWithTables(
        com.google.common.collect.Table<String, String, Map<String, byte[]>> tablesTable)
        throws IOException {
      com.google.common.collect.Table<String, String, Map<String, Table>> copy =
          HashBasedTable.create();
      for (Cell<String, String, Map<String, byte[]>> cell : tablesTable.cellSet()) {
        HashMap<String, Table> dataset = new HashMap<>();
        copy.put(cell.getRowKey(), cell.getColumnKey(), dataset);
        for (Map.Entry<String, byte[]> dsTables : cell.getValue().entrySet()) {
          Table table =
              Transport.getJsonFactory()
                  .createJsonParser(new ByteArrayInputStream(dsTables.getValue()))
                  .parse(Table.class);
          dataset.put(dsTables.getKey(), table);
        }
      }
      return copy;
    }
  }

  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient ExpectedLogs logged = ExpectedLogs.none(BigQueryIO.class);
  @Rule public transient TemporaryFolder testFolder = new TemporaryFolder();
  @Mock(extraInterfaces = Serializable.class)
  public transient BigQueryServices.JobService mockJobService;
  @Mock private transient IOChannelFactory mockIOChannelFactory;
  @Mock(extraInterfaces = Serializable.class) private transient DatasetService mockDatasetService;

  private void checkReadTableObject(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table) {
    checkReadTableObjectWithValidate(bound, project, dataset, table, true);
  }

  private void checkReadQueryObject(BigQueryIO.Read.Bound bound, String query) {
    checkReadQueryObjectWithValidate(bound, query, true);
  }

  private void checkReadTableObjectWithValidate(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table, boolean validate) {
    assertEquals(project, bound.getTable().getProjectId());
    assertEquals(dataset, bound.getTable().getDatasetId());
    assertEquals(table, bound.getTable().getTableId());
    assertNull(bound.query);
    assertEquals(validate, bound.getValidate());
  }

  private void checkReadQueryObjectWithValidate(
      BigQueryIO.Read.Bound bound, String query, boolean validate) {
    assertNull(bound.getTable());
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
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBuildTableBasedSource() {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.from("foo.com:project:somedataset.sometable");
    checkReadTableObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testBuildQueryBasedSource() {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.fromQuery("foo_query");
    checkReadQueryObject(bound, "foo_query");
  }

  @Test
  public void testBuildTableBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read.Bound bound =
        BigQueryIO.Read.from("foo.com:project:somedataset.sometable").withoutValidation();
    checkReadTableObjectWithValidate(bound, "foo.com:project", "somedataset", "sometable", false);
  }

  @Test
  public void testBuildQueryBasedSourceWithoutValidation() {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read.Bound bound =
        BigQueryIO.Read.fromQuery("some_query").withoutValidation();
    checkReadQueryObjectWithValidate(bound, "some_query", false);
  }

  @Test
  public void testBuildTableBasedSourceWithDefaultProject() {
    BigQueryIO.Read.Bound bound =
        BigQueryIO.Read.from("somedataset.sometable");
    checkReadTableObject(bound, null, "somedataset", "sometable");
  }

  @Test
  public void testBuildSourceWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.from(table);
    checkReadTableObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testValidateReadSetsDefaultProject() throws Exception {
    String projectId = "someproject";
    String datasetId = "somedataset";
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject(projectId);
    bqOptions.setTempLocation("gs://testbucket/testdir");

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .withDatasetService(mockDatasetService);
    when(mockDatasetService.getDataset(projectId, datasetId)).thenThrow(
        new RuntimeException("Unable to confirm BigQuery dataset presence"));

    Pipeline p = TestPipeline.create(bqOptions);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId("sometable");

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    p.apply(BigQueryIO.Read.from(tableRef)
        .withTestServices(fakeBqServices));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildSourceWithoutTableQueryOrValidation() {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: one of table reference and query must be set");
    p.apply(BigQueryIO.Read.withoutValidation());
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildSourceWithTableAndQuery() {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: table reference and query may not both be set");
    p.apply("ReadMyTable",
        BigQueryIO.Read
            .from("foo.com:project:somedataset.sometable")
            .fromQuery("query"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildSourceWithTableAndFlatten() {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a result flattening preference,"
            + " which only applies to queries");
    p.apply("ReadMyTable",
        BigQueryIO.Read
            .from("foo.com:project:somedataset.sometable")
            .withoutResultFlattening());
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildSourceWithTableAndFlattenWithoutValidation() {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a result flattening preference,"
            + " which only applies to queries");
    p.apply(
        BigQueryIO.Read
            .from("foo.com:project:somedataset.sometable")
            .withoutValidation()
            .withoutResultFlattening());
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildSourceWithTableAndSqlDialect() {
    BigQueryOptions bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation("gs://testbucket/testdir");

    Pipeline p = TestPipeline.create(bqOptions);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a SQL dialect preference,"
            + " which only applies to queries");
    p.apply(
        BigQueryIO.Read
            .from("foo.com:project:somedataset.sometable")
            .usingStandardSql());
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadFromTable() throws IOException {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
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
    sometable.setNumBytes(1024L * 1024L);
    FakeDatasetService fakeDatasetService =
        new FakeDatasetService()
            .withTable("non-executing-project", "somedataset", "sometable", sometable);
    SerializableFunction<Void, Schema> schemaGenerator =
        new SerializableFunction<Void, Schema>() {
          @Override
          public Schema apply(Void input) {
            return BigQueryAvroUtils.toGenericAvroSchema(
                "sometable",
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));
          }
        };
    Collection<Map<String, Object>> records =
        ImmutableList.<Map<String, Object>>builder()
            .add(ImmutableMap.<String, Object>builder().put("name", "a").put("number", 1L).build())
            .add(ImmutableMap.<String, Object>builder().put("name", "b").put("number", 2L).build())
            .add(ImmutableMap.<String, Object>builder().put("name", "c").put("number", 3L).build())
            .build();

    SerializableFunction<GenericJson, Void> onStartJob =
        new WriteExtractFiles(schemaGenerator, records);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns(onStartJob, "done")
            .pollJobReturns(job)
            .getJobReturns((Job) null)
            .verifyExecutingProject(bqOptions.getProject()))
        .withDatasetService(fakeDatasetService)
        .readerReturns(
            toJsonString(new TableRow().set("name", "a").set("number", 1)),
            toJsonString(new TableRow().set("name", "b").set("number", 2)),
            toJsonString(new TableRow().set("name", "c").set("number", 3)));

    Pipeline p = TestPipeline.create(bqOptions);
    PCollection<String> output = p
        .apply(BigQueryIO.Read.from("non-executing-project:somedataset.sometable")
            .withTestServices(fakeBqServices)
            .withoutValidation())
        .apply(ParDo.of(new DoFn<TableRow, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output((String) c.element().get("name"));
          }
        }));

    PAssert.that(output)
        .containsInAnyOrder(ImmutableList.of("a", "b", "c"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns("done", "done", "done")
            .pollJobReturns(Status.FAILED, Status.FAILED, Status.SUCCEEDED));

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
    .apply(BigQueryIO.Write.to("dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(new TableSchema().setFields(
            ImmutableList.of(
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("number").setType("INTEGER"))))
        .withTestServices(fakeBqServices)
        .withoutValidation());
    p.run();

    logged.verifyInfo("Starting BigQuery load job");
    logged.verifyInfo("BigQuery load job failed");
    logged.verifyInfo("try 0/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyInfo("try 1/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyInfo("try 2/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyNotLogged("try 3/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 0);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteUnknown() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns("done", "done")
            .pollJobReturns(Status.FAILED, Status.UNKNOWN));

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
    .apply(BigQueryIO.Write.to("project-id:dataset-id.table-id")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withTestServices(fakeBqServices)
        .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to poll the load job status");
    p.run();

    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 0);
  }

  @Test
  public void testBuildSourceDisplayData() {
    String tableSpec = "project:dataset.tableid";

    BigQueryIO.Read.Bound read = BigQueryIO.Read
        .from(tableSpec)
        .fromQuery("myQuery")
        .withoutResultFlattening()
        .usingStandardSql()
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("table", tableSpec));
    assertThat(displayData, hasDisplayItem("query", "myQuery"));
    assertThat(displayData, hasDisplayItem("flattenResults", false));
    assertThat(displayData, hasDisplayItem("useLegacySql", false));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testTableSourcePrimitiveDisplayData() throws IOException, InterruptedException {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigQueryIO.Read.Bound read = BigQueryIO.Read
        .from("project:dataset.tableId")
        .withTestServices(new FakeBigQueryServices()
            .withDatasetService(mockDatasetService)
            .withJobService(mockJobService))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("BigQueryIO.Read should include the table spec in its primitive display data",
        displayData, hasItem(hasDisplayItem("table")));
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testQuerySourcePrimitiveDisplayData() throws IOException, InterruptedException {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigQueryIO.Read.Bound read = BigQueryIO.Read
        .fromQuery("foobar")
        .withTestServices(new FakeBigQueryServices()
            .withDatasetService(mockDatasetService)
            .withJobService(mockJobService))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("BigQueryIO.Read should include the query in its primitive display data",
        displayData, hasItem(hasDisplayItem("query")));
  }


  @Test
  public void testBuildWrite() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.to("foo.com:project:somedataset.sometable");
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testBatchWritePrimitiveDisplayData() throws IOException, InterruptedException {
    testWritePrimitiveDisplayData(/* streaming: */ false);
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testStreamingWritePrimitiveDisplayData() throws IOException, InterruptedException {
    testWritePrimitiveDisplayData(/* streaming: */ true);
  }

  private void testWritePrimitiveDisplayData(boolean streaming) throws IOException,
      InterruptedException {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(streaming);
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create(options);

    BigQueryIO.Write.Bound write = BigQueryIO.Write
        .to("project:dataset.table")
        .withSchema(new TableSchema().set("col1", "type1").set("col2", "type2"))
        .withTestServices(new FakeBigQueryServices()
          .withDatasetService(mockDatasetService)
          .withJobService(mockJobService))
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
    BigQueryIO.Write.Bound bound =
        BigQueryIO.Write.to("foo.com:project:somedataset.sometable").withoutValidation();
    checkWriteObjectWithValidate(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, false);
  }

  @Test
  public void testBuildWriteDefaultProject() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.to("somedataset.sometable");
    checkWriteObject(
        bound, null, "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildWriteWithTableReference() {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.to(table);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildWriteWithoutTable() {
    Pipeline p = TestPipeline.create();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must set the table reference");
    p.apply(Create.<TableRow>of().withCoder(TableRowJsonCoder.of()))
        .apply(BigQueryIO.Write.withoutValidation());
  }

  @Test
  public void testBuildWriteWithSchema() {
    TableSchema schema = new TableSchema();
    BigQueryIO.Write.Bound bound =
        BigQueryIO.Write.to("foo.com:project:somedataset.sometable").withSchema(schema);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        schema, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildWriteWithCreateDispositionNever() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_NEVER, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildWriteWithCreateDispositionIfNeeded() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildWriteWithWriteDispositionTruncate() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_TRUNCATE);
  }

  @Test
  public void testBuildWriteWithWriteDispositionAppend() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND);
  }

  @Test
  public void testBuildWriteWithWriteDispositionEmpty() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_EMPTY);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildWriteDisplayData() {
    String tableSpec = "project:dataset.table";
    TableSchema schema = new TableSchema().set("col1", "type1").set("col2", "type2");

    BigQueryIO.Write.Bound write = BigQueryIO.Write
        .to(tableSpec)
        .withSchema(schema)
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("table"));
    assertThat(displayData, hasDisplayItem("schema"));
    assertThat(displayData,
        hasDisplayItem("createDisposition", CreateDisposition.CREATE_IF_NEEDED.toString()));
    assertThat(displayData,
        hasDisplayItem("writeDisposition", WriteDisposition.WRITE_APPEND.toString()));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  private void testWriteValidatesDataset(boolean unbounded) throws Exception {
    String projectId = "someproject";
    String datasetId = "somedataset";

    BigQueryOptions options = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    options.setProject(projectId);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .withDatasetService(mockDatasetService);
    when(mockDatasetService.getDataset(projectId, datasetId)).thenThrow(
        new RuntimeException("Unable to confirm BigQuery dataset presence"));

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
      tableRows = p.apply(Create.<TableRow>of().withCoder(TableRowJsonCoder.of()));
    }

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    tableRows
        .apply(
            BigQueryIO.Write.to(tableRef)
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
  }

  @Test
  public void testWriteValidateFailsCreateNoSchema() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("no schema was provided");
    TestPipeline.create()
        .apply(Create.<TableRow>of())
        .apply(BigQueryIO.Write
            .to("dataset.table")
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsTableAndTableSpec() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Cannot set both a table reference and a table function");
    TestPipeline.create()
        .apply(Create.<TableRow>of())
        .apply(BigQueryIO.Write
            .to("dataset.table")
            .to(new SerializableFunction<BoundedWindow, String>() {
              @Override
              public String apply(BoundedWindow input) {
                return null;
              }
            }));
  }

  @Test
  public void testWriteValidateFailsNoTableAndNoTableSpec() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must set the table reference of a BigQueryIO.Write transform");
    TestPipeline.create()
        .apply(Create.<TableRow>of())
        .apply("name", BigQueryIO.Write.withoutValidation());
  }

  @Test
  public void testBigQueryTableSourceThroughJsonAPI() throws Exception {
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .readerReturns(
            toJsonString(new TableRow().set("name", "a").set("number", "1")),
            toJsonString(new TableRow().set("name", "b").set("number", "2")),
            toJsonString(new TableRow().set("name", "c").set("number", "3")));

    String jobIdToken = "testJobIdToken";
    TableReference table = BigQueryIO.parseTableSpec("project.data_set.table_name");
    String extractDestinationDir = "mock://tempLocation";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        jobIdToken, table, extractDestinationDir, fakeBqServices, "project");

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", "1"),
        new TableRow().set("name", "b").set("number", "2"),
        new TableRow().set("name", "c").set("number", "3"));

    PipelineOptions options = PipelineOptionsFactory.create();
    Assert.assertThat(
        SourceTestUtils.readFromSource(bqSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);
  }

  @Test
  public void testBigQueryTableSourceInitSplit() throws Exception {
    Job extractJob = new Job();
    JobStatistics jobStats = new JobStatistics();
    JobStatistics4 extractStats = new JobStatistics4();
    extractStats.setDestinationUriFileCounts(ImmutableList.of(1L));
    jobStats.setExtract(extractStats);
    extractJob.setStatus(new JobStatus())
        .setStatistics(jobStats);

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .withDatasetService(mockDatasetService)
        .readerReturns(
            toJsonString(new TableRow().set("name", "a").set("number", "1")),
            toJsonString(new TableRow().set("name", "b").set("number", "2")),
            toJsonString(new TableRow().set("name", "c").set("number", "3")));

    String jobIdToken = "testJobIdToken";
    TableReference table = BigQueryIO.parseTableSpec("project:data_set.table_name");
    String extractDestinationDir = "mock://tempLocation";
    BoundedSource<TableRow> bqSource = BigQueryTableSource.create(
        jobIdToken, table, extractDestinationDir, fakeBqServices, "project");

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", "1"),
        new TableRow().set("name", "b").set("number", "2"),
        new TableRow().set("name", "c").set("number", "3"));

    when(mockJobService.pollJob(Mockito.<JobReference>any(), Mockito.anyInt()))
        .thenReturn(extractJob);
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation("mock://tempLocation");

    IOChannelUtils.setIOFactoryInternal("mock", mockIOChannelFactory, true /* override */);
    when(mockIOChannelFactory.resolve(anyString(), anyString()))
        .thenReturn("mock://tempLocation/output");
    when(mockDatasetService.getTable(anyString(), anyString(), anyString()))
        .thenReturn(new Table().setSchema(new TableSchema()));

    Assert.assertThat(
        SourceTestUtils.readFromSource(bqSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.splitIntoBundles(100, options);
    assertEquals(1, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));

    Mockito.verify(mockJobService)
        .startExtractJob(Mockito.<JobReference>any(), Mockito.<JobConfigurationExtract>any());
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

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .withDatasetService(mockDatasetService)
        .readerReturns(
            toJsonString(new TableRow().set("name", "a").set("number", "1")),
            toJsonString(new TableRow().set("name", "b").set("number", "2")),
            toJsonString(new TableRow().set("name", "c").set("number", "3")));

    String jobIdToken = "testJobIdToken";
    String extractDestinationDir = "mock://tempLocation";
    TableReference destinationTable = BigQueryIO.parseTableSpec("project:data_set.table_name");
    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        jobIdToken, "query", destinationTable, true /* flattenResults */, true /* useLegacySql */,
        extractDestinationDir, fakeBqServices);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", "1"),
        new TableRow().set("name", "b").set("number", "2"),
        new TableRow().set("name", "c").set("number", "3"));

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(extractDestinationDir);

    TableReference queryTable = new TableReference()
        .setProjectId("testProejct")
        .setDatasetId("testDataset")
        .setTableId("testTable");
    when(mockJobService.dryRunQuery(anyString(), Mockito.<JobConfigurationQuery>any()))
        .thenReturn(new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)
                .setReferencedTables(ImmutableList.of(queryTable))));
    when(mockDatasetService.getTable(
        eq(queryTable.getProjectId()), eq(queryTable.getDatasetId()), eq(queryTable.getTableId())))
        .thenReturn(new Table().setSchema(new TableSchema()));
    when(mockDatasetService.getTable(
        eq(destinationTable.getProjectId()),
        eq(destinationTable.getDatasetId()),
        eq(destinationTable.getTableId())))
        .thenReturn(new Table().setSchema(new TableSchema()));
    IOChannelUtils.setIOFactoryInternal("mock", mockIOChannelFactory, true /* override */);
    when(mockIOChannelFactory.resolve(anyString(), anyString()))
        .thenReturn("mock://tempLocation/output");
    when(mockJobService.pollJob(Mockito.<JobReference>any(), Mockito.anyInt()))
        .thenReturn(extractJob);

    Assert.assertThat(
        SourceTestUtils.readFromSource(bqSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.splitIntoBundles(100, options);
    assertEquals(1, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));

    Mockito.verify(mockJobService)
        .startQueryJob(
            Mockito.<JobReference>any(), Mockito.<JobConfigurationQuery>any());
    Mockito.verify(mockJobService)
        .startExtractJob(Mockito.<JobReference>any(), Mockito.<JobConfigurationExtract>any());
    Mockito.verify(mockDatasetService)
        .createDataset(anyString(), anyString(), anyString(), anyString());
    ArgumentCaptor<JobConfigurationQuery> queryConfigArg =
        ArgumentCaptor.forClass(JobConfigurationQuery.class);
    Mockito.verify(mockJobService).dryRunQuery(anyString(), queryConfigArg.capture());
    assertEquals(true, queryConfigArg.getValue().getFlattenResults());
    assertEquals(true, queryConfigArg.getValue().getUseLegacySql());
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

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(mockJobService)
        .withDatasetService(mockDatasetService)
        .readerReturns(
            toJsonString(new TableRow().set("name", "a").set("number", "1")),
            toJsonString(new TableRow().set("name", "b").set("number", "2")),
            toJsonString(new TableRow().set("name", "c").set("number", "3")));

    String jobIdToken = "testJobIdToken";
    String extractDestinationDir = "mock://tempLocation";
    TableReference destinationTable = BigQueryIO.parseTableSpec("project:data_set.table_name");
    BoundedSource<TableRow> bqSource = BigQueryQuerySource.create(
        jobIdToken, "query", destinationTable, true /* flattenResults */, true /* useLegacySql */,
        extractDestinationDir, fakeBqServices);

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", "1"),
        new TableRow().set("name", "b").set("number", "2"),
        new TableRow().set("name", "c").set("number", "3"));

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(extractDestinationDir);

    when(mockJobService.dryRunQuery(anyString(), Mockito.<JobConfigurationQuery>any()))
        .thenReturn(new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(100L)));
    when(mockDatasetService.getTable(
        eq(destinationTable.getProjectId()),
        eq(destinationTable.getDatasetId()),
        eq(destinationTable.getTableId())))
        .thenReturn(new Table().setSchema(new TableSchema()));
    IOChannelUtils.setIOFactoryInternal("mock", mockIOChannelFactory, true /* override */);
    when(mockIOChannelFactory.resolve(anyString(), anyString()))
        .thenReturn("mock://tempLocation/output");
    when(mockJobService.pollJob(Mockito.<JobReference>any(), Mockito.anyInt()))
        .thenReturn(extractJob);

    Assert.assertThat(
        SourceTestUtils.readFromSource(bqSource, options),
        CoreMatchers.is(expected));
    SourceTestUtils.assertSplitAtFractionBehavior(
        bqSource, 2, 0.3, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);

    List<? extends BoundedSource<TableRow>> sources = bqSource.splitIntoBundles(100, options);
    assertEquals(1, sources.size());
    BoundedSource<TableRow> actual = sources.get(0);
    assertThat(actual, CoreMatchers.instanceOf(TransformingSource.class));

    Mockito.verify(mockJobService)
        .startQueryJob(
            Mockito.<JobReference>any(), Mockito.<JobConfigurationQuery>any());
    Mockito.verify(mockJobService)
        .startExtractJob(Mockito.<JobReference>any(), Mockito.<JobConfigurationExtract>any());
    Mockito.verify(mockDatasetService)
        .createDataset(anyString(), anyString(), anyString(), anyString());
    ArgumentCaptor<JobConfigurationQuery> queryConfigArg =
        ArgumentCaptor.forClass(JobConfigurationQuery.class);
    Mockito.verify(mockJobService).dryRunQuery(anyString(), queryConfigArg.capture());
    assertEquals(true, queryConfigArg.getValue().getFlattenResults());
    assertEquals(true, queryConfigArg.getValue().getUseLegacySql());
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
        stringSource, stringSource.splitIntoBundles(100, options), options);
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
        stringSource, stringSource.splitIntoBundles(100, options), options);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPassThroughThenCleanup() throws Exception {
    Pipeline p = TestPipeline.create();

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
  @Category(NeedsRunner.class)
  public void testPassThroughThenCleanupExecuted() throws Exception {
    Pipeline p = TestPipeline.create();

    p.apply(Create.<Integer>of())
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
    testWritePartition(numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionSinglePartition() throws Exception {
    long numFiles = BigQueryIO.Write.Bound.MAX_NUM_FILES;
    long fileSize = 1;

    // One partition is needed.
    long expectedNumPartitions = 1;
    testWritePartition(numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionManyFiles() throws Exception {
    long numFiles = BigQueryIO.Write.Bound.MAX_NUM_FILES * 3;
    long fileSize = 1;

    // One partition is needed for each group of BigQueryWrite.MAX_NUM_FILES files.
    long expectedNumPartitions = 3;
    testWritePartition(numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionLargeFileSize() throws Exception {
    long numFiles = 10;
    long fileSize = BigQueryIO.Write.Bound.MAX_SIZE_BYTES / 3;

    // One partition is needed for each group of three files.
    long expectedNumPartitions = 4;
    testWritePartition(numFiles, fileSize, expectedNumPartitions);
  }

  private void testWritePartition(long numFiles, long fileSize, long expectedNumPartitions)
      throws Exception {
    List<Long> expectedPartitionIds = Lists.newArrayList();
    for (long i = 1; i <= expectedNumPartitions; ++i) {
      expectedPartitionIds.add(i);
    }

    List<KV<String, Long>> files = Lists.newArrayList();
    List<String> fileNames = Lists.newArrayList();
    for (int i = 0; i < numFiles; ++i) {
      String fileName = String.format("files%05d", i);
      fileNames.add(fileName);
      files.add(KV.of(fileName, fileSize));
    }

    TupleTag<KV<Long, List<String>>> multiPartitionsTag =
        new TupleTag<KV<Long, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<Long, List<String>>> singlePartitionTag =
        new TupleTag<KV<Long, List<String>>>("singlePartitionTag") {};

    PCollectionView<Iterable<KV<String, Long>>> filesView = PCollectionViews.iterableView(
        TestPipeline.create(),
        WindowingStrategy.globalDefault(),
        KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    WritePartition writePartition =
        new WritePartition(filesView, multiPartitionsTag, singlePartitionTag);

    DoFnTester<String, KV<Long, List<String>>> tester = DoFnTester.of(writePartition);
    tester.setSideInput(filesView, GlobalWindow.INSTANCE, files);
    tester.processElement(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    List<KV<Long, List<String>>> partitions;
    if (expectedNumPartitions > 1) {
      partitions = tester.takeSideOutputElements(multiPartitionsTag);
    } else {
      partitions = tester.takeSideOutputElements(singlePartitionTag);
    }
    List<Long> partitionIds = Lists.newArrayList();
    List<String> partitionFileNames = Lists.newArrayList();
    for (KV<Long, List<String>> partition : partitions) {
      partitionIds.add(partition.getKey());
      for (String name : partition.getValue()) {
        partitionFileNames.add(name);
      }
    }

    assertEquals(expectedPartitionIds, partitionIds);
    if (numFiles == 0) {
      assertThat(partitionFileNames, Matchers.hasSize(1));
      assertTrue(Files.exists(Paths.get(partitionFileNames.get(0))));
      assertThat(Files.readAllBytes(Paths.get(partitionFileNames.get(0))).length,
          Matchers.equalTo(0));
    } else {
      assertEquals(fileNames, partitionFileNames);
    }
  }

  @Test
  public void testWriteTables() throws Exception {
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns("done", "done", "done", "done")
            .pollJobReturns(Status.FAILED, Status.SUCCEEDED, Status.SUCCEEDED, Status.SUCCEEDED));

    long numPartitions = 3;
    long numFilesPerPartition = 10;
    String jobIdToken = "jobIdToken";
    String tempFilePrefix = "tempFilePrefix";
    String jsonTable = "{}";
    String jsonSchema = "{}";
    List<String> expectedTempTables = Lists.newArrayList();

    List<KV<Long, Iterable<List<String>>>> partitions = Lists.newArrayList();
    for (long i = 0; i < numPartitions; ++i) {
      List<String> filesPerPartition = Lists.newArrayList();
      for (int j = 0; j < numFilesPerPartition; ++j) {
        filesPerPartition.add(String.format("files%05d", j));
      }
      partitions.add(KV.of(i, (Iterable<List<String>>) Collections.singleton(filesPerPartition)));
      expectedTempTables.add(String.format("{\"tableId\":\"%s_%05d\"}", jobIdToken, i));
    }

    WriteTables writeTables = new WriteTables(
        false,
        fakeBqServices,
        jobIdToken,
        tempFilePrefix,
        jsonTable,
        jsonSchema,
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED);

    DoFnTester<KV<Long, Iterable<List<String>>>, String> tester = DoFnTester.of(writeTables);
    for (KV<Long, Iterable<List<String>>> partition : partitions) {
      tester.processElement(partition);
    }

    List<String> tempTables = tester.takeOutputElements();

    logged.verifyInfo("Starting BigQuery load job");
    logged.verifyInfo("BigQuery load job failed");
    logged.verifyInfo("try 0/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyInfo("try 1/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyNotLogged("try 2/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);

    assertEquals(expectedTempTables, tempTables);
  }

  @Test
  public void testRemoveTemporaryFiles() throws Exception {
    BigQueryOptions bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    int numFiles = 10;
    List<String> fileNames = Lists.newArrayList();
    String tempFilePrefix = bqOptions.getTempLocation() + "/";
    TableRowWriter writer = new TableRowWriter(tempFilePrefix);
    for (int i = 0; i < numFiles; ++i) {
      String fileName = String.format("files%05d", i);
      writer.open(fileName);
      fileNames.add(writer.close().getKey());
    }
    fileNames.add(tempFilePrefix + String.format("files%05d", numFiles));

    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 10);

    WriteTables.removeTemporaryFiles(bqOptions, tempFilePrefix, fileNames);

    testNumFiles(tempDir, 0);

    for (String fileName : fileNames) {
      logged.verifyDebug("Removing file " + fileName);
    }
    logged.verifyDebug(fileNames.get(numFiles) + " does not exist.");
  }

  @Test
  public void testWriteRename() throws Exception {
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns("done", "done")
            .pollJobReturns(Status.FAILED, Status.SUCCEEDED))
        .withDatasetService(mockDatasetService);

    long numTempTables = 3;
    String jobIdToken = "jobIdToken";
    String jsonTable = "{}";
    List<String> tempTables = Lists.newArrayList();
    for (long i = 0; i < numTempTables; ++i) {
      tempTables.add(String.format("{\"tableId\":\"%s_%05d\"}", jobIdToken, i));
    }

    PCollectionView<Iterable<String>> tempTablesView = PCollectionViews.iterableView(
        TestPipeline.create(),
        WindowingStrategy.globalDefault(),
        StringUtf8Coder.of());

    WriteRename writeRename = new WriteRename(
        fakeBqServices,
        jobIdToken,
        jsonTable,
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED,
        tempTablesView);

    DoFnTester<String, Void> tester = DoFnTester.of(writeRename);
    tester.setSideInput(tempTablesView, GlobalWindow.INSTANCE, tempTables);
    tester.processElement(null);

    logged.verifyInfo("Starting BigQuery copy job");
    logged.verifyInfo("BigQuery copy job failed");
    logged.verifyInfo("try 0/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyInfo("try 1/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
    logged.verifyNotLogged("try 2/" + BigQueryIO.Write.Bound.MAX_RETRY_JOBS);
  }

  @Test
  public void testRemoveTemporaryTables() throws Exception {
    String projectId = "someproject";
    String datasetId = "somedataset";
    List<String> tables = Lists.newArrayList("table1", "table2", "table3");
    List<TableReference> tableRefs = Lists.newArrayList(
        BigQueryIO.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, tables.get(0))),
        BigQueryIO.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, tables.get(1))),
        BigQueryIO.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, tables.get(2))));

    doThrow(new IOException("Unable to delete table"))
        .when(mockDatasetService).deleteTable(projectId, datasetId, tables.get(0));
    doNothing().when(mockDatasetService).deleteTable(projectId, datasetId, tables.get(1));
    doNothing().when(mockDatasetService).deleteTable(projectId, datasetId, tables.get(2));

    WriteRename.removeTemporaryTables(mockDatasetService, tableRefs);

    for (TableReference ref : tableRefs) {
      logged.verifyDebug("Deleting table " + toJsonString(ref));
    }
    logged.verifyWarn("Failed to delete the table " + toJsonString(tableRefs.get(0)));
    logged.verifyNotLogged("Failed to delete the table " + toJsonString(tableRefs.get(1)));
    logged.verifyNotLogged("Failed to delete the table " + toJsonString(tableRefs.get(2)));
  }

  private static void testNumFiles(File tempDir, int expectedNumFiles) {
    assertEquals(expectedNumFiles, tempDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isFile();
      }}).length);
  }

  private class WriteExtractFiles implements SerializableFunction<GenericJson, Void> {
    private final SerializableFunction<Void, Schema> schemaGenerator;
    private final Collection<Map<String, Object>> records;

    private WriteExtractFiles(
        SerializableFunction<Void, Schema> schemaGenerator,
        Collection<Map<String, Object>> records) {
      this.schemaGenerator = schemaGenerator;
      this.records = records;
    }

    @Override
    public Void apply(GenericJson input) {
      List<String> destinations = (List<String>) input.get("destinationUris");
      for (String destination : destinations) {
        String newDest = destination.replace("*", "000000000000");
        Schema schema = schemaGenerator.apply(null);
        try (WritableByteChannel channel = IOChannelUtils.create(newDest, MimeTypes.BINARY);
            DataFileWriter<GenericRecord> tableRowWriter =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
                    .create(schema, Channels.newOutputStream(channel))) {
          for (Map<String, Object> record : records) {
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
            for (Map.Entry<String, Object> field : record.entrySet()) {
              genericRecordBuilder.set(field.getKey(), field.getValue());
            }
            tableRowWriter.append(genericRecordBuilder.build());
          }
        } catch (IOException e) {
          throw new IllegalStateException(
              String.format("Could not create destination for extract job %s", destination), e);
        }
      }
      return null;
    }
  }
}
