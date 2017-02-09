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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.BigQueryQuerySource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.BigQueryTableSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.JsonSchemaToTableSchema;
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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.CoderProperties;
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
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
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
import org.joda.time.Instant;
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
        BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
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

  private static class TableContainer {
    Table table;
    List<TableRow> rows;
    List<String> ids;

    TableContainer(Table table) {
      this.table = table;
      this.rows = new ArrayList<>();
      this.ids = new ArrayList<>();
    }

    TableContainer addRow(TableRow row, String id) {
      rows.add(row);
      ids.add(id);
      return this;
    }

    Table getTable() {
      return table;
    }

     List<TableRow> getRows() {
      return rows;
    }
  }

  // Table information must be static, as each ParDo will get a separate instance of
  // FakeDatasetServices, and they must all modify the same storage.
  private static com.google.common.collect.Table<String, String, Map<String, TableContainer>>
      tables = HashBasedTable.create();

  /** A fake dataset service that can be serialized, for use in testReadFromTable. */
  private static class FakeDatasetService implements DatasetService, Serializable {

    @Override
    public Table getTable(TableReference tableRef)
        throws InterruptedException, IOException {
      synchronized (tables) {
        Map<String, TableContainer> dataset =
            checkNotNull(
                tables.get(tableRef.getProjectId(), tableRef.getDatasetId()),
                "Tried to get a dataset %s:%s from %s, but no such dataset was set",
                tableRef.getProjectId(),
                tableRef.getDatasetId(),
                tableRef.getTableId(),
                FakeDatasetService.class.getSimpleName());
        TableContainer tableContainer = dataset.get(tableRef.getTableId());
        return tableContainer == null ? null : tableContainer.getTable();
      }
    }

    public List<TableRow> getAllRows(String projectId, String datasetId, String tableId)
        throws InterruptedException, IOException {
      synchronized (tables) {
        return getTableContainer(projectId, datasetId, tableId).getRows();
      }
    }

    private TableContainer getTableContainer(String projectId, String datasetId, String tableId)
            throws InterruptedException, IOException {
       synchronized (tables) {
         Map<String, TableContainer> dataset =
             checkNotNull(
                 tables.get(projectId, datasetId),
                 "Tried to get a dataset %s:%s from %s, but no such dataset was set",
                 projectId,
                 datasetId,
                 FakeDatasetService.class.getSimpleName());
         return checkNotNull(dataset.get(tableId),
             "Tried to get a table %s:%s.%s from %s, but no such table was set",
             projectId,
             datasetId,
             tableId,
             FakeDatasetService.class.getSimpleName());
       }
    }

    @Override
    public void deleteTable(TableReference tableRef) throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }


    @Override
    public void createTable(Table table) throws IOException {
      TableReference tableReference = table.getTableReference();
      synchronized (tables) {
        Map<String, TableContainer> dataset =
            checkNotNull(
                tables.get(tableReference.getProjectId(), tableReference.getDatasetId()),
                "Tried to get a dataset %s:%s from %s, but no such table was set",
                tableReference.getProjectId(),
                tableReference.getDatasetId(),
                FakeDatasetService.class.getSimpleName());
        TableContainer tableContainer = dataset.get(tableReference.getTableId());
        if (tableContainer == null) {
          tableContainer = new TableContainer(table);
          dataset.put(tableReference.getTableId(), tableContainer);
        }
      }
    }

    @Override
    public boolean isTableEmpty(TableReference tableRef)
        throws IOException, InterruptedException {
      Long numBytes = getTable(tableRef).getNumBytes();
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
      synchronized (tables) {
        Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
        if (dataset == null) {
          dataset = new HashMap<>();
          tables.put(projectId, datasetId, dataset);
        }
      }
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
      synchronized (tables) {
        assertEquals(rowList.size(), insertIdList.size());

        long dataSize = 0;
        TableContainer tableContainer = getTableContainer(
            ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
        for (int i = 0; i < rowList.size(); ++i) {
          tableContainer.addRow(rowList.get(i), insertIdList.get(i));
          dataSize += rowList.get(i).toString().length();
        }
        return dataSize;
      }
    }

    @Override
    public Table patchTableDescription(TableReference tableReference,
                                       @Nullable String tableDescription)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException("Unsupported");
    }
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();
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
    assertEquals(query, bound.getQuery());
    assertEquals(validate, bound.getValidate());
  }

  private void checkWriteObject(
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, String tableDescription) {
    checkWriteObjectWithValidate(
        bound,
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
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, String tableDescription, boolean validate) {
    assertEquals(project, bound.getTable().get().getProjectId());
    assertEquals(dataset, bound.getTable().get().getDatasetId());
    assertEquals(table, bound.getTable().get().getTableId());
    assertEquals(schema, bound.getSchema());
    assertEquals(createDisposition, bound.createDisposition);
    assertEquals(writeDisposition, bound.writeDisposition);
    assertEquals(tableDescription, bound.tableDescription);
    assertEquals(validate, bound.validate);
  }

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    tables = HashBasedTable.create();
    BigQueryIO.clearCreatedTables();
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
    String tableId = "sometable";
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject(projectId);
    bqOptions.setTempLocation("gs://testbucket/testdir");

    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(projectId, datasetId, "", "");
    TableReference tableReference =
        new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
    fakeDatasetService.createTable(new Table().setTableReference(tableReference));

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService())
        .withDatasetService(fakeDatasetService);

    Pipeline p = TestPipeline.create(bqOptions);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId(tableId);

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(Matchers.containsString("Unsupported"));
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
  public void testReadFromTable() throws IOException, InterruptedException {
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
    sometable.setTableReference(
        new TableReference()
            .setProjectId("non-executing-project")
            .setDatasetId("somedataset")
            .setTableId("sometable"));
    sometable.setNumBytes(1024L * 1024L);
    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset("non-executing-project", "somedataset", "", "");
    fakeDatasetService.createTable(sometable);
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

    File tempDir = new File(bqOptions.getTempLocation());
    testNumFiles(tempDir, 0);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testStreamingWrite() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
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
            .apply(BigQueryIO.Write.to("project-id:dataset-id.table-id")
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
  private static class PartitionedGlobalWindows extends

      NonMergingWindowFn<TableRow, PartitionedGlobalWindow> {
    private SerializableFunction<TableRow, String> extractPartition;

    public PartitionedGlobalWindows(SerializableFunction<TableRow, String> extractPartition) {
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
    public PartitionedGlobalWindow getSideInputWindow(BoundedWindow window) {
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
  @Category(NeedsRunner.class)
  public void testStreamingWriteWithWindowFn() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeDatasetService datasetService = new FakeDatasetService();
    datasetService.createDataset("project-id", "dataset-id", "", "");
    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withDatasetService(datasetService);

    List<TableRow> inserts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      inserts.add(new TableRow().set("name", "number" + i).set("number", i));
    }

    // Create a windowing strategy that puts the input into five different windows depending on
    // record value.
    WindowFn<TableRow, PartitionedGlobalWindow> window = new PartitionedGlobalWindows(
        new SerializableFunction<TableRow, String>() {
          @Override
          public String apply(TableRow value) {
            try {
              int intValue = (Integer) value.get("number") % 5;
              return Integer.toString(intValue);
            } catch (NumberFormatException e) {
              fail(e.toString());
            }
            return value.toString();
          }
        }
    );

    SerializableFunction<BoundedWindow, String> tableFunction =
        new SerializableFunction<BoundedWindow, String>() {
          @Override
          public String apply(BoundedWindow input) {
            return "project-id:dataset-id.table-id-" + ((PartitionedGlobalWindow) input).value;
          }
    };

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(inserts).withCoder(TableRowJsonCoder.of()))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply(Window.<TableRow>into(window))
        .apply(BigQueryIO.Write
            .to(tableFunction)
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
    thrown.expectMessage("UNKNOWN status of load job");
    try {
      p.run();
    } finally {
      File tempDir = new File(bqOptions.getTempLocation());
      testNumFiles(tempDir, 0);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteFailedJobs() throws Exception {
    BigQueryOptions bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject("defaultProject");
    bqOptions.setTempLocation(testFolder.newFolder("BigQueryIOTest").getAbsolutePath());

    FakeBigQueryServices fakeBqServices = new FakeBigQueryServices()
        .withJobService(new FakeJobService()
            .startJobReturns("done", "done", "done")
            .pollJobReturns(Status.FAILED, Status.FAILED, Status.FAILED));

    Pipeline p = TestPipeline.create(bqOptions);
    p.apply(Create.of(
        new TableRow().set("name", "a").set("number", 1),
        new TableRow().set("name", "b").set("number", 2),
        new TableRow().set("name", "c").set("number", 3))
        .withCoder(TableRowJsonCoder.of()))
        .apply(BigQueryIO.Write.to("dataset-id.table-id")
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
    BigQueryIO.Write.Bound bound =
            BigQueryIO.Write.to("foo.com:project:somedataset.sometable");
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
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
        bound,
        "foo.com:project",
        "somedataset",
        "sometable",
        null,
        CreateDisposition.CREATE_IF_NEEDED,
        WriteDisposition.WRITE_EMPTY,
        null,
        false);
  }

  @Test
  public void testBuildWriteDefaultProject() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.to("somedataset.sometable");
    checkWriteObject(
        bound, null, "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
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
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBuildWriteWithoutTable() {

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must set the table reference");
    p.apply(Create.empty(TableRowJsonCoder.of())).apply(BigQueryIO.Write.withoutValidation());
  }

  @Test
  public void testBuildWriteWithSchema() {
    TableSchema schema = new TableSchema();
    BigQueryIO.Write.Bound bound =
        BigQueryIO.Write.to("foo.com:project:somedataset.sometable").withSchema(schema);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        schema, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
  }

  @Test
  public void testBuildWriteWithCreateDispositionNever() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_NEVER, WriteDisposition.WRITE_EMPTY, null);
  }

  @Test
  public void testBuildWriteWithCreateDispositionIfNeeded() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
  }

  @Test
  public void testBuildWriteWithWriteDispositionTruncate() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_TRUNCATE, null);
  }

  @Test
  public void testBuildWriteWithWriteDispositionAppend() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND, null);
  }

  @Test
  public void testBuildWriteWithWriteDispositionEmpty() {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_EMPTY);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, null);
  }

  @Test
  public void testBuildWriteWithWriteWithTableDescription() {
    final String tblDescription = "foo bar table";
    BigQueryIO.Write.Bound bound = BigQueryIO.Write
        .to("foo.com:project:somedataset.sometable")
        .withTableDescription(tblDescription);
    checkWriteObject(
        bound,
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

    BigQueryIO.Write.Bound write = BigQueryIO.Write
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
  public void testStreamingWriteFnCreateNever() throws Exception {
    BigQueryIO.StreamingWriteFn fn = new BigQueryIO.StreamingWriteFn(
        null, CreateDisposition.CREATE_NEVER, null, new FakeBigQueryServices());
    assertEquals(BigQueryIO.parseTableSpec("dataset.table"),
        fn.getOrCreateTable(null, "dataset.table"));
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
        .apply(BigQueryIO.Write.to(tableRef)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withoutValidation());
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
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("no schema was provided");
    p
        .apply(Create.empty(TableRowJsonCoder.of()))
        .apply(BigQueryIO.Write
            .to("dataset.table")
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsTableAndTableSpec() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Cannot set both a table reference and a table function");
    p
        .apply(Create.empty(TableRowJsonCoder.of()))
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
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must set the table reference of a BigQueryIO.Write transform");
    p
        .apply(Create.empty(TableRowJsonCoder.of()))
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
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of(table),
        extractDestinationDir, fakeBqServices,
        StaticValueProvider.of("project"));

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
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of(table),
        extractDestinationDir, fakeBqServices, StaticValueProvider.of("project"));

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
    when(mockDatasetService.getTable(any(TableReference.class)))
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
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of("query"),
        StaticValueProvider.of(destinationTable),
        true /* flattenResults */, true /* useLegacySql */,
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
    when(mockDatasetService.getTable(eq(queryTable)))
        .thenReturn(new Table().setSchema(new TableSchema()));
    when(mockDatasetService.getTable(eq(destinationTable)))
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
        StaticValueProvider.of(jobIdToken), StaticValueProvider.of("query"),
        StaticValueProvider.of(destinationTable),
        true /* flattenResults */, true /* useLegacySql */,
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
    when(mockDatasetService.getTable(eq(destinationTable)))
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
    p.enableAbandonedNodeEnforcement(false);

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
        p,
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
        StaticValueProvider.of(jobIdToken),
        tempFilePrefix,
        StaticValueProvider.of(jsonTable),
        StaticValueProvider.of(jsonSchema),
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED,
        null);

    DoFnTester<KV<Long, Iterable<List<String>>>, String> tester = DoFnTester.of(writeTables);
    for (KV<Long, Iterable<List<String>>> partition : partitions) {
      tester.processElement(partition);
    }

    List<String> tempTables = tester.takeOutputElements();

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
    p.enableAbandonedNodeEnforcement(false);

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
        p,
        WindowingStrategy.globalDefault(),
        StringUtf8Coder.of());

    WriteRename writeRename = new WriteRename(
        fakeBqServices,
        StaticValueProvider.of(jobIdToken),
        StaticValueProvider.of(jsonTable),
        WriteDisposition.WRITE_EMPTY,
        CreateDisposition.CREATE_IF_NEEDED,
        tempTablesView,
        null);

    DoFnTester<String, Void> tester = DoFnTester.of(writeRename);
    tester.setSideInput(tempTablesView, GlobalWindow.INSTANCE, tempTables);
    tester.processElement(null);
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
        .when(mockDatasetService).deleteTable(tableRefs.get(0));
    doNothing().when(mockDatasetService).deleteTable(tableRefs.get(1));
    doNothing().when(mockDatasetService).deleteTable(tableRefs.get(2));

    WriteRename.removeTemporaryTables(mockDatasetService, tableRefs);

    for (TableReference ref : tableRefs) {
      logged.verifyDebug("Deleting table " + toJsonString(ref));
    }
    logged.verifyWarn("Failed to delete the table " + toJsonString(tableRefs.get(0)));
    logged.verifyNotLogged("Failed to delete the table " + toJsonString(tableRefs.get(1)));
    logged.verifyNotLogged("Failed to delete the table " + toJsonString(tableRefs.get(2)));
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
    BigQueryIO.Read.Bound read = BigQueryIO.Read.from(
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
    BigQueryIO.Read.Bound read = BigQueryIO.Read.fromQuery(
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
    BigQueryIO.Write.Bound write = BigQueryIO.Write
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

  @Test
  public void testTagWithUniqueIdsAndTableProjectNotNullWithNvp() {
    BigQueryOptions bqOptions = PipelineOptionsFactory.as(BigQueryOptions.class);
    bqOptions.setProject("project");
    BigQueryIO.TagWithUniqueIdsAndTable tag =
        new BigQueryIO.TagWithUniqueIdsAndTable(
            bqOptions, NestedValueProvider.of(
                StaticValueProvider.of("data_set.table_name"),
                new BigQueryIO.TableSpecToTableRef()), null);
    TableReference table = BigQueryIO.parseTableSpec(tag.getTableSpec().get());
    assertNotNull(table.getProjectId());
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

  @Test
  public void testShardedKeyCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(BigQueryIO.ShardedKeyCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testUniqueStepIdRead() {
    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    Pipeline pipeline = TestPipeline.create(options);
    bqOptions.setTempLocation("gs://testbucket/testdir");
    BigQueryIO.Read.Bound read1 = BigQueryIO.Read.fromQuery(
        options.getInputQuery()).withoutValidation();
    pipeline.apply(read1);
    BigQueryIO.Read.Bound read2 = BigQueryIO.Read.fromQuery(
        options.getInputQuery()).withoutValidation();
    pipeline.apply(read2);
    assertNotEquals(read1.stepUuid, read2.stepUuid);
    assertNotEquals(read1.jobUuid.get(), read2.jobUuid.get());
  }

  @Test
  public void testUniqueStepIdWrite() {
    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setTempLocation("gs://testbucket/testdir");
    Pipeline pipeline = TestPipeline.create(options);
    BigQueryIO.Write.Bound write1 = BigQueryIO.Write
        .to(options.getOutputTable())
        .withSchema(NestedValueProvider.of(
            options.getOutputSchema(), new JsonSchemaToTableSchema()))
        .withoutValidation();
    BigQueryIO.Write.Bound write2 = BigQueryIO.Write
        .to(options.getOutputTable())
        .withSchema(NestedValueProvider.of(
            options.getOutputSchema(), new JsonSchemaToTableSchema()))
        .withoutValidation();
    pipeline
        .apply(Create.empty(TableRowJsonCoder.of()))
        .apply(write1);
    pipeline
        .apply(Create.empty(TableRowJsonCoder.of()))
        .apply(write2);
    assertNotEquals(write1.stepUuid, write2.stepUuid);
    assertNotEquals(write1.jobUuid.get(), write2.jobUuid.get());
  }
}
