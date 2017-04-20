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

import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics4;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.util.FluentBackoff;

import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.Transport;
import org.joda.time.Duration;

/**
 * A fake implementation of BigQuery's job service.
 */
class FakeJobService implements JobService, Serializable {
  static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  // Whenever a job is started, the first 5 calls to GetJob will report the job as pending,
  // the next 5 will return the job as running, and only then will the job report as done.
  private static final int GET_JOBS_TRANSITION_INTERVAL = 5;

  private FakeDatasetService datasetService;

  private static class JobInfo {
    Job job;
    int getJobCount = 0;

    JobInfo(Job job) {
      this.job = job;
    }
  }

  private static final com.google.common.collect.Table<String, String, JobInfo> allJobs =
      HashBasedTable.create();
  private static int numExtractJobCalls = 0;

  private static final com.google.common.collect.Table<String, String, List<String>>
      filesForLoadJobs = HashBasedTable.create();
  private static final com.google.common.collect.Table<String, String, JobStatistics>
      dryRunQueryResults = HashBasedTable.create();

  FakeJobService() {
    this.datasetService = new FakeDatasetService();
  }

  @Override
  public void startLoadJob(JobReference jobRef, JobConfigurationLoad loadConfig)
      throws InterruptedException, IOException {
    synchronized (allJobs) {
      Job job = new Job();
      job.setJobReference(jobRef);
      job.setConfiguration(new JobConfiguration().setLoad(loadConfig));
      job.setKind(" bigquery#job");
      job.setStatus(new JobStatus().setState("PENDING"));

      // Copy the files to a new location for import, as the temporary files will be deleted by
      // the caller.
      if (loadConfig.getSourceUris().size() > 0) {
        List<String> loadFiles = Lists.newArrayList();
        for (String filename : loadConfig.getSourceUris()) {
          loadFiles.add(filename + ThreadLocalRandom.current().nextInt());
        }
        IOChannelUtils.getFactory(loadFiles.get(0)).copy(loadConfig.getSourceUris(), loadFiles);
        filesForLoadJobs.put(jobRef.getProjectId(), jobRef.getJobId(), loadFiles);
      }

      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  @Override
  public void startExtractJob(JobReference jobRef, JobConfigurationExtract extractConfig)
      throws InterruptedException, IOException {
    checkArgument(extractConfig.getDestinationFormat().equals("AVRO"),
        "Only extract to AVRO is supported");
    synchronized (allJobs) {
      ++numExtractJobCalls;

      Job job = new Job();
      job.setJobReference(jobRef);
      job.setConfiguration(new JobConfiguration().setExtract(extractConfig));
      job.setKind(" bigquery#job");
      job.setStatus(new JobStatus().setState("PENDING"));
      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  public int getNumExtractJobCalls() {
    synchronized (allJobs) {
      return numExtractJobCalls;
    }
  }

  @Override
  public void startQueryJob(JobReference jobRef, JobConfigurationQuery query)
      throws IOException, InterruptedException {
    synchronized (allJobs) {
      Job job = new Job();
      job.setJobReference(jobRef);
      job.setConfiguration(new JobConfiguration().setQuery(query));
      job.setKind(" bigquery#job");
      job.setStatus(new JobStatus().setState("PENDING"));
      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  @Override
  public void startCopyJob(JobReference jobRef, JobConfigurationTableCopy copyConfig)
      throws IOException, InterruptedException {
    synchronized (allJobs) {
      Job job = new Job();
      job.setJobReference(jobRef);
      job.setConfiguration(new JobConfiguration().setCopy(copyConfig));
      job.setKind(" bigquery#job");
      job.setStatus(new JobStatus().setState("PENDING"));
      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  @Override
  public Job pollJob(JobReference jobRef, int maxAttempts)
      throws InterruptedException {
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withMaxRetries(maxAttempts)
            .withInitialBackoff(Duration.millis(10))
            .withMaxBackoff(Duration.standardSeconds(1))
            .backoff();
    Sleeper sleeper = Sleeper.DEFAULT;
    try {
      do {
        Job job = getJob(jobRef);
        if (job != null) {
          JobStatus status = job.getStatus();
          if (status != null && status.getState() != null
              && (status.getState().equals("DONE") || status.getState().equals("FAILED"))) {
            return job;
          }
        }
      } while (BackOffUtils.next(sleeper, backoff));
    } catch (IOException e) {
      return null;
    }
    return null;
  }

  public void expectDryRunQuery(String projectId, String query, JobStatistics result) {
    synchronized (dryRunQueryResults) {
      dryRunQueryResults.put(projectId, query, result);
    }
  }

  @Override
  public JobStatistics dryRunQuery(String projectId, JobConfigurationQuery query)
      throws InterruptedException, IOException {
    synchronized (dryRunQueryResults) {
      JobStatistics result = dryRunQueryResults.get(projectId, query.getQuery());
      if (result != null) {
        return result;
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public Job getJob(JobReference jobRef) throws InterruptedException {
    try {
      synchronized (allJobs) {
        JobInfo job = allJobs.get(jobRef.getProjectId(), jobRef.getJobId());
        if (job == null) {
          return null;
        }
        try {
          ++job.getJobCount;
          if (job.getJobCount == GET_JOBS_TRANSITION_INTERVAL + 1) {
            job.job.getStatus().setState("RUNNING");
          } else if (job.getJobCount == 2 * GET_JOBS_TRANSITION_INTERVAL + 1) {
            job.job.setStatus(runJob(job.job));
          }
        } catch (Exception e) {
          job.job.getStatus().setState("FAILED").setErrorResult(new ErrorProto());
        }
        return JSON_FACTORY.fromString(JSON_FACTORY.toString(job.job), Job.class);
      }
    } catch (IOException e) {
      return null;
    }
  }

  private JobStatus runJob(Job job) throws InterruptedException, IOException {
    if (job.getConfiguration().getLoad() != null) {
      return runLoadJob(job.getJobReference(), job.getConfiguration().getLoad());
    } else if (job.getConfiguration().getCopy() != null) {
      return runCopyJob(job.getConfiguration().getCopy());
    } else if (job.getConfiguration().getExtract() != null) {
      return runExtractJob(job, job.getConfiguration().getExtract());
    } else if (job.getConfiguration().getQuery() != null) {
      return runQueryJob(job.getConfiguration().getQuery());
    }
    return new JobStatus().setState("DONE");
  }

  private boolean validateDispositions(Table table, CreateDisposition createDisposition,
                                       WriteDisposition writeDisposition)
      throws InterruptedException, IOException {
    if (table == null) {
      if (createDisposition == CreateDisposition.CREATE_NEVER) {
        return false;
      }
    } else if (writeDisposition == WriteDisposition.WRITE_TRUNCATE) {
      datasetService.deleteTable(table.getTableReference());
    } else if (writeDisposition == WriteDisposition.WRITE_EMPTY) {
      List<TableRow> allRows = datasetService.getAllRows(table.getTableReference().getProjectId(),
          table.getTableReference().getDatasetId(), table.getTableReference().getTableId());
      if (!allRows.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private JobStatus runLoadJob(JobReference jobRef, JobConfigurationLoad load)
      throws InterruptedException, IOException {
    TableReference destination = load.getDestinationTable();
    TableSchema schema = load.getSchema();
    List<String> sourceFiles = filesForLoadJobs.get(jobRef.getProjectId(), jobRef.getJobId());
    WriteDisposition writeDisposition = WriteDisposition.valueOf(load.getWriteDisposition());
    CreateDisposition createDisposition = CreateDisposition.valueOf(load.getCreateDisposition());
    checkArgument(load.getSourceFormat().equals("NEWLINE_DELIMITED_JSON"));
    Table existingTable = datasetService.getTable(destination);
    if (!validateDispositions(existingTable, createDisposition, writeDisposition)) {
      return new JobStatus().setState("FAILED").setErrorResult(new ErrorProto());
    }

    datasetService.createTable(new Table().setTableReference(destination).setSchema(schema));

    List<TableRow> rows = Lists.newArrayList();
    for (String filename : sourceFiles) {
      rows.addAll(readRows(filename));
    }
    datasetService.insertAll(destination, rows, null);
    return new JobStatus().setState("DONE");
  }

  private JobStatus runCopyJob(JobConfigurationTableCopy copy)
      throws InterruptedException, IOException {
    List<TableReference> sources = copy.getSourceTables();
    TableReference destination = copy.getDestinationTable();
    WriteDisposition writeDisposition = WriteDisposition.valueOf(copy.getWriteDisposition());
    CreateDisposition createDisposition = CreateDisposition.valueOf(copy.getCreateDisposition());
    Table existingTable = datasetService.getTable(destination);
    if (!validateDispositions(existingTable, createDisposition, writeDisposition)) {
      return new JobStatus().setState("FAILED").setErrorResult(new ErrorProto());
    }

    List<TableRow> allRows = Lists.newArrayList();
    for (TableReference source : sources) {
      allRows.addAll(datasetService.getAllRows(
          source.getProjectId(), source.getDatasetId(), source.getTableId()));
    }
    datasetService.createTable(new Table().setTableReference(destination));
    datasetService.insertAll(destination, allRows, null);
    return new JobStatus().setState("DONE");
  }

  private JobStatus runExtractJob(Job job, JobConfigurationExtract extract)
      throws InterruptedException, IOException {
    TableReference sourceTable = extract.getSourceTable();

    List<TableRow> rows = datasetService.getAllRows(
        sourceTable.getProjectId(), sourceTable.getDatasetId(), sourceTable.getTableId());
    TableSchema schema = datasetService.getTable(sourceTable).getSchema();
    List<Long> destinationFileCounts = Lists.newArrayList();
    for (String destination : extract.getDestinationUris()) {
      destinationFileCounts.add(writeRows(sourceTable.getTableId(), rows, schema, destination));
    }
    job.setStatistics(new JobStatistics().setExtract(
        new JobStatistics4().setDestinationUriFileCounts(destinationFileCounts)));
    return new JobStatus().setState("DONE");
  }

  private JobStatus runQueryJob(JobConfigurationQuery query)
      throws IOException, InterruptedException  {
    List<TableRow> rows = FakeBigQueryServices.rowsFromEncodedQuery(query.getQuery());
    datasetService.createTable(new Table().setTableReference(query.getDestinationTable()));
    datasetService.insertAll(query.getDestinationTable(), rows, null);
    return new JobStatus().setState("DONE");
  }

  private List<TableRow> readRows(String filename) throws IOException {
    Coder<TableRow> coder = TableRowJsonCoder.of();
    List<TableRow> tableRows = Lists.newArrayList();
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      String line;
      while ((line = reader.readLine()) != null) {
        TableRow tableRow = coder.decode(
            new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)), Context.OUTER);
        tableRows.add(tableRow);
      }
    }
    return tableRows;
  }

  private long writeRows(String tableId, List<TableRow> rows, TableSchema schema,
                         String destinationPattern) throws IOException {
    Schema avroSchema = BigQueryAvroUtils.toGenericAvroSchema(tableId, schema.getFields());
    List<TableRow> rowsToWrite = Lists.newArrayList();
    int shard = 0;
    for (int i = 0; i < rows.size(); ++i) {
      rowsToWrite.add(rows.get(i));
      if (rowsToWrite.size() == 5) {
        writeRowsHelper(rowsToWrite, avroSchema, destinationPattern, shard++);
        rowsToWrite.clear();
      }
    }
    if (!rowsToWrite.isEmpty()) {
      writeRowsHelper(rowsToWrite, avroSchema, destinationPattern, shard++);
    }
    return shard;
  }

  private void writeRowsHelper(List<TableRow> rows, Schema avroSchema,
                               String destinationPattern, int shard) throws IOException {
    String filename = destinationPattern.replace("*", String.format("%012d", shard));
    try (WritableByteChannel channel = IOChannelUtils.create(filename, MimeTypes.BINARY);
         DataFileWriter<GenericRecord> tableRowWriter =
             new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(avroSchema))
                 .create(avroSchema, Channels.newOutputStream(channel))) {
      for (Map<String, Object> record : rows) {
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(avroSchema);
        for (Map.Entry<String, Object> field : record.entrySet()) {
          genericRecordBuilder.set(field.getKey(), field.getValue());
        }
        tableRowWriter.append(genericRecordBuilder.build());
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Could not create destination for extract job %s", filename), e);
    }
  }
}
