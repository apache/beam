package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
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
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.util.FluentBackoff;

import org.apache.beam.sdk.util.Transport;
import org.joda.time.Duration;

/**
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
      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  @Override
  public void startExtractJob(JobReference jobRef, JobConfigurationExtract extractConfig)
      throws InterruptedException, IOException {
    checkArgument(extractConfig.getDestinationFormat().equals("AVRO"),
        "Only extract to AVRO is supported");
    checkArgument(extractConfig.getDestinationUris().size() == 1,
        "Must specify exactly one destination URI.");
    synchronized (allJobs) {
      Job job = new Job();
      job.setJobReference(jobRef);
      job.setConfiguration(new JobConfiguration().setExtract(extractConfig));
      job.setKind(" bigquery#job");
      job.setStatus(new JobStatus().setState("PENDING"));
      allJobs.put(jobRef.getProjectId(), jobRef.getJobId(), new JobInfo(job));
    }
  }

  @Override
  public void startQueryJob(JobReference jobRef, JobConfigurationQuery query)
      throws IOException, InterruptedException {
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
            .withInitialBackoff(Duration.millis(50))
            .withMaxBackoff(Duration.standardMinutes(1))
            .backoff();
    Sleeper sleeper = Sleeper.DEFAULT;
    try {
      do {
        Job job = getJob(jobRef);
        if (job != null) {
          JobStatus status = job.getStatus();
          if (status != null && status.getState() != null && status.getState().equals("DONE")) {
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
        ++job.getJobCount;
        if (job.getJobCount == GET_JOBS_TRANSITION_INTERVAL + 1) {
          job.job.getStatus().setState("RUNNING");
        } else if (job.getJobCount == 2 * GET_JOBS_TRANSITION_INTERVAL + 1) {
          runJob(job.job);
          job.job.getStatus().setState("DONE");
        }
        return JSON_FACTORY.fromString(JSON_FACTORY.toString(job.job), Job.class);
      }
    } catch (IOException e) {
      return null;
    }
  }

  private void runJob(Job job) throws InterruptedException, IOException {
    if (job.getConfiguration().getLoad() != null) {
      runLoadJob(job.getConfiguration().getLoad());
    } else if (job.getConfiguration().getCopy() != null) {
      runCopyJob(job.getConfiguration().getCopy());
    } else if (job.getConfiguration().getExtract() != null) {
      runExtractJob(job, job.getConfiguration().getExtract());
    }
  }

  private void validateDispositions(Table table, CreateDisposition createDisposition,
                                    WriteDisposition writeDisposition)
      throws InterruptedException, IOException {
    if (table == null) {
      checkState(createDisposition != CreateDisposition.CREATE_NEVER,
          "CreateDisposition == CREATE_NEVER but the table doesn't exist.");
    } else if (writeDisposition == WriteDisposition.WRITE_TRUNCATE) {
      datasetService.deleteTable(table.getTableReference());
    } else if (writeDisposition == WriteDisposition.WRITE_EMPTY) {
      List<TableRow> allRows = datasetService.getAllRows(table.getTableReference().getProjectId(),
          table.getTableReference().getDatasetId(), table.getTableReference().getTableId());
      checkState(allRows.isEmpty(), "Write disposition was set to WRITE_EMPTY,"
          + " but the table was not empty.");
    }
  }
  private void runLoadJob(JobConfigurationLoad load)
      throws InterruptedException, IOException {
    TableReference destination = load.getDestinationTable();
    TableSchema schema = load.getSchema();
    List<String> sourceFiles = load.getSourceUris();
    WriteDisposition writeDisposition = WriteDisposition.valueOf(load.getWriteDisposition());
    CreateDisposition createDisposition = CreateDisposition.valueOf(load.getCreateDisposition());
    checkArgument(load.getSourceFormat().equals("NEWLINE_DELIMITED_JSON"));
    Table existingTable = datasetService.getTable(destination);
    validateDispositions(existingTable, createDisposition, writeDisposition);

    datasetService.createTable(new Table().setTableReference(destination).setSchema(schema));

    List<TableRow> rows = Lists.newArrayList();
    for (String filename : sourceFiles) {
      rows.addAll(readRows(filename));
    }
    datasetService.insertAll(destination, rows, null);
  }

  private void runCopyJob(JobConfigurationTableCopy copy)
      throws InterruptedException, IOException {
    List<TableReference> sources = copy.getSourceTables();
    TableReference destination = copy.getDestinationTable();
    WriteDisposition writeDisposition = WriteDisposition.valueOf(copy.getWriteDisposition());
    CreateDisposition createDisposition = CreateDisposition.valueOf(copy.getCreateDisposition());
    Table existingTable = datasetService.getTable(destination);
    validateDispositions(existingTable, createDisposition, writeDisposition);

    List<TableRow> allRows = Lists.newArrayList();
    for (TableReference source : sources) {
      allRows.addAll(datasetService.getAllRows(
          source.getProjectId(), source.getDatasetId(), source.getTableId()));
    }
    datasetService.insertAll(destination, allRows, null);
  }

  private void runExtractJob(Job job, JobConfigurationExtract extract) {
    TableReference sourceTable = extract.getSourceTable();
    extract.getDestinationUris().get(0);
    List<Long> destinationFileCounts = Lists.newArrayList(0L);
    job.setStatistics(new JobStatistics().setExtract(
        new JobStatistics4().setDestinationUriFileCounts(destinationFileCounts)));
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
}
