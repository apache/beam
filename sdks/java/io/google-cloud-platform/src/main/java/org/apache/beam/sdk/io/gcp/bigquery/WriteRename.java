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

import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.PendingJobManager;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies temporary tables to destination table. The input element is an {@link Iterable} that
 * provides the list of all temporary tables created for a given {@link TableDestination}.
 */
class WriteRename
    extends DoFn<Iterable<KV<TableDestination, WriteTables.Result>>, TableDestination> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteRename.class);

  private final BigQueryServices bqServices;
  private final PCollectionView<String> jobIdToken;

  // In the triggered scenario, the user-supplied create and write dispositions only apply to the
  // first trigger pane, as that's when when the table is created. Subsequent loads should always
  // append to the table, and so use CREATE_NEVER and WRITE_APPEND dispositions respectively.
  private final WriteDisposition firstPaneWriteDisposition;
  private final CreateDisposition firstPaneCreateDisposition;
  private final int maxRetryJobs;
  private final @Nullable String kmsKey;
  private final @Nullable ValueProvider<String> loadJobProjectId;
  private transient @Nullable DatasetService datasetService;

  private static class PendingJobData {
    final BigQueryHelpers.PendingJob retryJob;
    final TableDestination tableDestination;
    final List<TableReference> tempTables;
    final BoundedWindow window;

    public PendingJobData(
        BigQueryHelpers.PendingJob retryJob,
        TableDestination tableDestination,
        List<TableReference> tempTables,
        BoundedWindow window) {
      this.retryJob = retryJob;
      this.tableDestination = tableDestination;
      this.tempTables = tempTables;
      this.window = window;
    }
  }
  // All pending copy jobs.
  private List<PendingJobData> pendingJobs = Lists.newArrayList();

  public WriteRename(
      BigQueryServices bqServices,
      PCollectionView<String> jobIdToken,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      int maxRetryJobs,
      @Nullable String kmsKey,
      @Nullable ValueProvider<String> loadJobProjectId) {
    this.bqServices = bqServices;
    this.jobIdToken = jobIdToken;
    this.firstPaneWriteDisposition = writeDisposition;
    this.firstPaneCreateDisposition = createDisposition;
    this.maxRetryJobs = maxRetryJobs;
    this.kmsKey = kmsKey;
    this.loadJobProjectId = loadJobProjectId;
  }

  @StartBundle
  public void startBundle(StartBundleContext c) {
    pendingJobs.clear();
  }

  @Teardown
  public void onTeardown() {
    try {
      if (datasetService != null) {
        datasetService.close();
        datasetService = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element Iterable<KV<TableDestination, WriteTables.Result>> element,
      ProcessContext c,
      BoundedWindow window)
      throws Exception {
    Multimap<TableDestination, WriteTables.Result> tempTables = ArrayListMultimap.create();
    for (KV<TableDestination, WriteTables.Result> entry : element) {
      tempTables.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<TableDestination, Collection<WriteTables.Result>> entry :
        tempTables.asMap().entrySet()) {
      // Process each destination table.
      // Do not copy if no temp tables are provided.
      if (!entry.getValue().isEmpty()) {
        pendingJobs.add(startWriteRename(entry.getKey(), entry.getValue(), c, window));
      }
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    DatasetService datasetService =
        getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class));
    PendingJobManager jobManager = new PendingJobManager();
    for (PendingJobData pendingJob : pendingJobs) {
      jobManager.addPendingJob(
          pendingJob.retryJob,
          j -> {
            try {
              if (pendingJob.tableDestination.getTableDescription() != null) {
                TableReference ref = pendingJob.tableDestination.getTableReference();
                datasetService.patchTableDescription(
                    ref.clone()
                        .setTableId(BigQueryHelpers.stripPartitionDecorator(ref.getTableId())),
                    pendingJob.tableDestination.getTableDescription());
              }
              c.output(
                  pendingJob.tableDestination, pendingJob.window.maxTimestamp(), pendingJob.window);
              removeTemporaryTables(datasetService, pendingJob.tempTables);
              return null;
            } catch (IOException | InterruptedException e) {
              return e;
            }
          });
    }
    jobManager.waitForDone();
  }

  private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
    if (datasetService == null) {
      datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetService;
  }

  private PendingJobData startWriteRename(
      TableDestination finalTableDestination,
      Iterable<WriteTables.Result> tempTableNames,
      ProcessContext c,
      BoundedWindow window)
      throws Exception {
    // The pane may have advanced either here due to triggering or due to an upstream trigger. We
    // check the upstream
    // trigger to handle the case where an earlier pane triggered the single-partition path. If this
    // happened, then the
    // table will already exist so we want to append to the table.
    WriteTables.@Nullable Result firstTempTable = Iterables.getFirst(tempTableNames, null);
    boolean isFirstPane =
        firstTempTable != null && firstTempTable.isFirstPane() && c.pane().isFirst();
    WriteDisposition writeDisposition =
        isFirstPane ? firstPaneWriteDisposition : WriteDisposition.WRITE_APPEND;
    CreateDisposition createDisposition =
        isFirstPane ? firstPaneCreateDisposition : CreateDisposition.CREATE_NEVER;
    List<TableReference> tempTables =
        StreamSupport.stream(tempTableNames.spliterator(), false)
            .map(
                result ->
                    BigQueryHelpers.fromJsonString(result.getTableName(), TableReference.class))
            .collect(Collectors.toList());

    // Make sure each destination table gets a unique job id.
    String jobIdPrefix =
        BigQueryResourceNaming.createJobIdWithDestination(
            c.sideInput(jobIdToken), finalTableDestination, -1, c.pane().getIndex());

    BigQueryHelpers.PendingJob retryJob =
        startCopy(
            bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
            getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
            jobIdPrefix,
            finalTableDestination.getTableReference(),
            tempTables,
            writeDisposition,
            createDisposition,
            kmsKey,
            loadJobProjectId);
    return new PendingJobData(retryJob, finalTableDestination, tempTables, window);
  }

  private BigQueryHelpers.PendingJob startCopy(
      JobService jobService,
      DatasetService datasetService,
      String jobIdPrefix,
      TableReference ref,
      List<TableReference> tempTables,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      @Nullable String kmsKey,
      @Nullable ValueProvider<String> loadJobProjectId) {
    JobConfigurationTableCopy copyConfig =
        new JobConfigurationTableCopy()
            .setSourceTables(tempTables)
            .setDestinationTable(ref)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name());
    if (kmsKey != null) {
      copyConfig.setDestinationEncryptionConfiguration(
          new EncryptionConfiguration().setKmsKeyName(kmsKey));
    }

    String bqLocation =
        BigQueryHelpers.getDatasetLocation(datasetService, ref.getProjectId(), ref.getDatasetId());

    String projectId =
        loadJobProjectId == null || loadJobProjectId.get() == null
            ? ref.getProjectId()
            : loadJobProjectId.get();
    BigQueryHelpers.PendingJob retryJob =
        new BigQueryHelpers.PendingJob(
            jobId -> {
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(projectId)
                      .setJobId(jobId.getJobId())
                      .setLocation(bqLocation);
              LOG.info(
                  "Starting copy job for table {} using  {}, job id iteration {}",
                  ref,
                  jobRef,
                  jobId.getRetryIndex());
              try {
                jobService.startCopyJob(jobRef, copyConfig);
              } catch (IOException | InterruptedException e) {
                LOG.warn("Copy job {} failed.", jobRef, e);
                throw new RuntimeException(e);
              }
              return null;
            },
            // Function to poll the result of a load job.
            jobId -> {
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(projectId)
                      .setJobId(jobId.getJobId())
                      .setLocation(bqLocation);
              try {
                return jobService.pollJob(jobRef, BatchLoads.LOAD_JOB_POLL_MAX_RETRIES);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            },
            // Function to lookup a job.
            jobId -> {
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(projectId)
                      .setJobId(jobId.getJobId())
                      .setLocation(bqLocation);
              try {
                return jobService.getJob(jobRef);
              } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
              }
            },
            maxRetryJobs,
            jobIdPrefix);
    return retryJob;
  }

  static void removeTemporaryTables(DatasetService tableService, List<TableReference> tempTables) {
    for (TableReference tableRef : tempTables) {
      try {
        LOG.debug("Deleting table {}", BigQueryHelpers.toJsonString(tableRef));
        tableService.deleteTable(tableRef);
      } catch (Exception e) {
        LOG.warn("Failed to delete the table {}", BigQueryHelpers.toJsonString(tableRef), e);
      }
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    builder
        .add(
            DisplayData.item("firstPaneWriteDisposition", firstPaneWriteDisposition.toString())
                .withLabel("Write Disposition"))
        .add(
            DisplayData.item("firstPaneCreateDisposition", firstPaneCreateDisposition.toString())
                .withLabel("Create Disposition"))
        .add(
            DisplayData.item("launchesBigQueryJobs", true)
                .withLabel("This transform launches BigQuery jobs to read/write elements."));
  }
}
