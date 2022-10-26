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

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"nullness", "rawtypes"})
public class UpdateSchemaDestination<DestinationT>
    extends DoFn<
        Iterable<KV<DestinationT, WriteTables.Result>>,
        Iterable<KV<TableDestination, WriteTables.Result>>> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateSchemaDestination.class);
  private final BigQueryServices bqServices;
  private final PCollectionView<String> loadJobIdPrefixView;
  private final ValueProvider<String> loadJobProjectId;
  private transient @Nullable DatasetService datasetService;
  private final int maxRetryJobs;
  private final @Nullable String kmsKey;
  private @Nullable BigQueryServices.JobService jobService;
  private final Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions;
  private final BigQueryIO.Write.WriteDisposition writeDisposition;
  private final BigQueryIO.Write.CreateDisposition createDisposition;
  private final DynamicDestinations dynamicDestinations;

  private static class PendingJobData {
    final BigQueryHelpers.PendingJob retryJob;
    final TableDestination tableDestination;
    final BoundedWindow window;

    public PendingJobData(
        BigQueryHelpers.PendingJob retryJob,
        TableDestination tableDestination,
        BoundedWindow window) {
      this.retryJob = retryJob;
      this.tableDestination = tableDestination;
      this.window = window;
    }
  }

  private final List<UpdateSchemaDestination.PendingJobData> pendingJobs = Lists.newArrayList();

  public UpdateSchemaDestination(
      BigQueryServices bqServices,
      PCollectionView<String> loadJobIdPrefixView,
      @Nullable ValueProvider<String> loadJobProjectId,
      BigQueryIO.Write.WriteDisposition writeDisposition,
      BigQueryIO.Write.CreateDisposition createDisposition,
      int maxRetryJobs,
      @Nullable String kmsKey,
      Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions,
      DynamicDestinations dynamicDestinations) {
    this.loadJobProjectId = loadJobProjectId;
    this.loadJobIdPrefixView = loadJobIdPrefixView;
    this.bqServices = bqServices;
    this.maxRetryJobs = maxRetryJobs;
    this.kmsKey = kmsKey;
    this.schemaUpdateOptions = schemaUpdateOptions;
    this.createDisposition = createDisposition;
    this.writeDisposition = writeDisposition;
    this.dynamicDestinations = dynamicDestinations;
  }

  @StartBundle
  public void startBundle(StartBundleContext c) {
    pendingJobs.clear();
  }

  TableDestination getTableWithDefaultProject(DestinationT destination, BigQueryOptions options) {
    TableDestination tableDestination = dynamicDestinations.getTable(destination);
    TableReference tableReference = tableDestination.getTableReference();

    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      tableReference.setProjectId(
          options.getBigQueryProject() == null
              ? options.getProject()
              : options.getBigQueryProject());
      tableDestination = tableDestination.withTableReference(tableReference);
    }

    return tableDestination;
  }

  @ProcessElement
  public void processElement(
      @Element Iterable<KV<DestinationT, WriteTables.Result>> element,
      ProcessContext context,
      BoundedWindow window)
      throws IOException {
    DestinationT destination = null;
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (KV<DestinationT, WriteTables.Result> entry : element) {
      destination = entry.getKey();
      if (destination != null) {
        break;
      }
    }
    if (destination != null) {
      TableDestination tableDestination = getTableWithDefaultProject(destination, options);
      TableSchema schema = dynamicDestinations.getSchema(destination);
      TableReference tableReference = tableDestination.getTableReference();
      String jobIdPrefix =
          BigQueryResourceNaming.createJobIdWithDestination(
              context.sideInput(loadJobIdPrefixView),
              tableDestination,
              1,
              context.pane().getIndex());
      jobIdPrefix += "_schemaUpdateDestination";
      BigQueryHelpers.PendingJob updateSchemaDestinationJob =
          startZeroLoadJob(
              getJobService(context.getPipelineOptions().as(BigQueryOptions.class)),
              getDatasetService(context.getPipelineOptions().as(BigQueryOptions.class)),
              jobIdPrefix,
              tableReference,
              tableDestination.getTimePartitioning(),
              tableDestination.getClustering(),
              schema,
              writeDisposition,
              createDisposition,
              schemaUpdateOptions);
      if (updateSchemaDestinationJob != null) {
        pendingJobs.add(new PendingJobData(updateSchemaDestinationJob, tableDestination, window));
      }
    }
    List<KV<TableDestination, WriteTables.Result>> tableDestinations = new ArrayList<>();
    for (KV<DestinationT, WriteTables.Result> entry : element) {
      tableDestinations.add(
          KV.of(getTableWithDefaultProject(destination, options), entry.getValue()));
    }
    context.output(tableDestinations);
  }

  @Teardown
  public void onTeardown() {
    try {
      if (datasetService != null) {
        datasetService.close();
        datasetService = null;
      }
      if (jobService != null) {
        jobService.close();
        jobService = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    DatasetService datasetService =
        getDatasetService(context.getPipelineOptions().as(BigQueryOptions.class));
    BigQueryHelpers.PendingJobManager jobManager = new BigQueryHelpers.PendingJobManager();
    for (final PendingJobData pendingJobData : pendingJobs) {
      jobManager =
          jobManager.addPendingJob(
              pendingJobData.retryJob,
              j -> {
                try {
                  if (pendingJobData.tableDestination.getTableDescription() != null) {
                    TableReference ref = pendingJobData.tableDestination.getTableReference();
                    datasetService.patchTableDescription(
                        ref.clone()
                            .setTableId(BigQueryHelpers.stripPartitionDecorator(ref.getTableId())),
                        pendingJobData.tableDestination.getTableDescription());
                  }
                  return null;
                } catch (IOException | InterruptedException e) {
                  return e;
                }
              });
    }
    jobManager.waitForDone();
  }

  private BigQueryHelpers.PendingJob startZeroLoadJob(
      BigQueryServices.JobService jobService,
      DatasetService datasetService,
      String jobIdPrefix,
      TableReference tableReference,
      TimePartitioning timePartitioning,
      Clustering clustering,
      @Nullable TableSchema schema,
      BigQueryIO.Write.WriteDisposition writeDisposition,
      BigQueryIO.Write.CreateDisposition createDisposition,
      Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions) {
    JobConfigurationLoad loadConfig =
        new JobConfigurationLoad()
            .setDestinationTable(tableReference)
            .setSchema(schema)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name())
            .setSourceFormat("NEWLINE_DELIMITED_JSON");
    if (schemaUpdateOptions != null) {
      List<String> options =
          schemaUpdateOptions.stream().map(Enum::name).collect(Collectors.toList());
      loadConfig.setSchemaUpdateOptions(options);
    }
    if (!loadConfig
            .getWriteDisposition()
            .equals(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE.toString())
        && !loadConfig
            .getWriteDisposition()
            .equals(BigQueryIO.Write.WriteDisposition.WRITE_APPEND.toString())) {
      return null;
    }
    final Table destinationTable;
    try {
      destinationTable = datasetService.getTable(tableReference);
      if (destinationTable == null) {
        return null; // no need to update schema ahead if table does not exists
      }
    } catch (IOException | InterruptedException e) {
      LOG.warn("Failed to get table {} with {}", tableReference, e.toString());
      throw new RuntimeException(e);
    }
    if (destinationTable.getSchema().equals(schema)) {
      return null; // no need to update schema ahead if schema is already the same
    }
    if (timePartitioning != null) {
      loadConfig.setTimePartitioning(timePartitioning);
      // only set clustering if timePartitioning is set
      if (clustering != null) {
        loadConfig.setClustering(clustering);
      }
    }
    if (kmsKey != null) {
      loadConfig.setDestinationEncryptionConfiguration(
          new EncryptionConfiguration().setKmsKeyName(kmsKey));
    }
    String projectId =
        loadJobProjectId == null || loadJobProjectId.get() == null
            ? tableReference.getProjectId()
            : loadJobProjectId.get();
    String bqLocation =
        BigQueryHelpers.getDatasetLocation(
            datasetService, tableReference.getProjectId(), tableReference.getDatasetId());

    BigQueryHelpers.PendingJob retryJob =
        new BigQueryHelpers.PendingJob(
            // Function to load the data.
            jobId -> {
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(projectId)
                      .setJobId(jobId.getJobId())
                      .setLocation(bqLocation);
              LOG.info(
                  "Loading zero rows using job {}, job id {} iteration {}",
                  tableReference,
                  jobRef,
                  jobId.getRetryIndex());
              try {
                jobService.startLoadJob(
                    jobRef, loadConfig, new ByteArrayContent("text/plain", new byte[0]));
              } catch (IOException | InterruptedException e) {
                LOG.warn("Load job {} failed with {}", jobRef, e.toString());
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

  private BigQueryServices.JobService getJobService(PipelineOptions pipelineOptions)
      throws IOException {
    if (jobService == null) {
      jobService = bqServices.getJobService(pipelineOptions.as(BigQueryOptions.class));
    }
    return jobService;
  }

  private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
    if (datasetService == null) {
      datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetService;
  }
}
