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

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes partitions to BigQuery tables.
 *
 * <p>The input is a list of files corresponding to each partition of a table. loadThese files are
 * loaded into a temporary table (or into the final table if there is only one partition). The
 * output is a {@link KV} mapping each final table to a list of the temporary tables containing its
 * data.
 *
 * <p>In the case where all the data in the files fit into a single load job, this transform loads
 * the data directly into the final table, skipping temporary tables. In this case, the output
 * {@link KV} maps the final table to itself.
 */
class WriteTables<DestinationT>
    extends DoFn<KV<ShardedKey<DestinationT>, List<String>>, KV<TableDestination, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteTables.class);

  private final boolean singlePartition;
  private final BigQueryServices bqServices;
  private final PCollectionView<String> jobIdToken;
  private final WriteDisposition firstPaneWriteDisposition;
  private final CreateDisposition firstPaneCreateDisposition;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private Map<DestinationT, String> jsonSchemas = Maps.newHashMap();

  public WriteTables(
      boolean singlePartition,
      BigQueryServices bqServices,
      PCollectionView<String> jobIdToken,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this.singlePartition = singlePartition;
    this.bqServices = bqServices;
    this.jobIdToken = jobIdToken;
    this.firstPaneWriteDisposition = writeDisposition;
    this.firstPaneCreateDisposition = createDisposition;
    this.dynamicDestinations = dynamicDestinations;
  }

  @StartBundle
  public void startBundle(StartBundleContext c) {
    // Clear the map on each bundle so we can notice side-input updates.
    // (alternative is to use a cache with a TTL).
    jsonSchemas.clear();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    dynamicDestinations.setSideInputAccessorFromProcessContext(c);
    DestinationT destination = c.element().getKey().getKey();
    TableSchema tableSchema;
    String jsonSchema = jsonSchemas.get(destination);
    if (jsonSchema != null) {
      tableSchema = BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    } else {
      tableSchema = dynamicDestinations.getSchema(destination);
      if (tableSchema != null) {
        jsonSchemas.put(destination, BigQueryHelpers.toJsonString(tableSchema));
      }
    }

    TableDestination tableDestination = dynamicDestinations.getTable(destination);
    TableReference tableReference = tableDestination.getTableReference();
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      tableReference.setProjectId(
          c.getPipelineOptions().as(BigQueryOptions.class).getProject());
      tableDestination = new TableDestination(
          tableReference, tableDestination.getTableDescription());
    }

    Integer partition = c.element().getKey().getShardNumber();
    List<String> partitionFiles = Lists.newArrayList(c.element().getValue());
    String jobIdPrefix = BigQueryHelpers.createJobId(
            c.sideInput(jobIdToken), tableDestination, partition, c.pane().getIndex());

    if (!singlePartition) {
      tableReference.setTableId(jobIdPrefix);
    }

    WriteDisposition writeDisposition =
        (c.pane().getIndex() == 0) ? firstPaneWriteDisposition : WriteDisposition.WRITE_APPEND;
    CreateDisposition createDisposition =
        (c.pane().getIndex() == 0) ? firstPaneCreateDisposition : CreateDisposition.CREATE_NEVER;
    load(
        bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
        bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
        jobIdPrefix,
        tableReference,
        tableSchema,
        partitionFiles,
        writeDisposition,
        createDisposition,
        tableDestination.getTableDescription());
    c.output(KV.of(tableDestination, BigQueryHelpers.toJsonString(tableReference)));

    removeTemporaryFiles(partitionFiles);
  }

  private void load(
      JobService jobService,
      DatasetService datasetService,
      String jobIdPrefix,
      TableReference ref,
      @Nullable TableSchema schema,
      List<String> gcsUris,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      @Nullable String tableDescription)
      throws InterruptedException, IOException {
    JobConfigurationLoad loadConfig =
        new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSchema(schema)
            .setSourceUris(gcsUris)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name())
            .setSourceFormat("NEWLINE_DELIMITED_JSON");

    String projectId = ref.getProjectId();
    Job lastFailedLoadJob = null;
    for (int i = 0; i < BatchLoads.MAX_RETRY_JOBS; ++i) {
      String jobId = jobIdPrefix + "-" + i;
      JobReference jobRef = new JobReference().setProjectId(projectId).setJobId(jobId);
      jobService.startLoadJob(jobRef, loadConfig);
      Job loadJob = jobService.pollJob(jobRef, BatchLoads.LOAD_JOB_POLL_MAX_RETRIES);
      Status jobStatus = BigQueryHelpers.parseStatus(loadJob);
      switch (jobStatus) {
        case SUCCEEDED:
          if (tableDescription != null) {
            datasetService.patchTableDescription(ref, tableDescription);
          }
          return;
        case UNKNOWN:
          throw new RuntimeException(
              String.format(
                  "UNKNOWN status of load job [%s]: %s.",
                  jobId, BigQueryHelpers.jobToPrettyString(loadJob)));
        case FAILED:
          lastFailedLoadJob = loadJob;
          continue;
        default:
          throw new IllegalStateException(
              String.format(
                  "Unexpected status [%s] of load job: %s.",
                  jobStatus, BigQueryHelpers.jobToPrettyString(loadJob)));
      }
    }
    throw new RuntimeException(
        String.format(
            "Failed to create load job with id prefix %s, "
                + "reached max retries: %d, last failed load job: %s.",
            jobIdPrefix,
            BatchLoads.MAX_RETRY_JOBS,
            BigQueryHelpers.jobToPrettyString(lastFailedLoadJob)));
  }

  static void removeTemporaryFiles(Collection<String> files) throws IOException {
    ImmutableList.Builder<ResourceId> fileResources = ImmutableList.builder();
    for (String file: files) {
      fileResources.add(FileSystems.matchNewResource(file, false/* isDirectory */));
    }
    FileSystems.delete(fileResources.build(), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
  }
}
