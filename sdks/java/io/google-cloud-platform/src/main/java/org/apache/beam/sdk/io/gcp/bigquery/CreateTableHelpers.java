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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.CONNECTION_ID;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.STORAGE_URI;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.gax.rpc.ApiException;
import com.google.api.services.bigquery.model.BigLakeConfiguration;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableHelpers.class);

  /**
   * The list of tables created so far, so we don't try the creation each time.
   *
   * <p>TODO: We should put a bound on memory usage of this. Use guava cache instead.
   */
  private static Set<String> createdTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private static final Duration INITIAL_RPC_BACKOFF = Duration.millis(500);
  private static final FluentBackoff DEFAULT_BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(4).withInitialBackoff(INITIAL_RPC_BACKOFF);

  // When CREATE_IF_NEEDED is specified, BQ tables should be created if they do not exist. This
  // method detects
  // errors on table operations, and attempts to create the table if necessary.
  static void createTableWrapper(Callable<Void> action, Callable<Boolean> tryCreateTable)
      throws Exception {
    BackOff backoff = BackOffAdapter.toGcpBackOff(DEFAULT_BACKOFF_FACTORY.backoff());
    RuntimeException lastException = null;
    do {
      try {
        action.call();
        return;
      } catch (ApiException | StatusRuntimeException e) {
        lastException = e;
        // TODO: Once BigQuery reliably returns a consistent error on table not found, we should
        // only try creating
        // the table on that error.
        boolean created = tryCreateTable.call();
        if (!created) {
          throw e;
        }
      }
    } while (BackOffUtils.next(com.google.api.client.util.Sleeper.DEFAULT, backoff));
    throw Preconditions.checkStateNotNull(lastException);
  }

  static TableDestination possiblyCreateTable(
      BigQueryOptions bigQueryOptions,
      TableDestination tableDestination,
      Supplier<@Nullable TableSchema> schemaSupplier,
      Supplier<@Nullable TableConstraints> tableConstraintsSupplier,
      Supplier<@Nullable TableReference> cloneSourceSupplier,
      CreateDisposition createDisposition,
      @Nullable Coder<?> tableDestinationCoder,
      @Nullable String kmsKey,
      BigQueryServices bqServices,
      @Nullable Map<String, String> bigLakeConfiguration) {
    checkArgument(
        tableDestination.getTableSpec() != null,
        "DynamicDestinations.getTable() must return a TableDestination "
            + "with a non-null table spec, but %s "
            + "has a null table spec",
        tableDestination);
    boolean destinationCoderSupportsClustering =
        !(tableDestinationCoder instanceof TableDestinationCoderV2);
    checkArgument(
        tableDestination.getClustering() == null || destinationCoderSupportsClustering,
        "DynamicDestinations.getTable() may only return destinations with clustering configured"
            + " if a destination coder is supplied that supports clustering, but %s is configured"
            + " to use TableDestinationCoderV2. Set withClustering() on BigQueryIO.write() and, "
            + " if you provided a custom DynamicDestinations instance, override"
            + " getDestinationCoder() to return TableDestinationCoderV3.",
        tableDestination);
    TableReference tableReference = tableDestination.getTableReference().clone();
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      tableReference.setProjectId(bigQueryOptions.getProject());
      tableDestination = tableDestination.withTableReference(tableReference);
    }
    if (createDisposition == CreateDisposition.CREATE_NEVER) {
      return tableDestination;
    }

    String tableSpec = BigQueryHelpers.stripPartitionDecorator(tableDestination.getTableSpec());
    if (!createdTables.contains(tableSpec)) {
      // Another thread may have succeeded in creating the table in the meanwhile, so
      // check again. This check isn't needed for correctness, but we add it to prevent
      // every thread from attempting a create and overwhelming our BigQuery quota.
      synchronized (createdTables) {
        if (!createdTables.contains(tableSpec)) {
          tryCreateTable(
              bigQueryOptions,
              schemaSupplier,
              tableConstraintsSupplier,
              cloneSourceSupplier,
              tableDestination,
              createDisposition,
              tableSpec,
              kmsKey,
              bqServices,
              bigLakeConfiguration);
        }
      }
    }
    return tableDestination;
  }

  private static void tryCreateTable(
      BigQueryOptions options,
      Supplier<@Nullable TableSchema> schemaSupplier,
      Supplier<@Nullable TableConstraints> tableConstraintsSupplier,
      Supplier<@Nullable TableReference> cloneSourceSupplier,
      TableDestination tableDestination,
      CreateDisposition createDisposition,
      String tableSpec,
      @Nullable String kmsKey,
      BigQueryServices bqServices,
      @Nullable Map<String, String> bigLakeConfiguration) {
    TableReference tableReference = tableDestination.getTableReference().clone();
    tableReference.setTableId(BigQueryHelpers.stripPartitionDecorator(tableReference.getTableId()));
    try (DatasetService datasetService = bqServices.getDatasetService(options)) {
      if (datasetService.getTable(
              tableReference, Collections.emptyList(), DatasetService.TableMetadataView.BASIC)
          == null) {
        @Nullable TableReference cloneSource = cloneSourceSupplier.get();
        if (cloneSource != null) {
          cloneTable(
              options, datasetService, bqServices, cloneSource, tableReference, kmsKey, tableSpec);
        } else {
          TableSchema tableSchema = schemaSupplier.get();
          @Nullable TableConstraints tableConstraints = tableConstraintsSupplier.get();
          Preconditions.checkArgumentNotNull(
              tableSchema,
              "Unless create disposition is %s, a schema or clone source must be specified, i.e. "
                  + "DynamicDestinations.getSchema() and DynamicDestinations.getCloneSource() may "
                  + "not both return null. However, create disposition is %s, and "
                  + "no schema or clone source was returned for destination %s",
              CreateDisposition.CREATE_NEVER,
              createDisposition,
              tableDestination);
          Table table = new Table().setTableReference(tableReference).setSchema(tableSchema);

          if (tableConstraints != null) {
            table = table.setTableConstraints(tableConstraints);
          }

          String tableDescription = tableDestination.getTableDescription();
          if (tableDescription != null) {
            table = table.setDescription(tableDescription);
          }

          TimePartitioning timePartitioning = tableDestination.getTimePartitioning();
          if (timePartitioning != null) {
            table.setTimePartitioning(timePartitioning);
          }

          Clustering clustering = tableDestination.getClustering();
          if (clustering != null) {
            table.setClustering(clustering);
          }

          if (kmsKey != null) {
            table.setEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKey));
          }
          if (bigLakeConfiguration != null) {
            TableReference ref = table.getTableReference();
            table.setBiglakeConfiguration(
                new BigLakeConfiguration()
                    .setTableFormat(
                        MoreObjects.firstNonNull(
                            bigLakeConfiguration.get("tableFormat"), "iceberg"))
                    .setFileFormat(
                        MoreObjects.firstNonNull(bigLakeConfiguration.get("fileFormat"), "parquet"))
                    .setConnectionId(
                        Preconditions.checkArgumentNotNull(bigLakeConfiguration.get(CONNECTION_ID)))
                    .setStorageUri(
                        String.format(
                            "%s/%s/%s/%s",
                            Preconditions.checkArgumentNotNull(
                                bigLakeConfiguration.get(STORAGE_URI)),
                            ref.getProjectId(),
                            ref.getDatasetId(),
                            ref.getTableId())));
          }
          datasetService.createTable(table);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    createdTables.add(tableSpec);
  }

  private static void cloneTable(
      BigQueryOptions options,
      DatasetService datasetService,
      BigQueryServices bqServices,
      TableReference cloneSource,
      TableReference destination,
      @Nullable String kmsKey,
      String tableSpec)
      throws Exception {
    TableReference destinationWithDefaultProject = withDefaultProject(options, destination);
    TableReference source =
        withDefaultProject(options, cloneSource, destinationWithDefaultProject.getProjectId());
    JobConfigurationTableCopy copyConfig =
        new JobConfigurationTableCopy()
            .setSourceTables(Collections.singletonList(source))
            .setDestinationTable(destinationWithDefaultProject)
            .setOperationType("CLONE")
            .setWriteDisposition(WriteDisposition.WRITE_EMPTY.name())
            .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED.name());
    if (kmsKey != null) {
      copyConfig.setDestinationEncryptionConfiguration(
          new EncryptionConfiguration().setKmsKeyName(kmsKey));
    }

    String jobProjectId = destinationWithDefaultProject.getProjectId();
    String bqLocation =
        BigQueryHelpers.getDatasetLocation(
            datasetService,
            destinationWithDefaultProject.getProjectId(),
            destinationWithDefaultProject.getDatasetId());
    String jobIdPrefix = createCloneJobIdPrefix(options, source, destinationWithDefaultProject);
    try (JobService jobService = bqServices.getJobService(options)) {
      BigQueryHelpers.PendingJob cloneJob =
          new BigQueryHelpers.PendingJob(
              jobId -> {
                JobReference jobRef =
                    createJobReference(jobId.getJobId(), jobProjectId, bqLocation);
                try {
                  jobService.startCopyJob(jobRef, copyConfig);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
                return null;
              },
              jobId -> {
                JobReference jobRef =
                    createJobReference(jobId.getJobId(), jobProjectId, bqLocation);
                try {
                  return jobService.pollJob(jobRef, BatchLoads.LOAD_JOB_POLL_MAX_RETRIES);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              },
              jobId -> {
                JobReference jobRef =
                    createJobReference(jobId.getJobId(), jobProjectId, bqLocation);
                try {
                  return jobService.getJob(jobRef);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              },
              BatchLoads.DEFAULT_MAX_RETRY_JOBS,
              jobIdPrefix);
      cloneJob.runJob();
      new BigQueryHelpers.PendingJobManager()
          .addPendingJob(
              cloneJob,
              job -> {
                LOG.info("Created BigQuery clone table {} from {}", tableSpec, source);
                return null;
              })
          .waitForDone();
    }
  }

  private static JobReference createJobReference(String jobId, String projectId, String location) {
    return new JobReference().setProjectId(projectId).setJobId(jobId).setLocation(location);
  }

  private static TableReference withDefaultProject(
      BigQueryOptions options, TableReference tableReference) {
    return withDefaultProject(options, tableReference, null);
  }

  private static TableReference withDefaultProject(
      BigQueryOptions options, TableReference tableReference, @Nullable String defaultProjectId) {
    TableReference updated = tableReference.clone();
    if (Strings.isNullOrEmpty(updated.getProjectId())) {
      if (defaultProjectId != null && !defaultProjectId.isEmpty()) {
        updated.setProjectId(defaultProjectId);
      } else {
        @Nullable String projectId = options.getBigQueryProject();
        if (projectId == null || projectId.isEmpty()) {
          projectId = options.getProject();
        }
        if (projectId != null && !projectId.isEmpty()) {
          updated.setProjectId(projectId);
        }
      }
    }
    return updated;
  }

  private static String createCloneJobIdPrefix(
      BigQueryOptions options, TableReference source, TableReference destination) {
    String jobName = MoreObjects.firstNonNull(options.getJobName(), "beam_bq_clone");
    String stepToken =
        Hashing.murmur3_128()
            .hashUnencodedChars(
                BigQueryHelpers.toJsonString(source) + BigQueryHelpers.toJsonString(destination))
            .toString();
    return BigQueryResourceNaming.createJobIdPrefix(jobName, stepToken, JobType.CLONE);
  }

  @VisibleForTesting
  static void clearCreatedTables() {
    synchronized (createdTables) {
      createdTables.clear();
    }
  }
}
