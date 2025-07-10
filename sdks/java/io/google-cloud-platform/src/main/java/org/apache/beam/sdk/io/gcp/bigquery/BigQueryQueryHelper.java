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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.createTempTableReference;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper object for executing query jobs in BigQuery.
 *
 * <p>This object is not serializable, and its state can be safely discarded across serialization
 * boundaries for any associated source objects.
 */
class BigQueryQueryHelper {

  private static final Integer JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQueryHelper.class);

  public static JobStatistics dryRunQueryIfNeeded(
      BigQueryServices bqServices,
      BigQueryOptions options,
      AtomicReference<@Nullable JobStatistics> dryRunJobStats,
      String query,
      Boolean flattenResults,
      Boolean useLegacySql,
      @Nullable String location)
      throws InterruptedException, IOException {
    @Nullable JobStatistics maybeJobStatistics = dryRunJobStats.get();

    if (maybeJobStatistics != null) {
      return maybeJobStatistics;
    } else {
      JobStatistics jobStatistics =
          bqServices
              .getJobService(options)
              .dryRunQuery(
                  options.getBigQueryProject() == null
                      ? options.getProject()
                      : options.getBigQueryProject(),
                  createBasicQueryConfig(query, flattenResults, useLegacySql),
                  location);
      dryRunJobStats.compareAndSet(null, jobStatistics);
      return jobStatistics;
    }
  }

  public static TableReference executeQuery(
      BigQueryServices bqServices,
      BigQueryOptions options,
      AtomicReference<@Nullable JobStatistics> dryRunJobStats,
      String stepUuid,
      String query,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String queryTempDatasetId,
      @Nullable String queryTempProjectId,
      @Nullable String kmsKey)
      throws InterruptedException, IOException {
    // Step 1: Find the effective location of the query.
    @Nullable String effectiveLocation = location;
    try (DatasetService tableService = bqServices.getDatasetService(options)) {
      if (effectiveLocation == null) {
        List<TableReference> referencedTables =
            dryRunQueryIfNeeded(
                    bqServices,
                    options,
                    dryRunJobStats,
                    query,
                    flattenResults,
                    useLegacySql,
                    location)
                .getQuery()
                .getReferencedTables();
        if (referencedTables != null && !referencedTables.isEmpty()) {
          TableReference referencedTable = referencedTables.get(0);
          effectiveLocation =
              tableService
                  .getDataset(referencedTable.getProjectId(), referencedTable.getDatasetId())
                  .getLocation();
        }
      }

      // Step 2: Create a temporary dataset in the query location only if the user has not specified
      // a temp dataset. The temp table name may be deterministic but the query job ID has to be
      // non-deterministic to protect against retries. If BigQuery sees a repeated query job ID, it
      // will be skipped.
      String tempTableID =
          BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.QUERY);
      String queryJobId =
          BigQueryResourceNaming.createJobIdPrefix(
              options.getJobName(), stepUuid, JobType.QUERY, BigQueryHelpers.randomUUIDString());
      Optional<String> queryTempDatasetOpt = Optional.ofNullable(queryTempDatasetId);
      String project = queryTempProjectId;
      if (project == null) {
        project =
            options.getBigQueryProject() == null
                ? options.getProject()
                : options.getBigQueryProject();
      }
      TableReference queryResultTable =
          createTempTableReference(project, tempTableID, queryTempDatasetOpt);

      boolean beamToCreateTempDataset = !queryTempDatasetOpt.isPresent();
      // Create dataset only if it has not been set by the user
      if (beamToCreateTempDataset) {
        LOG.info(
            "Creating temporary dataset {} for query results", queryResultTable.getDatasetId());

        tableService.createDataset(
            queryResultTable.getProjectId(),
            queryResultTable.getDatasetId(),
            effectiveLocation,
            "Temporary tables for query results of job " + options.getJobName(),
            TimeUnit.DAYS.toMillis(1));
      } else { // If the user specified a temp dataset, check that the destination table does not
        // exist
        Table destTable = tableService.getTable(queryResultTable);
        checkArgument(
            destTable == null,
            "Refusing to write on existing table %s in the specified temp dataset %s",
            queryResultTable.getTableId(),
            queryResultTable.getDatasetId());
      }

      // Step 3: Execute the query. Generate a transient (random) query job ID, because this code
      // may be retried after the temporary dataset and table have been deleted by a previous
      // attempt -- in that case, we want to regenerate the temporary dataset and table, and we'll
      // need a fresh query ID to do that.
      LOG.info(
          "Exporting query results into temporary table {} using job {}",
          queryResultTable,
          queryJobId);

      @SuppressWarnings("nullness") // setLocation is not annotated, but does accept nulls
      JobReference jobReference =
          new JobReference()
              .setProjectId(
                  options.getBigQueryProject() == null
                      ? options.getProject()
                      : options.getBigQueryProject())
              .setLocation(effectiveLocation)
              .setJobId(queryJobId);

      JobConfigurationQuery queryConfiguration =
          createBasicQueryConfig(query, flattenResults, useLegacySql)
              .setAllowLargeResults(true)
              .setDestinationTable(queryResultTable)
              .setCreateDisposition("CREATE_IF_NEEDED")
              .setWriteDisposition("WRITE_TRUNCATE")
              .setPriority(priority.name());

      if (kmsKey != null) {
        queryConfiguration.setDestinationEncryptionConfiguration(
            new EncryptionConfiguration().setKmsKeyName(kmsKey));
      }

      JobService jobService = bqServices.getJobService(options);
      jobService.startQueryJob(jobReference, queryConfiguration);
      Job job = jobService.pollJob(jobReference, JOB_POLL_MAX_RETRIES);
      if (BigQueryHelpers.parseStatus(job) != Status.SUCCEEDED) {
        throw new IOException(
            String.format(
                "Query job %s failed, status: %s",
                queryJobId, BigQueryHelpers.statusToPrettyString(job.getStatus())));
      }

      LOG.info("Query job {} completed", queryJobId);
      return queryResultTable;

    } catch (RuntimeException | IOException | InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static JobConfigurationQuery createBasicQueryConfig(
      String query, Boolean flattenResults, Boolean useLegacySql) {
    return new JobConfigurationQuery()
        .setQuery(query)
        .setFlattenResults(flattenResults)
        .setUseLegacySql(useLegacySql);
  }
}
