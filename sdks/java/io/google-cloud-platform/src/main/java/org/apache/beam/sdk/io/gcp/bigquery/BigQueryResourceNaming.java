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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.BIGQUERY_JOB_TEMPLATE;

import com.google.api.services.bigquery.model.TableReference;
import java.util.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 * This class contains utilities to standardize how resources are named by BigQueryIO.
 *
 * <p>Resources can be any type of BigQuery job started by BigQueryIO and temporary BigQuery
 * resources created by BigQueryIO.
 *
 * <p>Some examples are: - BigQuery jobs - Export jobs - Query jobs - Load jobs - Copy jobs -
 * Temporary datasets - Temporary tables.
 *
 * <p>BigQuery resource naming follows this conveniton: {@code
 * "beam_bq_job_{TYPE}_{JOB_ID}_{STEP}_{RANDOM}"}.
 *
 * <p>This class has no backwards compatibility guaantees. It is considered internal.
 */
class BigQueryResourceNaming {

  /**
   * Generate a BigQuery job ID based on a prefix from {@link
   * BigQueryResourceNaming::createJobIdPrefix}, with destination information added to it.
   *
   * @param prefix A prefix generated in {@link BigQueryResourceNaming::createJobIdPrefix}.
   * @param tableDestination A descriptor of the destination table.
   * @param partition A partition number in the destination table.
   * @param index
   * @return
   */
  static String createJobIdWithDestination(
      String prefix, TableDestination tableDestination, int partition, long index) {
    // Job ID must be different for each partition of each table.
    String destinationHash =
        Hashing.murmur3_128().hashUnencodedChars(tableDestination.toString()).toString();
    String jobId = String.format("%s_%s", prefix, destinationHash);
    if (partition >= 0) {
      jobId += String.format("_%05d", partition);
    }
    if (index >= 0) {
      jobId += String.format("_%05d", index);
    }
    return jobId;
  }

  public enum JobType {
    LOAD,
    COPY,
    EXPORT,
    QUERY,
  }

  /**
   * Generate a name to be used for BigQuery jobs. The name can be used as-is, or as a prefix for BQ
   * job names that have destinations appended to them.
   *
   * @param jobName The name of the Apache Beam job.
   * @param stepUuid A uuid representing the step from which the job is launched
   * @param type The job type.
   * @param random A random string to use when naming jobs. If no random string is provided, then
   *     the parameter will be ignored.
   * @return
   */
  static String createJobIdPrefix(String jobName, String stepUuid, JobType type, String random) {
    jobName = jobName.replaceAll("-", "");
    String result =
        BIGQUERY_JOB_TEMPLATE
            .replaceFirst("\\{TYPE}", type.toString())
            .replaceFirst("\\{JOB_ID}", jobName)
            .replaceFirst("\\{STEP}", stepUuid);

    if (random != null) {
      return result.replaceFirst("\\{RANDOM}", random);
    } else {
      return result.replaceFirst("_\\{RANDOM}", "");
    }
  }

  static String createJobIdPrefix(String jobName, String stepUuid, JobType type) {
    return createJobIdPrefix(jobName, stepUuid, type, null);
  }

  static TableReference createTempTableReference(
      String projectId, String jobUuid, Optional<String> tempDatasetIdOpt) {
    String tempDatasetId = tempDatasetIdOpt.orElse("temp_dataset_" + jobUuid);
    String queryTempTableId = "temp_table_" + jobUuid;
    return new TableReference()
        .setProjectId(projectId)
        .setDatasetId(tempDatasetId)
        .setTableId(queryTempTableId);
  }
}
