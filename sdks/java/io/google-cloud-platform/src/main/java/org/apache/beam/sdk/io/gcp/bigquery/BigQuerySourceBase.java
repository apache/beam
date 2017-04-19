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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link BoundedSource} to read a table from BigQuery.
 *
 * <p>This source uses a BigQuery export job to take a snapshot of the table on GCS, and then
 * reads in parallel from each produced file. It is implemented by {@link BigQueryTableSource},
 * and {@link BigQueryQuerySource}, depending on the configuration of the read.
 * Specifically,
 * <ul>
 * <li>{@link BigQueryTableSource} is for reading BigQuery tables</li>
 * <li>{@link BigQueryQuerySource} is for querying BigQuery tables</li>
 * </ul>
 * ...
 */
abstract class BigQuerySourceBase extends BoundedSource<TableRow> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceBase.class);

  // The maximum number of retries to poll a BigQuery job.
  protected static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  protected final ValueProvider<String> jobIdToken;
  protected final String extractDestinationDir;
  protected final BigQueryServices bqServices;
  protected final ValueProvider<String> executingProject;

  private List<BoundedSource<TableRow>> cachedSplitResult;

  BigQuerySourceBase(
      ValueProvider<String> jobIdToken,
      String extractDestinationDir,
      BigQueryServices bqServices,
      ValueProvider<String> executingProject) {
    this.jobIdToken = checkNotNull(jobIdToken, "jobIdToken");
    this.extractDestinationDir = checkNotNull(extractDestinationDir, "extractDestinationDir");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.executingProject = checkNotNull(executingProject, "executingProject");
  }

  @Override
  public List<BoundedSource<TableRow>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // split() can be called multiple times, e.g. Dataflow runner may call it multiple times
    // with different desiredBundleSizeBytes in case the split() call produces too many sources.
    // We ignore desiredBundleSizeBytes anyway, however in any case, we should not initiate
    // another BigQuery extract job for the repeated split() calls.
    if (cachedSplitResult == null) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReference tableToExtract = getTableToExtract(bqOptions);
      JobService jobService = bqServices.getJobService(bqOptions);
      String extractJobId = BigQueryIO.getExtractJobId(jobIdToken);
      List<String> tempFiles = executeExtract(extractJobId, tableToExtract, jobService);

      TableSchema tableSchema = bqServices.getDatasetService(bqOptions)
          .getTable(tableToExtract).getSchema();

      cleanupTempResource(bqOptions);
      cachedSplitResult = checkNotNull(createSources(tempFiles, tableSchema));
    }
    return cachedSplitResult;
  }

  protected abstract TableReference getTableToExtract(BigQueryOptions bqOptions) throws Exception;

  protected abstract void cleanupTempResource(BigQueryOptions bqOptions) throws Exception;

  @Override
  public void validate() {
    // Do nothing, validation is done in BigQuery.Read.
  }

  @Override
  public Coder<TableRow> getDefaultOutputCoder() {
    return TableRowJsonCoder.of();
  }

  private List<String> executeExtract(
      String jobId, TableReference table, JobService jobService)
          throws InterruptedException, IOException {
    JobReference jobRef = new JobReference()
        .setProjectId(executingProject.get())
        .setJobId(jobId);

    String destinationUri = BigQueryIO.getExtractDestinationUri(extractDestinationDir);
    JobConfigurationExtract extract = new JobConfigurationExtract()
        .setSourceTable(table)
        .setDestinationFormat("AVRO")
        .setDestinationUris(ImmutableList.of(destinationUri));

    LOG.info("Starting BigQuery extract job: {}", jobId);
    jobService.startExtractJob(jobRef, extract);
    Job extractJob =
        jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
    if (BigQueryHelpers.parseStatus(extractJob) != Status.SUCCEEDED) {
      throw new IOException(String.format(
          "Extract job %s failed, status: %s.",
          extractJob.getJobReference().getJobId(),
          BigQueryHelpers.statusToPrettyString(extractJob.getStatus())));
    }

    List<String> tempFiles = BigQueryIO.getExtractFilePaths(extractDestinationDir, extractJob);
    return ImmutableList.copyOf(tempFiles);
  }

  private List<BoundedSource<TableRow>> createSources(
      List<String> files, TableSchema tableSchema) throws IOException, InterruptedException {
    final String jsonSchema = BigQueryIO.JSON_FACTORY.toString(tableSchema);

    SerializableFunction<GenericRecord, TableRow> function =
        new SerializableFunction<GenericRecord, TableRow>() {
          @Override
          public TableRow apply(GenericRecord input) {
            return BigQueryAvroUtils.convertGenericRecordToTableRow(
                input, BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class));
          }};

    List<BoundedSource<TableRow>> avroSources = Lists.newArrayList();
    for (String fileName : files) {
      avroSources.add(new TransformingSource<>(
          AvroSource.from(fileName), function, getDefaultOutputCoder()));
    }
    return ImmutableList.copyOf(avroSources);
  }

  protected static class BigQueryReader extends BoundedReader<TableRow> {
    private final BigQuerySourceBase source;
    private final BigQueryServices.BigQueryJsonReader reader;

    BigQueryReader(
        BigQuerySourceBase source, BigQueryServices.BigQueryJsonReader reader) {
      this.source = source;
      this.reader = reader;
    }

    @Override
    public BoundedSource<TableRow> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      return reader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public TableRow getCurrent() throws NoSuchElementException {
      return reader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
