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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.getExtractJobId;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.PipelineOptions;
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
abstract class BigQuerySourceBase<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceBase.class);

  // The maximum number of retries to poll a BigQuery job.
  protected static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  protected final String stepUuid;
  protected final BigQueryServices bqServices;

  private transient List<BoundedSource<T>> cachedSplitResult;
  private SerializableFunction<SchemaAndRecord, T> parseFn;
  private Coder<T> coder;

  BigQuerySourceBase(
      String stepUuid,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn
    ) {
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.coder = checkNotNull(coder, "coder");
    this.parseFn = checkNotNull(parseFn, "parseFn");
  }

  protected static class ExtractResult {
    public final TableSchema schema;
    public final List<ResourceId> extractedFiles;

    public ExtractResult(TableSchema schema, List<ResourceId> extractedFiles) {
      this.schema = schema;
      this.extractedFiles = extractedFiles;
    }
  }

  protected ExtractResult extractFiles(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    TableReference tableToExtract = getTableToExtract(bqOptions);
    TableSchema schema =
        bqServices.getDatasetService(bqOptions).getTable(tableToExtract).getSchema();
    JobService jobService = bqServices.getJobService(bqOptions);
    String extractJobId = getExtractJobId(createJobIdToken(options.getJobName(), stepUuid));
    final String extractDestinationDir =
        resolveTempLocation(bqOptions.getTempLocation(), "BigQueryExtractTemp", stepUuid);
    List<ResourceId> tempFiles =
        executeExtract(
            extractJobId,
            tableToExtract,
            jobService,
            bqOptions.getProject(),
            extractDestinationDir);
    return new ExtractResult(schema, tempFiles);
  }

  @Override
  public List<BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // split() can be called multiple times, e.g. Dataflow runner may call it multiple times
    // with different desiredBundleSizeBytes in case the split() call produces too many sources.
    // We ignore desiredBundleSizeBytes anyway, however in any case, we should not initiate
    // another BigQuery extract job for the repeated split() calls.
    if (cachedSplitResult == null) {
      ExtractResult res = extractFiles(options);
      LOG.info("Extract job produced {} files", res.extractedFiles.size());
      cleanupTempResource(options.as(BigQueryOptions.class));
      cachedSplitResult = checkNotNull(createSources(res.extractedFiles, res.schema));
    }
    return cachedSplitResult;
  }

  protected abstract TableReference getTableToExtract(BigQueryOptions bqOptions) throws Exception;

  protected abstract void cleanupTempResource(BigQueryOptions bqOptions) throws Exception;

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("BigQuery source must be split before being read");
  }

  @Override
  public void validate() {
    // Do nothing, validation is done in BigQuery.Read.
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  private List<ResourceId> executeExtract(
      String jobId, TableReference table, JobService jobService, String executingProject,
      String extractDestinationDir)
          throws InterruptedException, IOException {
    JobReference jobRef = new JobReference()
        .setProjectId(executingProject)
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

    LOG.info("BigQuery extract job completed: {}", jobId);

    return BigQueryIO.getExtractFilePaths(extractDestinationDir, extractJob);
  }

  private static class TableSchemaFunction
      implements Serializable, Function<String, TableSchema> {
    @Nullable
    @Override
    public TableSchema apply(@Nullable String input) {
      return BigQueryHelpers.fromJsonString(input, TableSchema.class);
    }
  }

  List<BoundedSource<T>> createSources(List<ResourceId> files, TableSchema schema)
      throws IOException, InterruptedException {

    final String jsonSchema = BigQueryIO.JSON_FACTORY.toString(schema);
    SerializableFunction<GenericRecord, T> fnWrapper =
        new SerializableFunction<GenericRecord, T>() {
          private Supplier<TableSchema> schema = Suppliers.memoize(
              Suppliers.compose(new TableSchemaFunction(), Suppliers.ofInstance(jsonSchema)));

          @Override
          public T apply(GenericRecord input) {
            return parseFn.apply(new SchemaAndRecord(input, schema.get()));
          }
        };
    List<BoundedSource<T>> avroSources = Lists.newArrayList();
    for (ResourceId file : files) {
      avroSources.add(
          AvroSource.from(file.toString()).withParseFn(fnWrapper, getOutputCoder()));
    }
    return ImmutableList.copyOf(avroSources);
  }
}
