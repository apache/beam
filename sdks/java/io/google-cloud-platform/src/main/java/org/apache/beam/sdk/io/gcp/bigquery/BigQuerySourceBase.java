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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.FileSystems.match;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link BoundedSource} to read a table from BigQuery.
 *
 * <p>This source uses a BigQuery export job to take a snapshot of the table on GCS, and then reads
 * in parallel from each produced file. It is implemented by {@link BigQueryTableSource}, and {@link
 * BigQueryQuerySource}, depending on the configuration of the read. Specifically,
 *
 * <ul>
 *   <li>{@link BigQueryTableSource} is for reading BigQuery tables
 *   <li>{@link BigQueryQuerySource} is for querying BigQuery tables
 * </ul>
 *
 * ...
 */
abstract class BigQuerySourceBase<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceBase.class);

  // The maximum number of retries to poll a BigQuery job.
  protected static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  protected final String stepUuid;
  protected final BigQueryServices bqServices;

  private transient @Nullable List<BoundedSource<T>> cachedSplitResult = null;
  private SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory;
  private Coder<T> coder;
  private final boolean useAvroLogicalTypes;

  BigQuerySourceBase(
      String stepUuid,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory,
      boolean useAvroLogicalTypes) {
    this.stepUuid = checkArgumentNotNull(stepUuid, "stepUuid");
    this.bqServices = checkArgumentNotNull(bqServices, "bqServices");
    this.coder = checkArgumentNotNull(coder, "coder");
    this.readerFactory = checkArgumentNotNull(readerFactory, "readerFactory");
    this.useAvroLogicalTypes = useAvroLogicalTypes;
  }

  protected static class ExtractResult {
    public final TableSchema schema;
    public final List<ResourceId> extractedFiles;
    public @Nullable List<MatchResult.Metadata> metadata = null;

    public ExtractResult(TableSchema schema, List<ResourceId> extractedFiles) {
      this(schema, extractedFiles, null);
    }

    public ExtractResult(
        TableSchema schema,
        List<ResourceId> extractedFiles,
        @Nullable List<MatchResult.Metadata> metadata) {
      this.schema = schema;
      this.extractedFiles = extractedFiles;
      this.metadata = metadata;
    }
  }

  protected ExtractResult extractFiles(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    TableReference tableToExtract = getTableToExtract(bqOptions);
    try (BigQueryServices.DatasetService datasetService = bqServices.getDatasetService(bqOptions)) {
      Table table = datasetService.getTable(tableToExtract);
      if (table == null) {
        throw new IOException(
            String.format(
                "Cannot start an export job since table %s does not exist",
                BigQueryHelpers.toTableSpec(tableToExtract)));
      }
      // emit this table ID as a lineage source
      Lineage.getSources()
          .add("bigquery", BigQueryHelpers.dataCatalogSegments(tableToExtract, bqOptions));

      TableSchema schema = table.getSchema();
      JobService jobService = bqServices.getJobService(bqOptions);
      String extractJobId =
          BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.EXPORT);
      final String extractDestinationDir =
          resolveTempLocation(bqOptions.getTempLocation(), "BigQueryExtractTemp", stepUuid);
      String bqLocation =
          BigQueryHelpers.getDatasetLocation(
              datasetService, tableToExtract.getProjectId(), tableToExtract.getDatasetId());
      List<ResourceId> tempFiles =
          executeExtract(
              extractJobId,
              tableToExtract,
              jobService,
              bqOptions.getProject(),
              extractDestinationDir,
              bqLocation,
              useAvroLogicalTypes);
      return new ExtractResult(schema, tempFiles);
    }
  }

  @Override
  public List<BoundedSource<T>> split(long desiredBundleSizeBytes, PipelineOptions options)
      throws Exception {
    // split() can be called multiple times, e.g. Dataflow runner may call it multiple times
    // with different desiredBundleSizeBytes in case the split() call produces too many sources.
    // We ignore desiredBundleSizeBytes anyway, however in any case, we should not initiate
    // another BigQuery extract job for the repeated split() calls.
    if (cachedSplitResult == null) {
      ExtractResult res = extractFiles(options);
      LOG.info("Extract job produced {} files", res.extractedFiles.size());
      if (res.extractedFiles.size() > 0) {
        BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
        final String extractDestinationDir =
            resolveTempLocation(bqOptions.getTempLocation(), "BigQueryExtractTemp", stepUuid);
        // Match all files in the destination directory to stat them in bulk.
        List<MatchResult> matches = match(ImmutableList.of(extractDestinationDir + "*"));
        if (matches.size() > 0) {
          res.metadata = matches.get(0).metadata();
        }
      }
      cleanupTempResource(options.as(BigQueryOptions.class));
      cachedSplitResult = createSources(res.extractedFiles, res.schema, res.metadata);
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
      String jobId,
      TableReference table,
      JobService jobService,
      String executingProject,
      String extractDestinationDir,
      String bqLocation,
      boolean useAvroLogicalTypes)
      throws InterruptedException, IOException {

    JobReference jobRef =
        new JobReference().setProjectId(executingProject).setLocation(bqLocation).setJobId(jobId);

    String destinationUri = BigQueryIO.getExtractDestinationUri(extractDestinationDir);
    JobConfigurationExtract extract =
        new JobConfigurationExtract()
            .setSourceTable(table)
            .setDestinationFormat("AVRO")
            .setUseAvroLogicalTypes(useAvroLogicalTypes)
            .setDestinationUris(ImmutableList.of(destinationUri));

    Job extractJob;
    try {
      LOG.info("Starting BigQuery extract job: {}", jobId);
      jobService.startExtractJob(jobRef, extract);
      extractJob = jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
    } catch (IOException exn) {
      // The error messages thrown in this case are generic and misleading, so leave this breadcrumb
      // in case it's the root cause.
      LOG.warn(
          "Error extracting table: {} "
              + "Note that external tables cannot be exported: "
              + "https://cloud.google.com/bigquery/docs/external-tables#external_table_limitations",
          exn);
      throw exn;
    }
    if (BigQueryHelpers.parseStatus(extractJob) != Status.SUCCEEDED) {
      throw new IOException(
          String.format(
              "Extract job %s failed, status: %s.",
              extractJob.getJobReference().getJobId(),
              BigQueryHelpers.statusToPrettyString(extractJob.getStatus())));
    }

    LOG.info("BigQuery extract job completed: {}", jobId);

    return BigQueryIO.getExtractFilePaths(extractDestinationDir, extractJob);
  }

  List<BoundedSource<T>> createSources(
      List<ResourceId> files, TableSchema schema, @Nullable List<MatchResult.Metadata> metadata)
      throws IOException, InterruptedException {
    String avroSchema =
        BigQueryAvroUtils.toGenericAvroSchema("root", schema.getFields()).toString();

    AvroSource.DatumReaderFactory<T> factory = readerFactory.apply(schema);

    Stream<AvroSource<GenericRecord>> avroSources;
    // If metadata is available, create AvroSources with said metadata in SINGLE_FILE_OR_SUBRANGE
    // mode.
    if (metadata != null) {
      avroSources = metadata.stream().map(AvroSource::from);
    } else {
      avroSources = files.stream().map(ResourceId::toString).map(AvroSource::from);
    }

    return avroSources
        .map(s -> s.withSchema(avroSchema))
        .map(s -> (AvroSource<T>) s.withDatumReaderFactory(factory))
        .map(s -> s.withCoder(coder))
        .collect(collectingAndThen(toList(), ImmutableList::copyOf));
  }
}
