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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.PendingJob;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.PendingJobManager;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes partitions to BigQuery tables.
 *
 * <p>The input is a list of files corresponding to each partition of a table. These files are
 * loaded into a temporary table (or into the final table if there is only one partition). The
 * output is a {@link KV} mapping each final table to a list of the temporary tables containing its
 * data.
 *
 * <p>In the case where all the data in the files fit into a single load job, this transform loads
 * the data directly into the final table, skipping temporary tables. In this case, the output
 * {@link KV} maps the final table to itself.
 */
class WriteTables<DestinationT>
    extends PTransform<
        PCollection<KV<ShardedKey<DestinationT>, List<String>>>,
        PCollection<KV<TableDestination, String>>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteTables.class);

  private final boolean tempTable;
  private final BigQueryServices bqServices;
  private final PCollectionView<String> loadJobIdPrefixView;
  private final WriteDisposition firstPaneWriteDisposition;
  private final CreateDisposition firstPaneCreateDisposition;
  private final Set<SchemaUpdateOption> schemaUpdateOptions;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private final List<PCollectionView<?>> sideInputs;
  private final TupleTag<KV<TableDestination, String>> mainOutputTag;
  private final TupleTag<String> temporaryFilesTag;
  private final ValueProvider<String> loadJobProjectId;
  private final int maxRetryJobs;
  private final boolean ignoreUnknownValues;
  private final @Nullable String kmsKey;
  private final String sourceFormat;
  private final boolean useAvroLogicalTypes;

  private class WriteTablesDoFn
      extends DoFn<KV<ShardedKey<DestinationT>, List<String>>, KV<TableDestination, String>> {
    private Map<DestinationT, String> jsonSchemas = Maps.newHashMap();

    // Represents a pending BigQuery load job.
    private class PendingJobData {
      final BoundedWindow window;
      final BigQueryHelpers.PendingJob retryJob;
      final List<String> partitionFiles;
      final TableDestination tableDestination;
      final TableReference tableReference;

      public PendingJobData(
          BoundedWindow window,
          BigQueryHelpers.PendingJob retryJob,
          List<String> partitionFiles,
          TableDestination tableDestination,
          TableReference tableReference) {
        this.window = window;
        this.retryJob = retryJob;
        this.partitionFiles = partitionFiles;
        this.tableDestination = tableDestination;
        this.tableReference = tableReference;
      }
    }
    // All pending load jobs.
    private List<PendingJobData> pendingJobs = Lists.newArrayList();

    @StartBundle
    public void startBundle(StartBundleContext c) {
      // Clear the map on each bundle so we can notice side-input updates.
      // (alternative is to use a cache with a TTL).
      jsonSchemas.clear();
      pendingJobs.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      DestinationT destination = c.element().getKey().getKey();
      TableSchema tableSchema;
      if (firstPaneCreateDisposition == CreateDisposition.CREATE_NEVER) {
        tableSchema = null;
      } else if (jsonSchemas.containsKey(destination)) {
        tableSchema =
            BigQueryHelpers.fromJsonString(jsonSchemas.get(destination), TableSchema.class);
      } else {
        tableSchema = dynamicDestinations.getSchema(destination);
        checkArgument(
            tableSchema != null,
            "Unless create disposition is %s, a schema must be specified, i.e. "
                + "DynamicDestinations.getSchema() may not return null. "
                + "However, create disposition is %s, and %s returned null for destination %s",
            CreateDisposition.CREATE_NEVER,
            firstPaneCreateDisposition,
            dynamicDestinations,
            destination);
        jsonSchemas.put(destination, BigQueryHelpers.toJsonString(tableSchema));
      }

      TableDestination tableDestination = dynamicDestinations.getTable(destination);
      checkArgument(
          tableDestination != null,
          "DynamicDestinations.getTable() may not return null, "
              + "but %s returned null for destination %s",
          dynamicDestinations,
          destination);
      boolean destinationCoderSupportsClustering =
          !(dynamicDestinations.getDestinationCoder() instanceof TableDestinationCoderV2);
      checkArgument(
          tableDestination.getClustering() == null || destinationCoderSupportsClustering,
          "DynamicDestinations.getTable() may only return destinations with clustering configured"
              + " if a destination coder is supplied that supports clustering, but %s is configured"
              + " to use TableDestinationCoderV2. Set withClustering() on BigQueryIO.write() and, "
              + " if you provided a custom DynamicDestinations instance, override"
              + " getDestinationCoder() to return TableDestinationCoderV3.",
          dynamicDestinations);
      TableReference tableReference = tableDestination.getTableReference();
      if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
        tableReference.setProjectId(c.getPipelineOptions().as(BigQueryOptions.class).getProject());
        tableDestination = tableDestination.withTableReference(tableReference);
      }

      Integer partition = c.element().getKey().getShardNumber();
      List<String> partitionFiles = Lists.newArrayList(c.element().getValue());
      String jobIdPrefix =
          BigQueryResourceNaming.createJobIdWithDestination(
              c.sideInput(loadJobIdPrefixView), tableDestination, partition, c.pane().getIndex());

      if (tempTable) {
        // This is a temp table. Create a new one for each partition and each pane.
        tableReference.setTableId(jobIdPrefix);
      }

      WriteDisposition writeDisposition = firstPaneWriteDisposition;
      CreateDisposition createDisposition = firstPaneCreateDisposition;
      if (c.pane().getIndex() > 0 && !tempTable) {
        // If writing directly to the destination, then the table is created on the first write
        // and we should change the disposition for subsequent writes.
        writeDisposition = WriteDisposition.WRITE_APPEND;
        createDisposition = CreateDisposition.CREATE_NEVER;
      } else if (tempTable) {
        // In this case, we are writing to a temp table and always need to create it.
        // WRITE_TRUNCATE is set so that we properly handle retries of this pane.
        writeDisposition = WriteDisposition.WRITE_TRUNCATE;
        createDisposition = CreateDisposition.CREATE_IF_NEEDED;
      }

      BigQueryHelpers.PendingJob retryJob =
          startLoad(
              bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
              bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
              jobIdPrefix,
              tableReference,
              tableDestination.getTimePartitioning(),
              tableDestination.getClustering(),
              tableSchema,
              partitionFiles,
              writeDisposition,
              createDisposition,
              schemaUpdateOptions);
      pendingJobs.add(
          new PendingJobData(window, retryJob, partitionFiles, tableDestination, tableReference));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("launchesBigQueryJobs", true)
              .withLabel("This transform launches BigQuery jobs to read/write elements."));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      DatasetService datasetService =
          bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class));

      PendingJobManager jobManager = new PendingJobManager();
      for (PendingJobData pendingJob : pendingJobs) {
        jobManager =
            jobManager.addPendingJob(
                pendingJob.retryJob,
                // Lambda called when the job is done.
                j -> {
                  try {
                    if (pendingJob.tableDestination.getTableDescription() != null) {
                      TableReference ref = pendingJob.tableReference;
                      datasetService.patchTableDescription(
                          ref.clone()
                              .setTableId(
                                  BigQueryHelpers.stripPartitionDecorator(ref.getTableId())),
                          pendingJob.tableDestination.getTableDescription());
                    }
                    c.output(
                        mainOutputTag,
                        KV.of(
                            pendingJob.tableDestination,
                            BigQueryHelpers.toJsonString(pendingJob.tableReference)),
                        pendingJob.window.maxTimestamp(),
                        pendingJob.window);
                    for (String file : pendingJob.partitionFiles) {
                      c.output(
                          temporaryFilesTag,
                          file,
                          pendingJob.window.maxTimestamp(),
                          pendingJob.window);
                    }
                    return null;
                  } catch (IOException | InterruptedException e) {
                    return e;
                  }
                });
      }
      jobManager.waitForDone();
    }
  }

  private static class GarbageCollectTemporaryFiles extends DoFn<Iterable<String>, Void> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      removeTemporaryFiles(c.element());
    }
  }

  public WriteTables(
      boolean tempTable,
      BigQueryServices bqServices,
      PCollectionView<String> loadJobIdPrefixView,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      List<PCollectionView<?>> sideInputs,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      @Nullable ValueProvider<String> loadJobProjectId,
      int maxRetryJobs,
      boolean ignoreUnknownValues,
      String kmsKey,
      String sourceFormat,
      boolean useAvroLogicalTypes,
      Set<SchemaUpdateOption> schemaUpdateOptions) {

    this.tempTable = tempTable;
    this.bqServices = bqServices;
    this.loadJobIdPrefixView = loadJobIdPrefixView;
    this.firstPaneWriteDisposition = writeDisposition;
    this.firstPaneCreateDisposition = createDisposition;
    this.sideInputs = sideInputs;
    this.dynamicDestinations = dynamicDestinations;
    this.mainOutputTag = new TupleTag<>("WriteTablesMainOutput");
    this.temporaryFilesTag = new TupleTag<>("TemporaryFiles");
    this.loadJobProjectId = loadJobProjectId;
    this.maxRetryJobs = maxRetryJobs;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.kmsKey = kmsKey;
    this.sourceFormat = sourceFormat;
    this.useAvroLogicalTypes = useAvroLogicalTypes;
    this.schemaUpdateOptions = schemaUpdateOptions;
  }

  @Override
  public PCollection<KV<TableDestination, String>> expand(
      PCollection<KV<ShardedKey<DestinationT>, List<String>>> input) {
    PCollectionTuple writeTablesOutputs =
        input.apply(
            ParDo.of(new WriteTablesDoFn())
                .withSideInputs(sideInputs)
                .withOutputTags(mainOutputTag, TupleTagList.of(temporaryFilesTag)));

    // Garbage collect temporary files.
    // We mustn't start garbage collecting files until we are assured that the WriteTablesDoFn has
    // succeeded in loading those files and won't be retried. Otherwise, we might fail part of the
    // way through deleting temporary files, and retry WriteTablesDoFn. This will then fail due
    // to missing files, causing either the entire workflow to fail or get stuck (depending on how
    // the runner handles persistent failures).
    writeTablesOutputs
        .get(temporaryFilesTag)
        .setCoder(StringUtf8Coder.of())
        .apply(WithKeys.of((Void) null))
        .setCoder(KvCoder.of(VoidCoder.of(), StringUtf8Coder.of()))
        .apply(
            Window.<KV<Void, String>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes())
        .apply(GroupByKey.create())
        .apply(Values.create())
        .apply(ParDo.of(new GarbageCollectTemporaryFiles()));

    return writeTablesOutputs.get(mainOutputTag);
  }

  private PendingJob startLoad(
      JobService jobService,
      DatasetService datasetService,
      String jobIdPrefix,
      TableReference ref,
      TimePartitioning timePartitioning,
      Clustering clustering,
      @Nullable TableSchema schema,
      List<String> gcsUris,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      Set<SchemaUpdateOption> schemaUpdateOptions) {
    JobConfigurationLoad loadConfig =
        new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSchema(schema)
            .setSourceUris(gcsUris)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name())
            .setSourceFormat(sourceFormat)
            .setIgnoreUnknownValues(ignoreUnknownValues)
            .setUseAvroLogicalTypes(useAvroLogicalTypes);
    if (schemaUpdateOptions != null) {
      List<String> options =
          schemaUpdateOptions.stream()
              .map(Enum<SchemaUpdateOption>::name)
              .collect(Collectors.toList());
      loadConfig.setSchemaUpdateOptions(options);
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
    String projectId = loadJobProjectId == null ? ref.getProjectId() : loadJobProjectId.get();
    String bqLocation =
        BigQueryHelpers.getDatasetLocation(datasetService, ref.getProjectId(), ref.getDatasetId());

    PendingJob retryJob =
        new PendingJob(
            // Function to load the data.
            jobId -> {
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(projectId)
                      .setJobId(jobId.getJobId())
                      .setLocation(bqLocation);
              LOG.info(
                  "Loading {} files into {} using job {}, job id iteration {}",
                  gcsUris.size(),
                  ref,
                  jobRef,
                  jobId.getRetryIndex());
              try {
                jobService.startLoadJob(jobRef, loadConfig);
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

  static void removeTemporaryFiles(Iterable<String> files) throws IOException {
    ImmutableList.Builder<ResourceId> fileResources = ImmutableList.builder();
    for (String file : files) {
      fileResources.add(FileSystems.matchNewResource(file, false /* isDirectory */));
    }
    FileSystems.delete(fileResources.build());
  }
}
