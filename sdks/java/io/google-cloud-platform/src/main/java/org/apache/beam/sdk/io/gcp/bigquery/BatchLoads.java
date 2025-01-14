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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;
import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PTransform that uses BigQuery batch-load jobs to write a PCollection to BigQuery. */
class BatchLoads<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchLoads.class);

  // The maximum number of file writers to keep open in a single bundle at a time, since file
  // writers default to 64mb buffers. This comes into play when writing dynamic table destinations.
  // The first 20 tables from a single BatchLoads transform will write files inline in the
  // transform. Anything beyond that might be shuffled.  Users using this transform directly who
  // know that they are running on workers with sufficient memory can increase this by calling
  // BatchLoads#setMaxNumWritersPerBundle. This allows the workers to do more work in memory, and
  // save on the cost of shuffling some of this data.
  // Keep in mind that specific runners may decide to run multiple bundles in parallel, based on
  // their own policy.
  @VisibleForTesting static final int DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE = 20;

  @VisibleForTesting
  // Maximum number of files in a single partition.
  static final int DEFAULT_MAX_FILES_PER_PARTITION = 10000;

  @VisibleForTesting
  // Maximum number of bytes in a single partition -- 11 TiB just under BQ's 12 TiB limit.
  static final long DEFAULT_MAX_BYTES_PER_PARTITION = 11 * (1L << 40);

  // The maximum size of a single file - 4TiB, just under the 5 TiB limit.
  static final long DEFAULT_MAX_FILE_SIZE = 4 * (1L << 40);

  static final int DEFAULT_NUM_FILE_SHARDS = 0;

  // If user triggering is supplied, we will trigger the file write after this many records are
  // written.
  static final int FILE_TRIGGERING_RECORD_COUNT = 500000;
  // If user triggering is supplied, we will trigger the file write after this many bytes are
  // written.
  static final int DEFAULT_FILE_TRIGGERING_BYTE_COUNT =
      AsyncWriteChannelOptions.UPLOAD_CHUNK_SIZE_DEFAULT; // 64MiB as of now

  // If using auto-sharding for unbounded data, we batch the records before triggering file write
  // to avoid generating too many small files.
  static final Duration FILE_TRIGGERING_BATCHING_DURATION = Duration.standardSeconds(1);

  // The maximum number of retries to poll the status of a job.
  // It sets to {@code Integer.MAX_VALUE} to block until the BigQuery job finishes.
  static final int LOAD_JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  static final int DEFAULT_MAX_RETRY_JOBS = 3;

  private BigQueryServices bigQueryServices;
  private final WriteDisposition writeDisposition;
  private final CreateDisposition createDisposition;
  private Set<SchemaUpdateOption> schemaUpdateOptions;
  private final boolean ignoreUnknownValues;
  private final boolean useAvroLogicalTypes;
  // Indicates that we are writing to a constant single table. If this is the case, we will create
  // the table, even if there is no data in it.
  private final boolean singletonTable;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;

  /**
   * destinationsWithMatching wraps the dynamicDestinations redirects the schema, partitioning, etc
   * to the final destination tables, if the final destination table exists already (and we're
   * appending to it). It is used in writing to temp tables and updating final table schema.
   */
  private DynamicDestinations<?, DestinationT> destinationsWithMatching;

  private final Coder<DestinationT> destinationCoder;
  private int maxNumWritersPerBundle;
  private long maxFileSize;
  private int maxFilesPerPartition;
  private long maxBytesPerPartition;
  private int numFileShards;
  private @Nullable Duration triggeringFrequency;
  private ValueProvider<String> customGcsTempLocation;
  private @Nullable ValueProvider<String> loadJobProjectId;
  private final Coder<ElementT> elementCoder;
  private final RowWriterFactory<ElementT, DestinationT> rowWriterFactory;
  private final @Nullable String kmsKey;
  private final String tempDataset;
  private final BadRecordRouter badRecordRouter;
  private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;
  private Coder<TableDestination> tableDestinationCoder;

  // The maximum number of times to retry failed load or copy jobs.
  private int maxRetryJobs = DEFAULT_MAX_RETRY_JOBS;

  BatchLoads(
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      boolean singletonTable,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      Coder<DestinationT> destinationCoder,
      ValueProvider<String> customGcsTempLocation,
      @Nullable ValueProvider<String> loadJobProjectId,
      boolean ignoreUnknownValues,
      Coder<ElementT> elementCoder,
      RowWriterFactory<ElementT, DestinationT> rowWriterFactory,
      @Nullable String kmsKey,
      boolean clusteringEnabled,
      boolean useAvroLogicalTypes,
      String tempDataset,
      BadRecordRouter badRecordRouter,
      ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
    bigQueryServices = new BigQueryServicesImpl();
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.singletonTable = singletonTable;
    this.dynamicDestinations = dynamicDestinations;
    this.destinationsWithMatching =
        DynamicDestinationsHelpers.matchTableDynamicDestinations(
            dynamicDestinations, bigQueryServices);
    this.destinationCoder = destinationCoder;
    this.maxNumWritersPerBundle = DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE;
    this.maxFileSize = DEFAULT_MAX_FILE_SIZE;
    this.numFileShards = DEFAULT_NUM_FILE_SHARDS;
    this.maxFilesPerPartition = DEFAULT_MAX_FILES_PER_PARTITION;
    this.maxBytesPerPartition = DEFAULT_MAX_BYTES_PER_PARTITION;
    this.triggeringFrequency = null;
    this.customGcsTempLocation = customGcsTempLocation;
    this.loadJobProjectId = loadJobProjectId;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.useAvroLogicalTypes = useAvroLogicalTypes;
    this.elementCoder = elementCoder;
    this.kmsKey = kmsKey;
    this.rowWriterFactory = rowWriterFactory;
    schemaUpdateOptions = Collections.emptySet();
    this.tempDataset = tempDataset;
    this.tableDestinationCoder =
        clusteringEnabled ? TableDestinationCoderV3.of() : TableDestinationCoderV2.of();
    this.badRecordRouter = badRecordRouter;
    this.badRecordErrorHandler = badRecordErrorHandler;
  }

  void setSchemaUpdateOptions(Set<SchemaUpdateOption> schemaUpdateOptions) {
    this.schemaUpdateOptions = schemaUpdateOptions;
    // In the case schemaUpdateOptions are specified by the user, do not wrap dynamicDestinations
    // to respect those options.
    if (schemaUpdateOptions != null && !schemaUpdateOptions.isEmpty()) {
      this.destinationsWithMatching = dynamicDestinations;
    } else {
      this.destinationsWithMatching =
          DynamicDestinationsHelpers.matchTableDynamicDestinations(
              dynamicDestinations, bigQueryServices);
    }
  }

  void setTestServices(BigQueryServices bigQueryServices) {
    this.bigQueryServices = bigQueryServices;
  }

  /** Get the maximum number of file writers that will be open simultaneously in a bundle. */
  public int getMaxNumWritersPerBundle() {
    return maxNumWritersPerBundle;
  }

  /** Set the maximum number of file writers that will be open simultaneously in a bundle. */
  public void setMaxNumWritersPerBundle(int maxNumWritersPerBundle) {
    this.maxNumWritersPerBundle = maxNumWritersPerBundle;
  }

  public void setTriggeringFrequency(Duration triggeringFrequency) {
    this.triggeringFrequency = triggeringFrequency;
  }

  public int getMaxRetryJobs() {
    return maxRetryJobs;
  }

  public void setMaxRetryJobs(int maxRetryJobs) {
    this.maxRetryJobs = maxRetryJobs;
  }

  public void setNumFileShards(int numFileShards) {
    this.numFileShards = numFileShards;
  }

  @VisibleForTesting
  void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  @VisibleForTesting
  void setMaxFilesPerPartition(int maxFilesPerPartition) {
    this.maxFilesPerPartition = maxFilesPerPartition;
  }

  @VisibleForTesting
  void setMaxBytesPerPartition(long maxBytesPerPartition) {
    this.maxBytesPerPartition = maxBytesPerPartition;
  }

  @Override
  public void validate(@Nullable PipelineOptions maybeOptions) {
    PipelineOptions options = Preconditions.checkArgumentNotNull(maybeOptions);
    // We will use a BigQuery load job -- validate the temp location.
    String tempLocation;
    if (customGcsTempLocation == null) {
      tempLocation = options.getTempLocation();
    } else {
      if (!customGcsTempLocation.isAccessible()) {
        // Can't perform verification in this case.
        return;
      }
      tempLocation = customGcsTempLocation.get();
    }
    checkArgument(
        !Strings.isNullOrEmpty(tempLocation),
        "BigQueryIO.Write needs a GCS temp location to store temp files. "
            + "This can be set by withCustomGcsTempLocation() in the Builder "
            + "or through the fallback pipeline option --tempLocation.");
    if (bigQueryServices == null) {
      try {
        GcsPath.fromUri(tempLocation);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "BigQuery temp location expected a valid 'gs://' path, but was given '%s'",
                tempLocation),
            e);
      }
    }
  }

  // Expand the pipeline when the user has requested periodically-triggered file writes.
  private WriteResult expandTriggered(PCollection<KV<DestinationT, ElementT>> input) {
    Duration triggeringFrequency = Preconditions.checkStateNotNull(this.triggeringFrequency);
    Pipeline p = input.getPipeline();
    final PCollectionView<String> loadJobIdPrefixView = createJobIdPrefixView(p, JobType.LOAD);
    final PCollectionView<String> tempLoadJobIdPrefixView =
        createJobIdPrefixView(p, JobType.TEMP_TABLE_LOAD);
    final PCollectionView<String> zeroLoadJobIdPrefixView =
        createJobIdPrefixView(p, JobType.SCHEMA_UPDATE);
    final PCollectionView<String> copyJobIdPrefixView = createJobIdPrefixView(p, JobType.COPY);
    final PCollectionView<String> tempFilePrefixView =
        createTempFilePrefixView(p, loadJobIdPrefixView);
    PCollection<WriteBundlesToFiles.Result<DestinationT>> results;
    if (numFileShards > 0) {
      // The user-supplied triggeringFrequency is often chosen to control how many BigQuery load
      // jobs are generated, to prevent going over BigQuery's daily quota for load jobs. If this
      // is set to a large value, currently we have to buffer all the data until the trigger fires.
      // Instead we ensure that the files are written if a threshold number of records are ready.
      // We use only the user-supplied trigger on the actual BigQuery load. This allows us to
      // offload the data to the filesystem.
      PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
          input.apply(
              "rewindowIntoGlobal",
              Window.<KV<DestinationT, ElementT>>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(
                          AfterFirst.of(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(triggeringFrequency),
                              AfterPane.elementCountAtLeast(FILE_TRIGGERING_RECORD_COUNT))))
                  .discardingFiredPanes());
      results = writeStaticallyShardedFiles(inputInGlobalWindow, tempFilePrefixView);
    } else {
      // In the case of dynamic sharding, however, we use a default trigger since the transform
      // performs sharding also batches elements to avoid generating too many tiny files. User
      // trigger is applied right after writes to limit the number of load jobs.
      PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
          input.apply(
              "rewindowIntoGlobal",
              Window.<KV<DestinationT, ElementT>>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
      results = writeDynamicallyShardedFilesTriggered(inputInGlobalWindow, tempFilePrefixView);
    }

    // Apply the user's trigger before we start generating BigQuery load jobs.
    results =
        results.apply(
            "applyUserTrigger",
            Window.<WriteBundlesToFiles.Result<DestinationT>>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(triggeringFrequency)))
                .discardingFiredPanes());

    TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>> multiPartitionsTag =
        new TupleTag<>("multiPartitionsTag");
    TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>> singlePartitionTag =
        new TupleTag<>("singlePartitionTag");

    // If we have non-default triggered output, we can't use the side-input technique used in
    // expandUntriggered. Instead make the result list a main input. Apply a GroupByKey first for
    // determinism.
    PCollectionTuple partitions =
        results
            .apply("AttachDestinationKey", WithKeys.of(result -> result.destination))
            .setCoder(
                KvCoder.of(destinationCoder, WriteBundlesToFiles.ResultCoder.of(destinationCoder)))
            .apply("GroupFilesByDestination", GroupByKey.create())
            .apply("ExtractResultValues", Values.create())
            .apply(
                "WritePartitionTriggered",
                ParDo.of(
                        new WritePartition<>(
                            singletonTable,
                            dynamicDestinations,
                            tempFilePrefixView,
                            maxFilesPerPartition,
                            maxBytesPerPartition,
                            multiPartitionsTag,
                            singlePartitionTag,
                            rowWriterFactory))
                    .withSideInputs(tempFilePrefixView)
                    .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));
    PCollection<KV<DestinationT, WriteTables.Result>> tempTables =
        writeTempTables(partitions.get(multiPartitionsTag), tempLoadJobIdPrefixView);

    List<PCollectionView<?>> sideInputsForUpdateSchema =
        Lists.newArrayList(zeroLoadJobIdPrefixView);
    sideInputsForUpdateSchema.addAll(dynamicDestinations.getSideInputs());

    PCollection<TableDestination> successfulMultiPartitionWrites =
        tempTables
            // Now that the load job has happened, we want the rename to happen immediately.
            .apply(
                "Window Into Global Windows",
                Window.<KV<DestinationT, WriteTables.Result>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))))
            // We use this and the following GBK to aggregate by final destination.
            // This way, each destination has its own pane sequence
            .apply("AddDestinationKeys", WithKeys.of(result -> result.getKey()))
            .setCoder(KvCoder.of(destinationCoder, tempTables.getCoder()))
            .apply("GroupTempTablesByFinalDestination", GroupByKey.create())
            .apply("ExtractTempTables", Values.create())
            .apply(
                ParDo.of(
                        new UpdateSchemaDestination<DestinationT>(
                            bigQueryServices,
                            zeroLoadJobIdPrefixView,
                            loadJobProjectId,
                            WriteDisposition.WRITE_APPEND,
                            CreateDisposition.CREATE_NEVER,
                            maxRetryJobs,
                            kmsKey,
                            schemaUpdateOptions,
                            destinationsWithMatching))
                    .withSideInputs(sideInputsForUpdateSchema))
            .apply(
                "WriteRenameTriggered",
                new WriteRename(
                    bigQueryServices,
                    copyJobIdPrefixView,
                    writeDisposition,
                    createDisposition,
                    maxRetryJobs,
                    kmsKey,
                    loadJobProjectId,
                    copyJobIdPrefixView));

    PCollection<TableDestination> successfulSinglePartitionWrites =
        writeSinglePartition(partitions.get(singlePartitionTag), loadJobIdPrefixView)
            .apply(
                "RewindowSinglePartitionResults",
                Window.<TableDestination>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))));

    PCollectionList<TableDestination> allSuccessfulWrites =
        PCollectionList.of(successfulMultiPartitionWrites).and(successfulSinglePartitionWrites);

    return writeResult(p, allSuccessfulWrites.apply(Flatten.pCollections()));
  }

  // Expand the pipeline when the user has not requested periodically-triggered file writes.
  public WriteResult expandUntriggered(PCollection<KV<DestinationT, ElementT>> input) {
    Pipeline p = input.getPipeline();
    final PCollectionView<String> loadJobIdPrefixView = createJobIdPrefixView(p, JobType.LOAD);
    final PCollectionView<String> tempLoadJobIdPrefixView =
        createJobIdPrefixView(p, JobType.TEMP_TABLE_LOAD);
    final PCollectionView<String> zeroLoadJobIdPrefixView =
        createJobIdPrefixView(p, JobType.SCHEMA_UPDATE);
    final PCollectionView<String> copyJobIdPrefixView = createJobIdPrefixView(p, JobType.COPY);
    final PCollectionView<String> tempFilePrefixView =
        createTempFilePrefixView(p, loadJobIdPrefixView);
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal",
            Window.<KV<DestinationT, ElementT>>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .discardingFiredPanes());
    PCollection<WriteBundlesToFiles.Result<DestinationT>> results =
        (numFileShards == 0)
            ? writeDynamicallyShardedFilesUntriggered(inputInGlobalWindow, tempFilePrefixView)
            : writeStaticallyShardedFiles(inputInGlobalWindow, tempFilePrefixView);

    TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>> multiPartitionsTag =
        new TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>>("multiPartitionsTag") {};
    TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>> singlePartitionTag =
        new TupleTag<KV<ShardedKey<DestinationT>, WritePartition.Result>>("singlePartitionTag") {};

    // This transform will look at the set of files written for each table, and if any table has
    // too many files or bytes, will partition that table's files into multiple partitions for
    // loading.
    PCollectionTuple partitions =
        results
            .apply("ReifyResults", new CombineAsIterable<>())
            .setCoder(IterableCoder.of(WriteBundlesToFiles.ResultCoder.of(destinationCoder)))
            .apply(
                "WritePartitionUntriggered",
                ParDo.of(
                        new WritePartition<>(
                            singletonTable,
                            dynamicDestinations,
                            tempFilePrefixView,
                            maxFilesPerPartition,
                            maxBytesPerPartition,
                            multiPartitionsTag,
                            singlePartitionTag,
                            rowWriterFactory))
                    .withSideInputs(tempFilePrefixView)
                    .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

    PCollection<TableDestination> successfulSinglePartitionWrites =
        writeSinglePartition(partitions.get(singlePartitionTag), loadJobIdPrefixView);

    List<PCollectionView<?>> sideInputsForUpdateSchema =
        Lists.newArrayList(zeroLoadJobIdPrefixView);
    sideInputsForUpdateSchema.addAll(dynamicDestinations.getSideInputs());

    PCollection<TableDestination> successfulMultiPartitionWrites =
        writeTempTables(partitions.get(multiPartitionsTag), tempLoadJobIdPrefixView)
            .apply("ReifyRenameInput", new ReifyAsIterable<>())
            .apply(
                ParDo.of(
                        new UpdateSchemaDestination<DestinationT>(
                            bigQueryServices,
                            zeroLoadJobIdPrefixView,
                            loadJobProjectId,
                            WriteDisposition.WRITE_APPEND,
                            CreateDisposition.CREATE_NEVER,
                            maxRetryJobs,
                            kmsKey,
                            schemaUpdateOptions,
                            destinationsWithMatching))
                    .withSideInputs(sideInputsForUpdateSchema))
            .apply(
                "WriteRenameUntriggered",
                new WriteRename(
                    bigQueryServices,
                    copyJobIdPrefixView,
                    writeDisposition,
                    createDisposition,
                    maxRetryJobs,
                    kmsKey,
                    loadJobProjectId,
                    copyJobIdPrefixView))
            .setCoder(tableDestinationCoder);

    PCollectionList<TableDestination> allSuccessfulWrites =
        PCollectionList.of(successfulSinglePartitionWrites).and(successfulMultiPartitionWrites);

    return writeResult(p, allSuccessfulWrites.apply(Flatten.pCollections()));
  }

  // Generate the base job id string.
  private PCollectionView<String> createJobIdPrefixView(Pipeline p, final JobType type) {
    // Create a singleton job ID token at execution time. This will be used as the base for all
    // load jobs issued from this instance of the transform.
    return p.apply("JobIdCreationRoot_" + type.toString(), Create.of((Void) null))
        .apply(
            "CreateJobId_" + type.toString(),
            ParDo.of(
                new DoFn<Void, String>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    c.output(
                        BigQueryResourceNaming.createJobIdPrefix(
                            c.getPipelineOptions().getJobName(),
                            BigQueryHelpers.randomUUIDString(),
                            type));
                  }
                }))
        .apply("JobIdSideInput_" + type.toString(), View.asSingleton());
  }

  // Generate the temporary-file prefix.
  private PCollectionView<String> createTempFilePrefixView(
      Pipeline p, final PCollectionView<String> jobIdView) {
    return p.apply("Create dummy value", Create.of(""))
        .apply(
            "GetTempFilePrefix",
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void getTempFilePrefix(ProcessContext c) {
                        String tempLocationRoot;
                        if (customGcsTempLocation != null) {
                          tempLocationRoot = customGcsTempLocation.get();
                        } else {
                          tempLocationRoot = c.getPipelineOptions().getTempLocation();
                        }
                        String tempLocation =
                            resolveTempLocation(
                                tempLocationRoot, "BigQueryWriteTemp", c.sideInput(jobIdView));
                        LOG.info(
                            "Writing BigQuery temporary files to {} before loading them.",
                            tempLocation);
                        c.output(tempLocation);
                      }
                    })
                .withSideInputs(jobIdView))
        .apply("TempFilePrefixView", View.asSingleton());
  }

  // Writes input data to dynamically-sharded per-bundle files without triggering. Input records are
  // spilt to new files if memory is constrained. Returns a PCollection of filename, file byte size,
  // and table destination.
  PCollection<WriteBundlesToFiles.Result<DestinationT>> writeDynamicallyShardedFilesUntriggered(
      PCollection<KV<DestinationT, ElementT>> input, PCollectionView<String> tempFilePrefix) {
    TupleTag<WriteBundlesToFiles.Result<DestinationT>> writtenFilesTag =
        new TupleTag<WriteBundlesToFiles.Result<DestinationT>>("writtenFiles") {};
    TupleTag<KV<ShardedKey<DestinationT>, ElementT>> unwrittedRecordsTag =
        new TupleTag<KV<ShardedKey<DestinationT>, ElementT>>("unwrittenRecords") {};
    PCollectionTuple writeBundlesTuple =
        input.apply(
            "WriteBundlesToFiles",
            ParDo.of(
                    new WriteBundlesToFiles<>(
                        tempFilePrefix,
                        unwrittedRecordsTag,
                        maxNumWritersPerBundle,
                        maxFileSize,
                        rowWriterFactory,
                        input.getCoder(),
                        badRecordRouter))
                .withSideInputs(tempFilePrefix)
                .withOutputTags(
                    writtenFilesTag,
                    TupleTagList.of(ImmutableList.of(unwrittedRecordsTag, BAD_RECORD_TAG))));
    PCollection<WriteBundlesToFiles.Result<DestinationT>> writtenFiles =
        writeBundlesTuple
            .get(writtenFilesTag)
            .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));
    PCollection<KV<ShardedKey<DestinationT>, ElementT>> unwrittenRecords =
        writeBundlesTuple
            .get(unwrittedRecordsTag)
            .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), elementCoder));
    badRecordErrorHandler.addErrorCollection(
        writeBundlesTuple.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline())));

    // If the bundles contain too many output tables to be written inline to files (due to memory
    // limits), any unwritten records will be spilled to the unwrittenRecordsTag PCollection.
    // Group these records by key, and write the files after grouping. Since the record is grouped
    // by key, we can ensure that only one file is open at a time in each bundle.
    PCollection<WriteBundlesToFiles.Result<DestinationT>> writtenFilesGrouped =
        writeShardedRecords(unwrittenRecords, tempFilePrefix);

    // PCollection of filename, file byte size, and table destination.
    return PCollectionList.of(writtenFiles)
        .and(writtenFilesGrouped)
        .apply("FlattenFiles", Flatten.pCollections())
        .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));
  }

  // Writes input data to statically-sharded files. Returns a PCollection of filename, file byte
  // size, and table destination.
  PCollection<WriteBundlesToFiles.Result<DestinationT>> writeStaticallyShardedFiles(
      PCollection<KV<DestinationT, ElementT>> input, PCollectionView<String> tempFilePrefix) {
    checkState(numFileShards > 0);
    PCollection<KV<ShardedKey<DestinationT>, ElementT>> shardedRecords =
        input
            .apply(
                "AddShard",
                ParDo.of(
                    new DoFn<KV<DestinationT, ElementT>, KV<ShardedKey<DestinationT>, ElementT>>() {
                      int shardNumber;

                      @Setup
                      public void setup() {
                        shardNumber = ThreadLocalRandom.current().nextInt(numFileShards);
                      }

                      @ProcessElement
                      public void processElement(
                          @Element KV<DestinationT, ElementT> element,
                          OutputReceiver<KV<ShardedKey<DestinationT>, ElementT>> o) {
                        DestinationT destination = element.getKey();
                        o.output(
                            KV.of(
                                ShardedKey.of(destination, ++shardNumber % numFileShards),
                                element.getValue()));
                      }
                    }))
            .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), elementCoder));

    return writeShardedRecords(shardedRecords, tempFilePrefix);
  }

  // Writes input data to dynamically-sharded files with triggering. The input data is sharded by
  // table destinations and each destination may be sub-sharded dynamically. Returns a PCollection
  // of filename, file byte size, and table destination.
  PCollection<WriteBundlesToFiles.Result<DestinationT>> writeDynamicallyShardedFilesTriggered(
      PCollection<KV<DestinationT, ElementT>> input, PCollectionView<String> tempFilePrefix) {
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    Duration maxBufferingDuration =
        options.getMaxBufferingDurationMilliSec() > 0
            ? Duration.millis(options.getMaxBufferingDurationMilliSec())
            : FILE_TRIGGERING_BATCHING_DURATION;
    GcsOptions gcsOptions = input.getPipeline().getOptions().as(GcsOptions.class);
    int byteSize =
        MoreObjects.firstNonNull(
            gcsOptions.getGcsUploadBufferSizeBytes(), DEFAULT_FILE_TRIGGERING_BYTE_COUNT);
    // In contrast to fixed sharding with user trigger, here we use a global window with default
    // trigger and rely on GroupIntoBatches transform to group, batch and at the same time
    // parallelize properly. We also ensure that the files are written if a threshold number of
    // records are ready. Dynamic sharding is achieved via the withShardedKey() option provided by
    // GroupIntoBatches.
    TupleTag<Result<DestinationT>> successfulResultsTag = new TupleTag<>();
    PCollectionTuple writeResults =
        input
            .apply(
                GroupIntoBatches.<DestinationT, ElementT>ofSize(FILE_TRIGGERING_RECORD_COUNT)
                    .withByteSize(byteSize)
                    .withMaxBufferingDuration(maxBufferingDuration)
                    .withShardedKey())
            .setCoder(
                KvCoder.of(
                    org.apache.beam.sdk.util.ShardedKey.Coder.of(destinationCoder),
                    IterableCoder.of(elementCoder)))
            .apply(
                "StripShardId",
                MapElements.via(
                    new SimpleFunction<
                        KV<org.apache.beam.sdk.util.ShardedKey<DestinationT>, Iterable<ElementT>>,
                        KV<DestinationT, Iterable<ElementT>>>() {
                      @Override
                      public KV<DestinationT, Iterable<ElementT>> apply(
                          KV<org.apache.beam.sdk.util.ShardedKey<DestinationT>, Iterable<ElementT>>
                              input) {
                        return KV.of(input.getKey().getKey(), input.getValue());
                      }
                    }))
            .setCoder(KvCoder.of(destinationCoder, IterableCoder.of(elementCoder)))
            .apply(
                "WriteGroupedRecords",
                ParDo.of(
                        new WriteGroupedRecordsToFiles<DestinationT, ElementT>(
                            tempFilePrefix,
                            maxFileSize,
                            rowWriterFactory,
                            badRecordRouter,
                            successfulResultsTag,
                            elementCoder))
                    .withSideInputs(tempFilePrefix)
                    .withOutputTags(successfulResultsTag, TupleTagList.of(BAD_RECORD_TAG)));
    badRecordErrorHandler.addErrorCollection(
        writeResults.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline())));

    return writeResults
        .get(successfulResultsTag)
        .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));
  }

  private PCollection<Result<DestinationT>> writeShardedRecords(
      PCollection<KV<ShardedKey<DestinationT>, ElementT>> shardedRecords,
      PCollectionView<String> tempFilePrefix) {
    TupleTag<Result<DestinationT>> successfulResultsTag = new TupleTag<>();
    PCollectionTuple writeResults =
        shardedRecords
            .apply("GroupByDestination", GroupByKey.create())
            .apply(
                "StripShardId",
                MapElements.via(
                    new SimpleFunction<
                        KV<ShardedKey<DestinationT>, Iterable<ElementT>>,
                        KV<DestinationT, Iterable<ElementT>>>() {
                      @Override
                      public KV<DestinationT, Iterable<ElementT>> apply(
                          KV<ShardedKey<DestinationT>, Iterable<ElementT>> input) {
                        return KV.of(input.getKey().getKey(), input.getValue());
                      }
                    }))
            .setCoder(KvCoder.of(destinationCoder, IterableCoder.of(elementCoder)))
            .apply(
                "WriteGroupedRecords",
                ParDo.of(
                        new WriteGroupedRecordsToFiles<>(
                            tempFilePrefix,
                            maxFileSize,
                            rowWriterFactory,
                            badRecordRouter,
                            successfulResultsTag,
                            elementCoder))
                    .withSideInputs(tempFilePrefix)
                    .withOutputTags(successfulResultsTag, TupleTagList.of(BAD_RECORD_TAG)));

    badRecordErrorHandler.addErrorCollection(
        writeResults
            .get(BAD_RECORD_TAG)
            .setCoder(BadRecord.getCoder(shardedRecords.getPipeline())));

    return writeResults
        .get(successfulResultsTag)
        .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));
  }

  // Take in a list of files and write them to temporary tables.
  private PCollection<KV<DestinationT, WriteTables.Result>> writeTempTables(
      PCollection<KV<ShardedKey<DestinationT>, WritePartition.Result>> input,
      PCollectionView<String> jobIdTokenView) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList(jobIdTokenView);
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    Coder<KV<ShardedKey<DestinationT>, WritePartition.Result>> partitionsCoder =
        KvCoder.of(
            ShardedKeyCoder.of(NullableCoder.of(destinationCoder)),
            WritePartition.ResultCoder.INSTANCE);

    // If WriteBundlesToFiles produced more than DEFAULT_MAX_FILES_PER_PARTITION files or
    // DEFAULT_MAX_BYTES_PER_PARTITION bytes, then
    // the import needs to be split into multiple partitions, and those partitions will be
    // specified in multiPartitionsTag.
    return input
        .setCoder(partitionsCoder)
        // Reshuffle will distribute this among multiple workers, and also guard against
        // reexecution of the WritePartitions step once WriteTables has begun.
        .apply("MultiPartitionsReshuffle", Reshuffle.of())
        .apply(
            "MultiPartitionsWriteTables",
            new WriteTables<>(
                true,
                bigQueryServices,
                jobIdTokenView,
                WriteDisposition.WRITE_EMPTY,
                CreateDisposition.CREATE_IF_NEEDED,
                sideInputs,
                destinationsWithMatching,
                loadJobProjectId,
                maxRetryJobs,
                ignoreUnknownValues,
                kmsKey,
                rowWriterFactory.getSourceFormat(),
                useAvroLogicalTypes,
                // Note that we can't pass through the schema update options when creating temporary
                // tables. They also shouldn't be needed. See
                // https://github.com/apache/beam/issues/21105 for additional details.
                schemaUpdateOptions,
                tempDataset))
        .setCoder(KvCoder.of(destinationCoder, WriteTables.ResultCoder.INSTANCE));
  }

  // In the case where the files fit into a single load job, there's no need to write temporary
  // tables and rename. We can load these files directly into the target BigQuery table.
  PCollection<TableDestination> writeSinglePartition(
      PCollection<KV<ShardedKey<DestinationT>, WritePartition.Result>> input,
      PCollectionView<String> loadJobIdPrefixView) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList(loadJobIdPrefixView);
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    Coder<KV<ShardedKey<DestinationT>, WritePartition.Result>> partitionsCoder =
        KvCoder.of(
            ShardedKeyCoder.of(NullableCoder.of(destinationCoder)),
            WritePartition.ResultCoder.INSTANCE);
    // Write single partition to final table
    PCollection<KV<DestinationT, WriteTables.Result>> successfulWrites =
        input
            .setCoder(partitionsCoder)
            // Reshuffle will distribute this among multiple workers, and also guard against
            // reexecution of the WritePartitions step once WriteTables has begun.
            .apply("SinglePartitionsReshuffle", Reshuffle.of())
            .apply(
                "SinglePartitionWriteTables",
                new WriteTables<>(
                    false,
                    bigQueryServices,
                    loadJobIdPrefixView,
                    writeDisposition,
                    createDisposition,
                    sideInputs,
                    dynamicDestinations,
                    loadJobProjectId,
                    maxRetryJobs,
                    ignoreUnknownValues,
                    kmsKey,
                    rowWriterFactory.getSourceFormat(),
                    useAvroLogicalTypes,
                    schemaUpdateOptions,
                    null))
            .setCoder(KvCoder.of(destinationCoder, WriteTables.ResultCoder.INSTANCE));

    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    String defaultProjectId =
        options.getBigQueryProject() == null ? options.getProject() : options.getBigQueryProject();

    return successfulWrites
        .apply(Keys.create())
        .apply(
            "Convert to TableDestinations",
            ParDo.of(
                    new DoFn<DestinationT, TableDestination>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        dynamicDestinations.setSideInputAccessorFromProcessContext(c);
                        TableDestination tableDestination =
                            dynamicDestinations.getTable(c.element());
                        TableReference tableReference = tableDestination.getTableReference();

                        // get project ID from options if it's not included in the table reference
                        if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
                          tableReference.setProjectId(defaultProjectId);
                          tableDestination = tableDestination.withTableReference(tableReference);
                        }
                        c.output(tableDestination);
                      }
                    })
                .withSideInputs(sideInputs))
        .setCoder(tableDestinationCoder);
  }

  private WriteResult writeResult(Pipeline p, PCollection<TableDestination> successfulWrites) {
    PCollection<TableRow> empty =
        p.apply("CreateEmptyFailedInserts", Create.empty(TypeDescriptor.of(TableRow.class)));

    return WriteResult.in(
        p,
        new TupleTag<>("failedInserts"),
        empty,
        null,
        new TupleTag<>("successfulInserts"),
        successfulWrites,
        null,
        null,
        null,
        null);
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, ElementT>> input) {
    return (triggeringFrequency != null) ? expandTriggered(input) : expandUntriggered(input);
  }
}
