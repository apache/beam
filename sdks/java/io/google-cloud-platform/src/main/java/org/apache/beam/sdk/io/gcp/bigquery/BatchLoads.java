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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PTransform that uses BigQuery batch-load jobs to write a PCollection to BigQuery. */
class BatchLoads<DestinationT>
    extends PTransform<PCollection<KV<DestinationT, TableRow>>, WriteResult> {
  static final Logger LOG = LoggerFactory.getLogger(BatchLoads.class);

  // The maximum number of file writers to keep open in a single bundle at a time, since file
  // writers default to 64mb buffers. This comes into play when writing dynamic table destinations.
  // The first 20 tables from a single BatchLoads transform will write files inline in the
  // transform. Anything beyond that might be shuffled.  Users using this transform directly who
  // know that they are running on workers with sufficient memory can increase this by calling
  // BatchLoads#setMaxNumWritersPerBundle. This allows the workers to do more work in memory, and
  // save on the cost of shuffling some of this data.
  // Keep in mind that specific runners may decide to run multiple bundles in parallel, based on
  // their own policy.
  @VisibleForTesting
  static final int DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE = 20;

  @VisibleForTesting
  // Maximum number of files in a single partition.
  static final int MAX_NUM_FILES = 10000;

  @VisibleForTesting
  // Maximum number of bytes in a single partition -- 11 TiB just under BQ's 12 TiB limit.
  static final long MAX_SIZE_BYTES = 11 * (1L << 40);

  // The maximum size of a single file - 4TiB, just under the 5 TiB limit.
  static final long DEFAULT_MAX_FILE_SIZE = 4 * (1L << 40);

  static final int DEFAULT_NUM_FILE_SHARDS = 0;

  // If user triggering is supplied, we will trigger the file write after this many records are
  // written.
  static final int FILE_TRIGGERING_RECORD_COUNT = 500000;

  // The maximum number of retries to poll the status of a job.
  // It sets to {@code Integer.MAX_VALUE} to block until the BigQuery job finishes.
  static final int LOAD_JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  // The maximum number of retry jobs.
  static final int MAX_RETRY_JOBS = 3;

  private BigQueryServices bigQueryServices;
  private final WriteDisposition writeDisposition;
  private final CreateDisposition createDisposition;
  // Indicates that we are writing to a constant single table. If this is the case, we will create
  // the table, even if there is no data in it.
  private final boolean singletonTable;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private final Coder<DestinationT> destinationCoder;
  private int maxNumWritersPerBundle;
  private long maxFileSize;
  private int numFileShards;
  private Duration triggeringFrequency;


  BatchLoads(WriteDisposition writeDisposition, CreateDisposition createDisposition,
             boolean singletonTable,
             DynamicDestinations<?, DestinationT> dynamicDestinations,
             Coder<DestinationT> destinationCoder) {
    bigQueryServices = new BigQueryServicesImpl();
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.singletonTable = singletonTable;
    this.dynamicDestinations = dynamicDestinations;
    this.destinationCoder = destinationCoder;
    this.maxNumWritersPerBundle = DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE;
    this.maxFileSize = DEFAULT_MAX_FILE_SIZE;
    this.numFileShards = DEFAULT_NUM_FILE_SHARDS;
    this.triggeringFrequency = null;
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

  public void setNumFileShards(int numFileShards) {
    this.numFileShards = numFileShards;
  }

  @VisibleForTesting
  void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  @Override
  public void validate(PipelineOptions options) {
    // We will use a BigQuery load job -- validate the temp location.
    String tempLocation = options.getTempLocation();
    checkArgument(
        !Strings.isNullOrEmpty(tempLocation),
        "BigQueryIO.Write needs a GCS temp location to store temp files.");
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

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, TableRow>> input) {
    if (triggeringFrequency != null) {
      checkArgument(numFileShards > 0);
    }

    Pipeline p = input.getPipeline();

    // Create a singleton job ID token at execution time. This will be used as the base for all
    // load jobs issued from this instance of the transform.
    final PCollection<String> jobIdToken =
        p.apply("TriggerIdCreation", Create.of("ignored"))
            .apply(
                "CreateJobId",
                MapElements.via(
                    new SimpleFunction<String, String>() {
                      @Override
                      public String apply(String input) {
                        return BigQueryHelpers.randomUUIDString();
                      }
                    }));
    final PCollectionView<String> jobIdTokenView = jobIdToken.apply(View.<String>asSingleton());

    PCollectionView<String> tempFilePrefix = jobIdToken
            .apply(
                "GetTempFilePrefix",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void getTempFilePrefix(ProcessContext c) {
                        String tempLocation = resolveTempLocation(
                            c.getPipelineOptions().getTempLocation(),
                            "BigQueryWriteTemp", c.element());
                        LOG.info("Writing BigQuery temporary files to {} before loading them.",
                            tempLocation);
                          c.output(tempLocation);
                      }
                    }))
            .apply("TempFilePrefixView", View.<String>asSingleton());

    // PCollection of filename, file byte size, and table destination.
    PCollection<WriteBundlesToFiles.Result<DestinationT>> results;
    if (numFileShards == 0) {
      PCollection<KV<DestinationT, TableRow>> inputInGlobalWindow =
          input.apply(
              "rewindowIntoGlobal",
              Window.<KV<DestinationT, TableRow>>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
      TupleTag<WriteBundlesToFiles.Result<DestinationT>> writtenFilesTag =
          new TupleTag<WriteBundlesToFiles.Result<DestinationT>>("writtenFiles") {
          };
      TupleTag<KV<ShardedKey<DestinationT>, TableRow>> unwrittedRecordsTag =
          new TupleTag<KV<ShardedKey<DestinationT>, TableRow>>("unwrittenRecords") {
          };
      PCollectionTuple writeBundlesTuple = inputInGlobalWindow
          .apply("WriteBundlesToFiles",
              ParDo.of(new WriteBundlesToFiles<>(tempFilePrefix, unwrittedRecordsTag,
                  maxNumWritersPerBundle, maxFileSize))
                  .withSideInputs(tempFilePrefix)
                  .withOutputTags(writtenFilesTag, TupleTagList.of(unwrittedRecordsTag)));
      PCollection<WriteBundlesToFiles.Result<DestinationT>> writtenFiles =
          writeBundlesTuple.get(writtenFilesTag)
              .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));

      // If the bundles contain too many output tables to be written inline to files (due to memory
      // limits), any unwritten records will be spilled to the unwrittenRecordsTag PCollection.
      // Group these records by key, and write the files after grouping. Since the record is grouped
      // by key, we can ensure that only one file is open at a time in each bundle.
      PCollection<WriteBundlesToFiles.Result<DestinationT>> writtenFilesGrouped =
          writeBundlesTuple
              .get(unwrittedRecordsTag)
              .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), TableRowJsonCoder.of()))
              .apply("GroupByDestination", GroupByKey.<ShardedKey<DestinationT>, TableRow>create())
              .apply("WriteGroupedRecords",
                  ParDo.of(new WriteGroupedRecordsToFiles<DestinationT>(
                      tempFilePrefix, maxFileSize))
                      .withSideInputs(tempFilePrefix))
              .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));

      // PCollection of filename, file byte size, and table destination.
      results = PCollectionList.of(writtenFiles).and(writtenFilesGrouped)
          .apply("FlattenFiles", Flatten.<Result<DestinationT>>pCollections());
    } else {
      // The user-supplied triggeringDuration is often chosen to to control how many BigQuery load
      // jobs are generated, to prevent going over BigQuery's daily quota for load jobs. If this
      // is set to a large value, currently we have to buffer all the data unti the trigger fires.
      // Instead we ensure that the files are written if a threshold number of records are ready.
      // We use only the user-supplied trigger on the actual BigQuery load. This allows us to
      // offload the data to the filesystem.
      Trigger trigger = DefaultTrigger.of();
      if (triggeringFrequency != null) {
        trigger = Repeatedly.forever(AfterFirst.of(
            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(triggeringFrequency),
            AfterPane.elementCountAtLeast(FILE_TRIGGERING_RECORD_COUNT)));
      }
      PCollection<KV<DestinationT, TableRow>> inputInGlobalWindow =
          input.apply(
              "rewindowIntoGlobal",
              Window.<KV<DestinationT, TableRow>>into(new GlobalWindows())
                  .triggering(trigger)
                  .discardingFiredPanes());

      results = inputInGlobalWindow
          .apply("AddShard", ParDo.of(new DoFn<KV<DestinationT, TableRow>,
              KV<ShardedKey<DestinationT>, TableRow>>() {
        int shardNumber;
        @Setup
        public void setup() {
          shardNumber = ThreadLocalRandom.current().nextInt(numFileShards);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
          DestinationT destination = c.element().getKey();
          TableRow tableRow = c.element().getValue();
          c.output(KV.of(ShardedKey.of(destination, (shardNumber + 1) % numFileShards),
              tableRow));
        }
      }))
          .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), TableRowJsonCoder.of()))
          .apply("GroupByDestination", GroupByKey.<ShardedKey<DestinationT>, TableRow>create())
          .apply("WriteGroupedRecords",
              ParDo.of(new WriteGroupedRecordsToFiles<DestinationT>(tempFilePrefix, maxFileSize))
                  .withSideInputs(tempFilePrefix));
    }
    results.setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));

    TupleTag<KV<ShardedKey<DestinationT>, List<String>>> multiPartitionsTag =
        new TupleTag<KV<ShardedKey<DestinationT>, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<ShardedKey<DestinationT>, List<String>>> singlePartitionTag =
        new TupleTag<KV<ShardedKey<DestinationT>, List<String>>>("singlePartitionTag") {};

    PCollectionTuple partitions;
    if (triggeringFrequency == null) {
      // Turn the list of files and record counts in a PCollectionView that can be used as a
      // side input.
      PCollectionView<Iterable<WriteBundlesToFiles.Result<DestinationT>>> resultsView =
          results.apply("ResultsView",
              View.<WriteBundlesToFiles.Result<DestinationT>>asIterable());
      // This transform will look at the set of files written for each table, and if any table has
      // too many files or bytes, will partition that table's files into multiple partitions for
      // loading.
      PCollection<Iterable<Result<DestinationT>>> singleton = p.apply("singleton",
          Create.<Iterable<Result<DestinationT>>>of(ImmutableList.<Result<DestinationT>>of())
              .withCoder(IterableCoder.of(WriteBundlesToFiles.ResultCoder.of(destinationCoder))));
      partitions =
          singleton.apply(
              "WritePartitionUntriggered",
              ParDo.of(
                  new WritePartition<>(
                      singletonTable,
                      dynamicDestinations,
                      tempFilePrefix,
                      resultsView,
                      multiPartitionsTag,
                      singlePartitionTag))
                  .withSideInputs(tempFilePrefix, resultsView)
                  .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));
    } else {
      // Apply the user's trigger to ensure we don't generate too many BigQuery load jobs.
      results  =
          results.apply(
              "applyUserTrigger",
              Window.< WriteBundlesToFiles.Result<DestinationT>>into(new GlobalWindows())
                  .triggering(Repeatedly.forever(
                      AfterProcessingTime.pastFirstElementInPane()
                          .plusDelayOf(triggeringFrequency)))
                  .discardingFiredPanes());

      // If we have non-default triggered output, the above side-input path is incorrect. Instead
      // make the result list a main input. Apply a GroupByKey first for determinism.
      partitions = results
          .apply("AttachSingletonKey",
              WithKeys.<Void, WriteBundlesToFiles.Result<DestinationT>>of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(),
              WriteBundlesToFiles.ResultCoder.of(destinationCoder)))
          .apply("GroupOntoSingleton", GroupByKey.<Void, Result<DestinationT>>create())
          .apply("ExtractValues", Values.<Iterable<Result<DestinationT>>>create())
          .apply(
              "WritePartitionTriggered",
              ParDo.of(
                  new WritePartition<>(
                      singletonTable,
                      dynamicDestinations,
                      tempFilePrefix,
                      null,
                      multiPartitionsTag,
                      singlePartitionTag))
                  .withSideInputs(tempFilePrefix)
                  .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));
    }

    List<PCollectionView<?>> writeTablesSideInputs =
        Lists.<PCollectionView<?>>newArrayList(jobIdTokenView);
    writeTablesSideInputs.addAll(dynamicDestinations.getSideInputs());

    Coder<KV<ShardedKey<DestinationT>, List<String>>> partitionsCoder =
          KvCoder.of(
              ShardedKeyCoder.of(NullableCoder.of(destinationCoder)),
              ListCoder.of(StringUtf8Coder.of()));

    // If WriteBundlesToFiles produced more than MAX_NUM_FILES files or MAX_SIZE_BYTES bytes, then
    // the import needs to be split into multiple partitions, and those partitions will be
    // specified in multiPartitionsTag.
    PCollection<KV<TableDestination, String>> tempTables =
        partitions
            .get(multiPartitionsTag)
            .setCoder(partitionsCoder)
            // Reshuffle will distribute this among multiple workers, and also guard against
            // reexecution of the WritePartitions step once WriteTables has begun.
            .apply(
                "MultiPartitionsReshuffle",
                Reshuffle.<ShardedKey<DestinationT>, List<String>>of())
            .apply(
                "MultiPartitionsWriteTables",
                ParDo.of(
                        new WriteTables<>(
                            false,
                            bigQueryServices,
                            jobIdTokenView,
                            WriteDisposition.WRITE_EMPTY,
                            CreateDisposition.CREATE_IF_NEEDED,
                            dynamicDestinations))
                    .withSideInputs(writeTablesSideInputs));

    if (triggeringFrequency == null) {
      // This view maps each final table destination to the set of temporary partitioned tables
      // the PCollection was loaded into.
      PCollectionView<Map<TableDestination, Iterable<String>>> tempTablesView =
          tempTables.apply("TempTablesView", View.<TableDestination, String>asMultimap());

      PCollection<KV<TableDestination, Iterable<String>>> singleton = p
          .apply("singletonRename", Create.<KV<TableDestination, Iterable<String>>>of(
              (KV<TableDestination, Iterable<String>>) null)
              .withCoder(NullableCoder.of(KvCoder.of(TableDestinationCoder.of(), IterableCoder.of
                  (StringUtf8Coder.of())))));
      singleton.apply(
          "WriteRenameUntriggered",
          ParDo.of(
              new WriteRename(
                  bigQueryServices,
                  jobIdTokenView,
                  writeDisposition,
                  createDisposition,
                  tempTablesView))
              .withSideInputs(tempTablesView, jobIdTokenView));
    } else {
      tempTables
          .setCoder(KvCoder.of(TableDestinationCoder.of(), StringUtf8Coder.of()))
          .apply(GroupByKey.<TableDestination, String>create())
          .apply("WriteRenameTriggered",
              ParDo.of(
                  new WriteRename(
                      bigQueryServices,
                      jobIdTokenView,
                      writeDisposition,
                      createDisposition,
                      null))
                  .withSideInputs(jobIdTokenView));
    }

    // Write single partition to final table
    partitions
        .get(singlePartitionTag)
        .setCoder(partitionsCoder)
        // Reshuffle will distribute this among multiple workers, and also guard against
        // reexecution of the WritePartitions step once WriteTables has begun.
        .apply(
            "SinglePartitionsReshuffle", Reshuffle.<ShardedKey<DestinationT>, List<String>>of())
        .apply(
            "SinglePartitionWriteTables",
            ParDo.of(
                    new WriteTables<>(
                        true,
                        bigQueryServices,
                        jobIdTokenView,
                        writeDisposition,
                        createDisposition,
                        dynamicDestinations))
                .withSideInputs(writeTablesSideInputs));

    PCollection<TableRow> empty =
        p.apply("CreateEmptyFailedInserts", Create.empty(TypeDescriptor.of(TableRow.class)));
    return WriteResult.in(input.getPipeline(), new TupleTag<TableRow>("failedInserts"), empty);
  }
}
