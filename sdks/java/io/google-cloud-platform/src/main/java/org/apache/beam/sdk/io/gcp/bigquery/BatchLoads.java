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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** PTransform that uses BigQuery batch-load jobs to write a PCollection to BigQuery. */
class BatchLoads extends PTransform<PCollection<KV<TableDestination, TableRow>>, WriteResult> {
  private BigQueryServices bigQueryServices;
  private final WriteDisposition writeDisposition;
  private final CreateDisposition createDisposition;
  private final ValueProvider<String> singletonJsonTableRef;
  private final String singletonTableDescription;
  private final SchemaFunction schemaFunction;

  BatchLoads(WriteDisposition writeDisposition, CreateDisposition createDisposition,
             ValueProvider<String> singletonJsonTableRef,
             String singletonTableDescription,
             SchemaFunction schemaFunction) {
    bigQueryServices = new BigQueryServicesImpl();
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.singletonJsonTableRef  = singletonJsonTableRef;
    this.singletonTableDescription = singletonTableDescription;
    this.schemaFunction = schemaFunction;
  }

  BatchLoads withTestServices(BigQueryServices bigQueryServices) {
    this.bigQueryServices = bigQueryServices;
    return this;
  }

  @Override
  public void validate(PipelineOptions options) {
    // We will use a BigQuery load job -- validate the temp location.
    String tempLocation = options.getTempLocation();
    checkArgument(
        !Strings.isNullOrEmpty(tempLocation),
        "BigQueryIO.Write needs a GCS temp location to store temp files.");
    if (write.getBigQueryServices() == null) {
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
  public WriteResult expand(PCollection<KV<TableDestination, TableRow>> input) {
    Pipeline p = input.getPipeline();
    BigQueryOptions options = p.getOptions().as(BigQueryOptions.class);

    validate(p.getOptions());

    final String stepUuid = BigQueryHelpers.randomUUIDString();

    String tempLocation = options.getTempLocation();
    String tempFilePrefix;
    try {
      IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
      tempFilePrefix =
          factory.resolve(factory.resolve(tempLocation, "BigQueryWriteTemp"), stepUuid);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to resolve BigQuery temp location in %s", tempLocation), e);
    }

    // Create a singleton job ID token at execution time. This will be used as the base for all
    // load jobs issued from this instance of the transfomr.
    PCollection<String> singleton = p.apply("Create", Create.of(tempFilePrefix));
    PCollectionView<String> jobIdTokenView =
        p.apply("TriggerIdCreation", Create.of("ignored"))
            .apply(
                "CreateJobId",
                MapElements.via(
                    new SimpleFunction<String, String>() {
                      @Override
                      public String apply(String input) {
                        return stepUuid;
                      }
                    }))
            .apply(View.<String>asSingleton());

    PCollection<KV<TableDestination, TableRow>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal",
            Window.<KV<TableDestination, TableRow>>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .discardingFiredPanes());

    // PCollection of filename, file byte size, and table destination.
    PCollection<WriteBundlesToFiles.Result> results =
        inputInGlobalWindow
            .apply("WriteBundlesToFiles", ParDo.of(new WriteBundlesToFiles(tempFilePrefix)))
            .setCoder(WriteBundlesToFiles.ResultCoder.of());

    TupleTag<KV<ShardedKey<TableDestination>, List<String>>> multiPartitionsTag =
        new TupleTag<KV<ShardedKey<TableDestination>, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<ShardedKey<TableDestination>, List<String>>> singlePartitionTag =
        new TupleTag<KV<ShardedKey<TableDestination>, List<String>>>("singlePartitionTag") {};

    // Turn the list of files and record counts in a PCollectionView that can be used as a
    // side input.
    PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView =
        results.apply("ResultsView", View.<WriteBundlesToFiles.Result>asIterable());
    // This transform will look at the set of files written for each table, and if any table has
    // too many files or bytes, will partition that table's files into multiple partitions for
    // loading.
    PCollectionTuple partitions =
        singleton.apply(
            "WritePartition",
            ParDo.of(
                    new WritePartition(
                        singletonJsonTableRef,
                        singletonTableDescription,
                        resultsView,
                        multiPartitionsTag,
                        singlePartitionTag))
                .withSideInputs(resultsView)
                .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

    List<PCollectionView<?>> writeTablesSideInputs = Lists.newArrayList();
    writeTablesSideInputs.add(jobIdTokenView);
    if (schemaFunction.getSideInput() != null) {
      writeTablesSideInputs.add(schemaFunction.getSideInput());
    }

    Coder<KV<ShardedKey<TableDestination>, List<String>>> partitionsCoder =
        KvCoder.of(
            ShardedKeyCoder.of(TableDestinationCoder.of()), ListCoder.of(StringUtf8Coder.of()));
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
                Reshuffle.<ShardedKey<TableDestination>, List<String>>of())
            .apply(
                "MultiPartitionsWriteTables",
                ParDo.of(
                        new WriteTables(
                            false,
                            bigQueryServices,
                            jobIdTokenView,
                            tempFilePrefix,
                            WriteDisposition.WRITE_EMPTY,
                            CreateDisposition.CREATE_IF_NEEDED,
                            schemaFunction))
                    .withSideInputs(writeTablesSideInputs));

    // This view maps each final table destination to the set of temporary partitioned tables
    // the PCollection was loaded into.
    PCollectionView<Map<TableDestination, Iterable<String>>> tempTablesView =
        tempTables.apply("TempTablesView", View.<TableDestination, String>asMultimap());

    singleton.apply(
        "WriteRename",
        ParDo.of(
                new WriteRename(
                    bigQueryServices,
                    jobIdTokenView,
                    writeDisposition,
                    createDisposition,
                    tempTablesView))
            .withSideInputs(tempTablesView, jobIdTokenView));

    // Write single partition to final table
    partitions
        .get(singlePartitionTag)
        .setCoder(partitionsCoder)
        // Reshuffle will distribute this among multiple workers, and also guard against
        // reexecution of the WritePartitions step once WriteTables has begun.
        .apply(
            "SinglePartitionsReshuffle", Reshuffle.<ShardedKey<TableDestination>, List<String>>of())
        .apply(
            "SinglePartitionWriteTables",
            ParDo.of(
                    new WriteTables(
                        true,
                        bigQueryServices,
                        jobIdTokenView,
                        tempFilePrefix,
                        writeDisposition,
                        createDisposition,
                        schemaFunction))
                .withSideInputs(writeTablesSideInputs));

    return WriteResult.in(input.getPipeline());
  }
}
