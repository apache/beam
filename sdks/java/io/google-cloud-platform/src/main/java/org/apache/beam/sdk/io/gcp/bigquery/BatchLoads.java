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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;


/**
 * PTransform that uses BigQuery batch-load jobs to write a PCollection to BigQuery.
 */
class BatchLoads<T> extends
    PTransform<PCollection<KV<TableDestination, TableRow>>, WriteResult> {
  BigQueryIO.Write<T> write;

  private static class ConstantSchemaFunction implements
      SerializableFunction<TableDestination, TableSchema> {
    private final @Nullable
    String jsonSchema;

    ConstantSchemaFunction(TableSchema schema) {
      this.jsonSchema = BigQueryHelpers.toJsonString(schema);
    }

    @Override
    @Nullable
    public TableSchema apply(TableDestination table) {
      return BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    }
  }

  BatchLoads(BigQueryIO.Write<T> write) {
    this.write = write;
  }

  @Override
  public WriteResult expand(PCollection<KV<TableDestination, TableRow>> input) {
    Pipeline p = input.getPipeline();
    BigQueryOptions options = p.getOptions().as(BigQueryOptions.class);
    ValueProvider<TableReference> table = write.getTableWithDefaultProject(options);

    final String stepUuid = BigQueryHelpers.randomUUIDString();

    String tempLocation = options.getTempLocation();
    String tempFilePrefix;
    try {
      IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
      tempFilePrefix = factory.resolve(
          factory.resolve(tempLocation, "BigQueryWriteTemp"),
          stepUuid);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to resolve BigQuery temp location in %s", tempLocation),
          e);
    }

    // Create a singleton job ID token at execution time. This will be used as the base for all
    // load jobs issued from this instance of the transfomr.
    PCollection<String> singleton = p.apply("Create", Create.of(tempFilePrefix));
    PCollectionView<String> jobIdTokenView = p
        .apply("TriggerIdCreation", Create.of("ignored"))
        .apply("CreateJobId", MapElements.via(
            new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                return stepUuid;
              }
            }))
        .apply(View.<String>asSingleton());

    PCollection<KV<TableDestination, TableRow>> inputInGlobalWindow =
        input.apply(
            Window.<KV<TableDestination, TableRow>>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .discardingFiredPanes());

    // PCollection of filename, file byte size, and table destination.
    PCollection<WriteBundlesToFiles.Result> results = inputInGlobalWindow
        .apply("WriteBundlesToFiles",
            ParDo.of(new WriteBundlesToFiles(tempFilePrefix)));

    TupleTag<KV<KV<TableDestination, Integer>, List<String>>> multiPartitionsTag =
        new TupleTag<KV<KV<TableDestination, Integer>, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<KV<TableDestination, Integer>, List<String>>> singlePartitionTag =
        new TupleTag<KV<KV<TableDestination, Integer>, List<String>>>("singlePartitionTag") {};

    // Turn the list of files and record counts in a PCollectionView that can be used as a
    // side input.
    PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView = results
        .apply("ResultsView", View.<WriteBundlesToFiles.Result>asIterable());
    // This transform will look at the set of files written for each table, and if any table has
    // too many files or bytes, will partition that table's files into multiple partitions for
    // loading.
    PCollectionTuple partitions = singleton.apply(ParDo
        .of(new WritePartition(
            write.getTable(),
            write.getTableDescription(),
            resultsView,
            multiPartitionsTag,
            singlePartitionTag))
        .withSideInputs(resultsView)
        .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

    // Since BigQueryIO.java does not yet have support for per-table schemas, inject a constant
    // schema function here. If no schema is specified, this function will return null.
    SerializableFunction<TableDestination, TableSchema> schemaFunction =
        new ConstantSchemaFunction(write.getSchema());

    // If WriteBundlesToFiles produced more than MAX_NUM_FILES files or MAX_SIZE_BYTES bytes, then
    // the import needs to be split into multiple partitions, and those partitions will be
    // specified in multiPartitionsTag.
    PCollection<KV<TableDestination, String>> tempTables = partitions.get(multiPartitionsTag)
        // What's this GroupByKey for? Is this so we have a deterministic temp tables? If so, maybe
        // Reshuffle is better here.
        .apply("MultiPartitionsGroupByKey",
            GroupByKey.<KV<TableDestination, Integer>, List<String>>create())
        .apply("MultiPartitionsWriteTables", ParDo.of(new WriteTables(
            false,
            write.getBigQueryServices(),
            jobIdTokenView,
            tempFilePrefix,
            WriteDisposition.WRITE_EMPTY,
            CreateDisposition.CREATE_IF_NEEDED,
            schemaFunction))
            .withSideInputs(jobIdTokenView));

    // This view maps each final table destination to the set of temporary partitioned tables
    // the PCollection was loaded into.
    PCollectionView<Map<TableDestination, Iterable<String>>> tempTablesView = tempTables
        .apply("TempTablesView", View.<TableDestination, String>asMultimap());

    singleton.apply(ParDo
        .of(new WriteRename(
            write.getBigQueryServices(),
            jobIdTokenView,
            write.getWriteDisposition(),
            write.getCreateDisposition(),
            tempTablesView,
            write.getTableDescription()))
        .withSideInputs(tempTablesView, jobIdTokenView));

    // Write single partition to final table
    partitions.get(singlePartitionTag)
        .apply("SinglePartitionGroupByKey",
            GroupByKey.<KV<TableDestination, Integer>, List<String>>create())
        .apply("SinglePartitionWriteTables", ParDo.of(new WriteTables(
            true,
            write.getBigQueryServices(),
            jobIdTokenView,
            tempFilePrefix,
            write.getWriteDisposition(),
            write.getCreateDisposition(),
            schemaFunction))
            .withSideInputs(jobIdTokenView));

    return WriteResult.in(input.getPipeline());
  }
}
