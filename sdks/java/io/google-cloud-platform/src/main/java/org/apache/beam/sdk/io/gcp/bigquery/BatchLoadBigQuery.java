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
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * PTransform that uses BigQuery batch-load jobs to write a PCollection to BigQuery.
 */
class BatchLoadBigQuery<T> extends PTransform<PCollection<T>, WriteResult> {
  BigQueryIO.Write<T> write;

  BatchLoadBigQuery(BigQueryIO.Write<T> write) {
    this.write = write;
  }

  @Override
  public WriteResult expand(PCollection<T> input) {
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

    // Create a singleton job ID token at execution time.
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

    PCollection<T> typedInputInGlobalWindow =
        input.apply(
            Window.<T>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .discardingFiredPanes());
    // Avoid applying the formatFunction if it is the identity formatter.
    PCollection<TableRow> inputInGlobalWindow;
    if (write.getFormatFunction() == BigQueryIO.IDENTITY_FORMATTER) {
      inputInGlobalWindow = (PCollection<TableRow>) typedInputInGlobalWindow;
    } else {
      inputInGlobalWindow =
          typedInputInGlobalWindow.apply(
              MapElements.into(new TypeDescriptor<TableRow>() {}).via(write.getFormatFunction()));
    }

    // PCollection of filename, file byte size.
    PCollection<KV<String, Long>> results = inputInGlobalWindow
        .apply("WriteBundles",
            ParDo.of(new WriteBundles(tempFilePrefix)));

    TupleTag<KV<Long, List<String>>> multiPartitionsTag =
        new TupleTag<KV<Long, List<String>>>("multiPartitionsTag") {};
    TupleTag<KV<Long, List<String>>> singlePartitionTag =
        new TupleTag<KV<Long, List<String>>>("singlePartitionTag") {};

    // Turn the list of files and record counts in a PCollectionView that can be used as a
    // side input.
    PCollectionView<Iterable<KV<String, Long>>> resultsView = results
        .apply("ResultsView", View.<KV<String, Long>>asIterable());
    PCollectionTuple partitions = singleton.apply(ParDo
        .of(new WritePartition(
            resultsView,
            multiPartitionsTag,
            singlePartitionTag))
        .withSideInputs(resultsView)
        .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

    // If WriteBundles produced more than MAX_NUM_FILES files or MAX_SIZE_BYTES bytes, then
    // the import needs to be split into multiple partitions, and those partitions will be
    // specified in multiPartitionsTag.
    PCollection<String> tempTables = partitions.get(multiPartitionsTag)
        .apply("MultiPartitionsGroupByKey", GroupByKey.<Long, List<String>>create())
        .apply("MultiPartitionsWriteTables", ParDo.of(new WriteTables(
            false,
            write.getBigQueryServices(),
            jobIdTokenView,
            tempFilePrefix,
            NestedValueProvider.of(table, new TableRefToJson()),
            write.getJsonSchema(),
            WriteDisposition.WRITE_EMPTY,
            CreateDisposition.CREATE_IF_NEEDED,
            write.getTableDescription()))
            .withSideInputs(jobIdTokenView));

    PCollectionView<Iterable<String>> tempTablesView = tempTables
        .apply("TempTablesView", View.<String>asIterable());
    singleton.apply(ParDo
        .of(new WriteRename(
            write.getBigQueryServices(),
            jobIdTokenView,
            NestedValueProvider.of(table, new TableRefToJson()),
            write.getWriteDisposition(),
            write.getCreateDisposition(),
            tempTablesView,
            write.getTableDescription()))
        .withSideInputs(tempTablesView, jobIdTokenView));

    // Write single partition to final table
    partitions.get(singlePartitionTag)
        .apply("SinglePartitionGroupByKey", GroupByKey.<Long, List<String>>create())
        .apply("SinglePartitionWriteTables", ParDo.of(new WriteTables(
            true,
            write.getBigQueryServices(),
            jobIdTokenView,
            tempFilePrefix,
            NestedValueProvider.of(table, new TableRefToJson()),
            write.getJsonSchema(),
            write.getWriteDisposition(),
            write.getCreateDisposition(),
            write.getTableDescription()))
            .withSideInputs(jobIdTokenView));

    return WriteResult.in(input.getPipeline());
  }
}
