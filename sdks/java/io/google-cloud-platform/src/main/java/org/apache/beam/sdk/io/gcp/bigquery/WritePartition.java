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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Partitions temporary files based on number of files and file sizes. Output key is a pair of
 * tablespec and the list of files corresponding to each partition of that table.
 */
class WritePartition extends DoFn<String, KV<KV<TableDestination, Integer>, List<String>>> {
  private final ValueProvider<TableReference> singletonOutputTable;
  private final String singletonOutputTableDescription;
  private final PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView;
  private TupleTag<KV<KV<TableDestination, Integer>, List<String>>> multiPartitionsTag;
  private TupleTag<KV<KV<TableDestination, Integer>, List<String>>> singlePartitionTag;

  public WritePartition(
      ValueProvider<TableReference> singletonOutputTable,
      String singletonOutputTableDescription,
      PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView,
      TupleTag<KV<KV<TableDestination, Integer>, List<String>>> multiPartitionsTag,
      TupleTag<KV<KV<TableDestination, Integer>, List<String>>> singlePartitionTag) {
    this.singletonOutputTable = singletonOutputTable;
    this.singletonOutputTableDescription = singletonOutputTableDescription;
    this.resultsView = resultsView;
    this.multiPartitionsTag = multiPartitionsTag;
    this.singlePartitionTag = singlePartitionTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    List<WriteBundlesToFiles.Result> results = Lists.newArrayList(c.sideInput(resultsView));

    // If there are no elements to write _and_ the user specified a constant output table, then
    // generate an empty table of that name.
    if (results.isEmpty() && singletonOutputTable != null) {
      TableReference singletonTable = singletonOutputTable.get();
      if (singletonTable != null) {
        TableRowWriter writer = new TableRowWriter(c.element());
        writer.open(UUID.randomUUID().toString());
        TableRowWriter.Result writerResult = writer.close();
        results.add(new Result(writerResult.filename, writerResult.byteSize,
            new TableDestination(singletonTable, singletonOutputTableDescription)));
      }
    }


    long partitionId = 0;
    Map<TableDestination, Integer> currNumFilesMap = Maps.newHashMap();
    Map<TableDestination, Long> currSizeBytesMap = Maps.newHashMap();
    Map<TableDestination, List<List<String>>> currResultsMap = Maps.newHashMap();
    for (int i = 0; i < results.size(); ++i) {
      WriteBundlesToFiles.Result fileResult = results.get(i);
      TableDestination tableDestination = fileResult.tableDestination;
      // JAVA8
      List<List<String>> partitions = currResultsMap.getOrDefault(tableDestination, null);
      if (partitions == null) {
        partitions = Lists.newArrayList();
        partitions.add(Lists.<String>newArrayList());
        currResultsMap.put(tableDestination, partitions);
      }
      int currNumFiles = currNumFilesMap.getOrDefault(tableDestination, 0);
      long currSizeBytes = currSizeBytesMap.getOrDefault(tableDestination, 0L);
      if (currNumFiles + 1 > Write.MAX_NUM_FILES
          || currSizeBytes + fileResult.fileByteSize > Write.MAX_SIZE_BYTES) {
        // Add a new partition for this table.
        partitions.add(Lists.<String>newArrayList());
      //  c.sideOutput(multiPartitionsTag, KV.of(++partitionId, currResults));
        currNumFiles = 0;
        currSizeBytes = 0;
        currNumFilesMap.remove(tableDestination);
        currSizeBytesMap.remove(tableDestination);
      }
      currNumFilesMap.put(tableDestination, currNumFiles + 1);
      currSizeBytesMap.put(tableDestination, currSizeBytes + fileResult.fileByteSize);
      // Always add to the most recent partition for this table.
      partitions.get(partitions.size() - 1).add(fileResult.filename);
    }

    for (Map.Entry<TableDestination, List<List<String>>> entry : currResultsMap.entrySet()) {
      TableDestination tableDestination = entry.getKey();
      List<List<String>> partitions = entry.getValue();
      TupleTag<KV<KV<TableDestination, Integer>, List<String>>> outputTag =
          (partitions.size() == 1) ? singlePartitionTag : multiPartitionsTag;
      for (int i = 0; i < partitions.size(); ++i) {
        c.output(outputTag, KV.of(KV.of(tableDestination, i + 1), partitions.get(i)));
      }
    }
  }
}
