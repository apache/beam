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
class WritePartition extends DoFn<String, KV<ShardedKey<TableDestination>, List<String>>> {
  private final ValueProvider<String> singletonOutputJsonTableRef;
  private final String singletonOutputTableDescription;
  private final PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView;
  private TupleTag<KV<ShardedKey<TableDestination>, List<String>>> multiPartitionsTag;
  private TupleTag<KV<ShardedKey<TableDestination>, List<String>>> singlePartitionTag;

  private static class PartitionData {
    private int numFiles = 0;
    private long byteSize = 0;
    private List<String> filenames = Lists.newArrayList();

    int getNumFiles() {
      return numFiles;
    }

    void addFiles(int numFiles) {
      this.numFiles += numFiles;
    }

    long getByteSize() {
      return byteSize;
    }

    void addBytes(long numBytes) {
      this.byteSize += numBytes;
    }

    List<String> getFilenames() {
      return  filenames;
    }

    void addFilename(String filename) {
      filenames.add(filename);
    }

    // Check to see whether we can add to this partition without exceeding the maximum partition
    // size.
    boolean canAccept(int numFiles, long numBytes) {
      return this.numFiles + numFiles <= Write.MAX_NUM_FILES
          && this.byteSize + numBytes <= Write.MAX_SIZE_BYTES;
    }
  }

  private static class DestinationData {
    private List<PartitionData> partitions = Lists.newArrayList();

    DestinationData() {
      // Always start out with a single empty partition.
      partitions.add(new PartitionData());
    }

    List<PartitionData> getPartitions() {
      return partitions;
    }

    PartitionData getLatestPartition() {
      return partitions.get(partitions.size() - 1);
    }

    void addPartition(PartitionData partition) {
       partitions.add(partition);
    }
  }

  WritePartition(
      ValueProvider<String> singletonOutputJsonTableRef,
      String singletonOutputTableDescription,
      PCollectionView<Iterable<WriteBundlesToFiles.Result>> resultsView,
      TupleTag<KV<ShardedKey<TableDestination>, List<String>>> multiPartitionsTag,
      TupleTag<KV<ShardedKey<TableDestination>, List<String>>> singlePartitionTag) {
    this.singletonOutputJsonTableRef = singletonOutputJsonTableRef;
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
    if (results.isEmpty() && singletonOutputJsonTableRef != null) {
      TableReference singletonTable = BigQueryHelpers.fromJsonString(
          singletonOutputJsonTableRef.get(), TableReference.class);
      if (singletonTable != null) {
        TableRowWriter writer = new TableRowWriter(c.element());
        writer.open(UUID.randomUUID().toString());
        TableRowWriter.Result writerResult = writer.close();
        results.add(new Result(writerResult.filename, writerResult.byteSize,
            new TableDestination(singletonTable, singletonOutputTableDescription)));
      }
    }


    Map<TableDestination, DestinationData> currentResults = Maps.newHashMap();
    for (WriteBundlesToFiles.Result fileResult : results) {
      TableDestination tableDestination = fileResult.tableDestination;
      DestinationData destinationData = currentResults.get(tableDestination);
      if (destinationData == null) {
        destinationData = new DestinationData();
        currentResults.put(tableDestination, destinationData);
      }

      PartitionData latestPartition = destinationData.getLatestPartition();
      if (!latestPartition.canAccept(1, fileResult.fileByteSize)) {
        // Too much data, roll over to a new partition.
        latestPartition = new PartitionData();
        destinationData.addPartition(latestPartition);
      }
      latestPartition.addFilename(fileResult.filename);
      latestPartition.addFiles(1);
      latestPartition.addBytes(fileResult.fileByteSize);
    }

    // Now that we've figured out which tables and partitions to write out, emit this information
    // to the next stage.
    for (Map.Entry<TableDestination, DestinationData> entry : currentResults.entrySet()) {
      TableDestination tableDestination = entry.getKey();
      DestinationData destinationData = entry.getValue();
      // In the fast-path case where we only output one table, the transform loads it directly
      // to the final table. In this case, we output on a special TupleTag so the enclosing
      // transform knows to skip the rename step.
      TupleTag<KV<ShardedKey<TableDestination>, List<String>>> outputTag =
          (destinationData.getPartitions().size() == 1) ? singlePartitionTag : multiPartitionsTag;
      for (int i = 0; i < destinationData.getPartitions().size(); ++i) {
        PartitionData partitionData = destinationData.getPartitions().get(i);
        c.output(outputTag, KV.of(ShardedKey.of(tableDestination, i + 1),
            partitionData.getFilenames()));
      }
    }
  }
}
