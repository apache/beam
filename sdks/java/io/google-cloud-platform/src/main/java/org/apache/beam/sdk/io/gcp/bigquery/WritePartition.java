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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Partitions temporary files based on number of files and file sizes. Output key is a pair of
 * tablespec and the list of files corresponding to each partition of that table.
 */
class WritePartition<DestinationT>
    extends DoFn<
        Iterable<WriteBundlesToFiles.Result<DestinationT>>,
        KV<ShardedKey<DestinationT>, List<String>>> {
  private final boolean singletonTable;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private final PCollectionView<String> tempFilePrefix;
  private final int maxNumFiles;
  private final long maxSizeBytes;
  private final RowWriterFactory<?, DestinationT> rowWriterFactory;

  private @Nullable TupleTag<KV<ShardedKey<DestinationT>, List<String>>> multiPartitionsTag;
  private TupleTag<KV<ShardedKey<DestinationT>, List<String>>> singlePartitionTag;

  private static class PartitionData {
    private int numFiles = 0;
    private long byteSize = 0;
    private List<String> filenames = Lists.newArrayList();
    private final int maxNumFiles;
    private final long maxSizeBytes;

    private PartitionData(int maxNumFiles, long maxSizeBytes) {
      this.maxNumFiles = maxNumFiles;
      this.maxSizeBytes = maxSizeBytes;
    }

    static PartitionData withMaximums(int maxNumFiles, long maxSizeBytes) {
      return new PartitionData(maxNumFiles, maxSizeBytes);
    }

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
      return filenames;
    }

    void addFilename(String filename) {
      filenames.add(filename);
    }

    // Check to see whether we can add to this partition without exceeding the maximum partition
    // size.
    boolean canAccept(int numFiles, long numBytes) {
      if (filenames.isEmpty()) {
        return true;
      }
      return this.numFiles + numFiles <= maxNumFiles && this.byteSize + numBytes <= maxSizeBytes;
    }
  }

  private static class DestinationData {
    private List<PartitionData> partitions = Lists.newArrayList();

    private DestinationData() {}

    private static DestinationData create(int maxNumFiles, long maxSizeBytes) {
      DestinationData destinationData = new DestinationData();
      // Always start out with a single empty partition.
      destinationData.partitions.add(new PartitionData(maxNumFiles, maxSizeBytes));
      return destinationData;
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
      boolean singletonTable,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      PCollectionView<String> tempFilePrefix,
      int maxNumFiles,
      long maxSizeBytes,
      TupleTag<KV<ShardedKey<DestinationT>, List<String>>> multiPartitionsTag,
      TupleTag<KV<ShardedKey<DestinationT>, List<String>>> singlePartitionTag,
      RowWriterFactory<?, DestinationT> rowWriterFactory) {
    this.singletonTable = singletonTable;
    this.dynamicDestinations = dynamicDestinations;
    this.tempFilePrefix = tempFilePrefix;
    this.maxNumFiles = maxNumFiles;
    this.maxSizeBytes = maxSizeBytes;
    this.multiPartitionsTag = multiPartitionsTag;
    this.singlePartitionTag = singlePartitionTag;
    this.rowWriterFactory = rowWriterFactory;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    List<WriteBundlesToFiles.Result<DestinationT>> results = Lists.newArrayList(c.element());

    // If there are no elements to write _and_ the user specified a constant output table, then
    // generate an empty table of that name.
    if (results.isEmpty() && singletonTable) {
      String tempFilePrefix = c.sideInput(this.tempFilePrefix);
      // Return a null destination in this case - the constant DynamicDestinations class will
      // resolve it to the singleton output table.
      DestinationT destination = dynamicDestinations.getDestination(null);

      BigQueryRowWriter<?> writer = rowWriterFactory.createRowWriter(tempFilePrefix, destination);
      writer.close();
      BigQueryRowWriter.Result writerResult = writer.getResult();

      results.add(
          new Result<>(writerResult.resourceId.toString(), writerResult.byteSize, destination));
    }

    Map<DestinationT, DestinationData> currentResults = Maps.newHashMap();
    for (WriteBundlesToFiles.Result<DestinationT> fileResult : results) {
      DestinationT destination = fileResult.destination;
      DestinationData destinationData =
          currentResults.computeIfAbsent(
              destination, k -> DestinationData.create(maxNumFiles, maxSizeBytes));

      PartitionData latestPartition = destinationData.getLatestPartition();
      if (!latestPartition.canAccept(1, fileResult.fileByteSize)) {
        // Too much data, roll over to a new partition.
        latestPartition = PartitionData.withMaximums(maxNumFiles, maxSizeBytes);
        destinationData.addPartition(latestPartition);
      }
      latestPartition.addFilename(fileResult.filename);
      latestPartition.addFiles(1);
      latestPartition.addBytes(fileResult.fileByteSize);
    }

    // Now that we've figured out which tables and partitions to write out, emit this information
    // to the next stage.
    for (Map.Entry<DestinationT, DestinationData> entry : currentResults.entrySet()) {
      DestinationT destination = entry.getKey();
      DestinationData destinationData = entry.getValue();
      // In the fast-path case where we only output one table, the transform loads it directly
      // to the final table. In this case, we output on a special TupleTag so the enclosing
      // transform knows to skip the rename step.
      TupleTag<KV<ShardedKey<DestinationT>, List<String>>> outputTag =
          (destinationData.getPartitions().size() == 1) ? singlePartitionTag : multiPartitionsTag;
      for (int i = 0; i < destinationData.getPartitions().size(); ++i) {
        PartitionData partitionData = destinationData.getPartitions().get(i);
        c.output(outputTag, KV.of(ShardedKey.of(destination, i + 1), partitionData.getFilenames()));
      }
    }
  }
}
