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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded reader for doing reads from Hcat. */
@DoFn.BoundedPerElement
public class HCatRecordReader extends DoFn<HCatRecordReader.PartitionWrapper, HCatRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordReader.class);

  /** Wrapper class for partition data. */
  public static class PartitionWrapper implements Serializable {
    final Partition partition;
    final HCatalogIO.Read readSpec;
    final String watermarkPartitionColumn;
    final ImmutableList<String> partitionCols;
    final SerializableFunction<String, Instant> watermarkerFn;

    public PartitionWrapper(
        Partition partition,
        HCatalogIO.Read readSpec,
        String watermarkPartitionColumn,
        ImmutableList<String> partitionCols,
        SerializableFunction<String, Instant> watermarkerFn) {

      this.partition = partition;
      this.readSpec = readSpec;
      this.watermarkPartitionColumn = watermarkPartitionColumn;
      this.partitionCols = partitionCols;
      this.watermarkerFn = watermarkerFn;
    }
  }

  private ReaderContext getReaderContext(PartitionWrapper wrapper, long desiredSplitCount)
      throws HCatException {
    final HCatalogIO.Read spec = wrapper.readSpec;
    final Partition partition = wrapper.partition;
    final List<String> values = partition.getValues();
    final ImmutableList<String> partitionCols = wrapper.partitionCols;
    checkArgument(
        values.size() == partitionCols.size(),
        "Number of input partitions should be equal to the values of list partition values.");

    List<String> filter = new ArrayList<>();
    for (int i = 0; i < partitionCols.size(); i++) {
      filter.add(partitionCols.get(i) + "=" + "'" + values.get(i) + "'");
    }

    final String filterString = filter.stream().collect(Collectors.joining(" and "));

    ReadEntity entity =
        new ReadEntity.Builder()
            .withDatabase(spec.getDatabase())
            .withFilter(filterString)
            .withTable(spec.getTable())
            .build();
    // pass the 'desired' split count as an hint to the API
    Map<String, String> configProps = new HashMap<>(spec.getConfigProperties());
    configProps.put(
        HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, String.valueOf(desiredSplitCount));
    return DataTransferFactory.getHCatReader(entity, configProps).prepareRead();
  }

  private long getFileSizeForPartitions(PartitionWrapper partitionWrapper) throws Exception {
    IMetaStoreClient client = null;
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry :
        partitionWrapper.readSpec.getConfigProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      List<org.apache.hadoop.hive.ql.metadata.Partition> p = new ArrayList<>();

      Table table =
          HCatUtil.getTable(
              client,
              partitionWrapper.readSpec.getDatabase(),
              partitionWrapper.readSpec.getTable());

      final org.apache.hadoop.hive.ql.metadata.Partition partition =
          new org.apache.hadoop.hive.ql.metadata.Partition(table, partitionWrapper.partition);
      p.add(partition);
      final List<Long> fileSizeForPartitions = StatsUtils.getFileSizeForPartitions(hiveConf, p);
      return fileSizeForPartitions.get(0);
    } finally {
      // IMetaStoreClient is not AutoCloseable, closing it manually
      if (client != null) {
        client.close();
      }
    }
  }

  /** Reads data for a specific partition. */
  @ProcessElement
  @SuppressWarnings("unused")
  public void processElement(ProcessContext processContext, SplitTracker tracker) throws Exception {
    Integer startFrom;
    if (tracker.getLastClaimedSplit() == null) {
      // This is the first time we are doing this. Try claiming the very first split
      startFrom = tracker.getSplit().getFrom();
    } else {
      final Integer lastClaimedSplit = tracker.getLastClaimedSplit();
      startFrom = lastClaimedSplit + 1;
    }

    final PartitionWrapper element = processContext.element();
    final ImmutableList<String> partitionCols = element.partitionCols;
    final List<String> values = element.partition.getValues();
    final String watermarkPartitionColumn = element.watermarkPartitionColumn;
    final int indexOfPartitionColumnWithWaterMark =
        watermarkPartitionColumn.indexOf(watermarkPartitionColumn);
    final String partitionWaterMark = values.get(indexOfPartitionColumnWithWaterMark);

    final Instant partitionWatermarkTimeStamp =
        processContext.element().watermarkerFn.apply(partitionWaterMark);

    int desiredSplitCount = 1;
    long estimatedSizeBytes = getFileSizeForPartitions(processContext.element());
    if (estimatedSizeBytes > 0) {
      desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / Integer.MAX_VALUE);
    }
    ReaderContext readerContext = getReaderContext(processContext.element(), desiredSplitCount);

    for (int i = startFrom; tracker.tryClaim(i); i++) {
      HCatReader reader = DataTransferFactory.getHCatReader(readerContext, i);
      Iterator<HCatRecord> hcatIterator = reader.read();
      while (hcatIterator.hasNext()) {
        final HCatRecord record = hcatIterator.next();
        processContext.output(record);
      }
    }
    LOG.info("Watermark update to {}", partitionWatermarkTimeStamp);
    // Once we have read completely from partition, we can advance the watermark associated with the
    // partition
    processContext.updateWatermark(partitionWatermarkTimeStamp);
  }

  @GetInitialRestriction
  @SuppressWarnings("unused")
  public Split getInitialRestriction(PartitionWrapper request) throws Exception {
    final Partition partition = request.partition;
    int desiredSplitCount = 1;
    long estimatedSizeBytes = getFileSizeForPartitions(request);
    if (estimatedSizeBytes > 0) {
      desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / Integer.MAX_VALUE);
    }
    ReaderContext readerContext = getReaderContext(request, desiredSplitCount);
    // process the splits returned by native API
    // this could be different from 'desiredSplitCount' calculated above
    LOG.info(
        "Splitting: estimated size {}, desired split count {}, actual split count",
        estimatedSizeBytes,
        desiredSplitCount);
    return new Split(0, readerContext.numSplits());
  }

  @NewTracker
  @SuppressWarnings("unused")
  public SplitTracker newTracker(Split split) {
    return split.newTracker();
  }
}
