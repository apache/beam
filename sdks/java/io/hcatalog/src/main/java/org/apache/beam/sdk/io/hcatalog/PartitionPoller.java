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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded poller to listen for new partitions. */
@DoFn.UnboundedPerElement
public class PartitionPoller
    extends DoFn<PartitionPoller.ReadRequest, HCatRecordReader.PartitionWrapper> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionPoller.class);
  private transient IMetaStoreClient metaStoreClient;
  private Map<String, String> configProperties;
  private String database;
  private String table;
  final Boolean shouldTreatSourceAsBounded;

  /** Wrapper class for reads. */
  public static class ReadRequest implements Serializable {

    final Duration pollInterval;
    final HCatalogIO.Read readSpec;
    final SerializableComparator<Partition> comparator;
    final SerializableFunction<String, Instant> watermarkerFn;
    final ImmutableList<String> partitionCols;
    final String watermarkPartitionColumn;

    public ReadRequest(
        Duration pollInterval,
        HCatalogIO.Read readSpec,
        SerializableComparator<Partition> comparator,
        SerializableFunction<String, Instant> watermarkerFn,
        ImmutableList partitionCols,
        String watermarkPartitionColumn) {
      this.pollInterval = pollInterval;
      this.readSpec = readSpec;
      this.comparator = comparator;
      this.partitionCols = partitionCols;
      this.watermarkPartitionColumn = watermarkPartitionColumn;
      this.watermarkerFn = watermarkerFn;
    }
  }

  public PartitionPoller(
      final Map<String, String> properties,
      final String database,
      final String table,
      final Boolean isBounded) {
    this.configProperties = properties;
    this.database = database;
    this.table = table;
    this.shouldTreatSourceAsBounded = isBounded;
  }

  /** Outputs newer partitions to be processed by the reader. */
  @ProcessElement
  @SuppressWarnings("unused")
  public ProcessContinuation processElement(
      final ProcessContext c, PartitionRangeTracker tracker) {
    final ReadRequest readRequest = c.element();
    final PartitionRange range = tracker.getRange();
    final ImmutableList<Partition> allAvailablePartitions = range.getPartitions();

    List<Partition> forSort = new ArrayList<>(allAvailablePartitions);
    Collections.sort(forSort, readRequest.comparator);

    int trueStart;
    if (tracker.getRange().getLastCompletedPartition() != null) {
      final int indexOfLastCompletedPartition =
          forSort.indexOf(tracker.getRange().getLastCompletedPartition());
      trueStart = indexOfLastCompletedPartition + 1;
    } else {
      trueStart = 0;
    }

    for (int i = trueStart; i < forSort.size(); i++) {
      if (i >= forSort.size()) {
        return ProcessContinuation.stop();
      }
      if (tracker.tryClaim(forSort.get(i))) {
        c.output(
            new HCatRecordReader.PartitionWrapper(
                forSort.get(i),
                readRequest.readSpec,
                readRequest.watermarkPartitionColumn,
                readRequest.partitionCols,
                readRequest.watermarkerFn));
      }
    }

    if (shouldTreatSourceAsBounded) {
      LOG.info("Will terminate reading since the source is set to behave as bounded.");
      return ProcessContinuation.stop();
    } else {
      LOG.info(
          "{} - will resume polling in {} ms.",
          c.element(),
          c.element().pollInterval);
      return ProcessContinuation.resume().withResumeDelay(c.element().pollInterval);
    }
  }

  @DoFn.Setup
  @SuppressWarnings("unused")
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : configProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    metaStoreClient = HCatUtil.getHiveMetastoreClient(HCatUtil.getHiveConf(conf));
  }

  @DoFn.Teardown
  @SuppressWarnings("unused")
  public void teardown() {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }

  @GetInitialRestriction
  @SuppressWarnings("unused")
  public PartitionRange getInitialRestriction(ReadRequest request) throws Exception {
    final ImmutableList<Partition> partitions =
        getPartitions(request.readSpec.getDatabase(), request.readSpec.getTable());
    return new PartitionRange(partitions, request.comparator, null);
  }

  @NewTracker
  @SuppressWarnings("unused")
  public PartitionRangeTracker newTracker(PartitionRange range) throws Exception {
    final SerializableComparator<Partition> comparator = range.getComparator();
    final Partition lastCompletedPartition = range.getLastCompletedPartition();
    if (range.getLastCompletedPartition() == null) {
      return range.newTracker();
    } else {
      final PartitionRange partitionRange =
          new PartitionRange(getPartitions(database, table), comparator, lastCompletedPartition);
      return partitionRange.newTracker();
    }
  }

  private ImmutableList<Partition> getPartitions(String database, String table) throws Exception {
    final List<Partition> partitions =
        metaStoreClient.listPartitions(database, table, Short.MAX_VALUE);
    return ImmutableList.copyOf(partitions);
  }
}
