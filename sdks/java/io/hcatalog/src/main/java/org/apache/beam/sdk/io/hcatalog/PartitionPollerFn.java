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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded poller to listen for new partitions. */
@UnboundedPerElement
class PartitionPollerFn extends DoFn<Read, HCatRecordReaderFn.PartitionWrapper> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionPollerFn.class);
  private transient IMetaStoreClient metaStoreClient;
  private Map<String, String> configProperties;
  private String database;
  private String table;
  final Boolean shouldTreatSourceAsBounded;

  public PartitionPollerFn(
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
      final ProcessContext c, RestrictionTracker<PartitionRange, Partition> tracker) {
    final Read readRequest = c.element();
    final PartitionRange range = tracker.currentRestriction();
    final ImmutableList<Partition> allAvailablePartitions = range.getPartitions();

    List<Partition> forSort = new ArrayList<>(allAvailablePartitions);
    Collections.sort(forSort, readRequest.getPartitionComparator());

    int trueStart;
    if (tracker.currentRestriction().getLastCompletedPartition() != null) {
      final int indexOfLastCompletedPartition =
          forSort.indexOf(tracker.currentRestriction().getLastCompletedPartition());
      trueStart = indexOfLastCompletedPartition + 1;
    } else {
      trueStart = 0;
    }

    for (int i = trueStart; i < forSort.size(); i++) {
      if (tracker.tryClaim(forSort.get(i))) {
        c.output(
            new HCatRecordReaderFn.PartitionWrapper(
                forSort.get(i),
                readRequest,
                readRequest.getWatermarkPartitionColumn(),
                readRequest.getPartitionCols(),
                readRequest.getWatermarkTimestampConverter()));
      }
    }

    if (shouldTreatSourceAsBounded) {
      LOG.info("Will terminate reading since the source is set to behave as bounded.");
      return ProcessContinuation.stop();
    } else {
      LOG.info("{} - will resume polling in {} ms.", readRequest, readRequest.getPollingInterval());
      return ProcessContinuation.resume().withResumeDelay(readRequest.getPollingInterval());
    }
  }

  @Setup
  @SuppressWarnings("unused")
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : configProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    metaStoreClient = HCatUtil.getHiveMetastoreClient(HCatUtil.getHiveConf(conf));
  }

  @Teardown
  @SuppressWarnings("unused")
  public void teardown() {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }

  @GetInitialRestriction
  @SuppressWarnings("unused")
  public PartitionRange getInitialRestriction(Read request) throws Exception {
    final ImmutableList<Partition> partitions =
        getPartitions(request.getDatabase(), request.getTable());
    return new PartitionRange(partitions, request.getPartitionComparator(), null);
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
